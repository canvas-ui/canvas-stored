import fs from 'fs-extra';
import path from 'path';
import { pathToFileURL } from 'url';
import chokidar from 'chokidar';
import picomatch from 'picomatch';
import Debug from 'debug';
import StorageBackend from '../StorageBackend.js';
import { checksumFile } from '../../utils/checksum.js';
import { detectMimeType } from '../../utils/mime.js';

const debug = Debug('stored:backend:file');

// Per-backend staging dir for streaming puts. Lives inside the backend root so
// temp files share a filesystem with their final destination (cheap rename /
// hardlink), and is always excluded from the watcher.
const TMP_DIR = '.stored-tmp';
const TMP_IGNORE = /(^|[/\\])\.stored-tmp([/\\]|$)/;

export default class FileBackend extends StorageBackend {
    #root;
    #watcher = null;
    #watchEnabled;
    #ignored;
    #ignoreMatcher = null;
    #defaultAlgorithms = ['sha256'];

    constructor(name, config = {}) {
        super(name, config);
        if (!config.root) throw new Error('FileBackend requires root path');
        this.#root = path.resolve(config.root);
        this.#watchEnabled = config.watch ?? false;
        this.#ignored = config.ignored || null;
        this.#ignoreMatcher = this.#buildIgnoreMatcher(this.#ignored);
        this.#defaultAlgorithms = config.algorithms || ['sha256'];
        this.type = 'local';
        fs.ensureDirSync(this.#root);
        debug(`FileBackend "${name}" initialized at ${this.#root}`);
    }

    get root() { return this.#root; }
    get tempDir() { return path.join(this.#root, TMP_DIR); }
    get watching() { return !!this.#watcher; }
    get capabilities() { return { ...super.capabilities, canEnumerate: true }; }

    // One matcher shared by watch(), list() and (via list) scan(), so exclusion
    // patterns behave identically live and on resync. Accepts glob strings,
    // RegExps and predicate functions (rel-path based, '/'-separated). Chokidar
    // v4+ dropped glob support in `ignored`, hence the picomatch bridge.
    #buildIgnoreMatcher(patterns) {
        if (!patterns) return null;
        const list = (Array.isArray(patterns) ? patterns : [patterns]).filter(Boolean);
        if (list.length === 0) return null;

        const globs = [];
        const regexes = [];
        const fns = [];
        for (const pattern of list) {
            if (typeof pattern === 'string') {
                globs.push(pattern);
                // "**/node_modules/**" should also prune the directory itself so
                // enumeration/watch never descends into it.
                if (pattern.endsWith('/**')) globs.push(pattern.slice(0, -3));
            } else if (pattern instanceof RegExp) {
                regexes.push(pattern);
            } else if (typeof pattern === 'function') {
                fns.push(pattern);
            }
        }
        const globMatch = globs.length > 0 ? picomatch(globs, { dot: true }) : null;
        return (relPath) => {
            const rel = String(relPath).split(path.sep).join('/');
            if (rel === '' || rel === '.') return false;
            if (globMatch && globMatch(rel)) return true;
            if (regexes.some((re) => re.test(rel))) return true;
            if (fns.some((fn) => fn(rel))) return true;
            return false;
        };
    }

    #isIgnored(relPath) {
        return this.#ignoreMatcher ? this.#ignoreMatcher(relPath) : false;
    }

    // Truthful server-side location. Consumers decide whether to surface a local
    // path to clients (it leaks the server fs layout); stored:// is the address.
    nativeUrl(key) { return pathToFileURL(this.#resolvePath(key)).href; }

    // ─────────────────────────────────────────────────────────────────────────
    // CRUD Operations
    // ─────────────────────────────────────────────────────────────────────────

    #resolvePath(key) { return path.join(this.#root, key); }

    // Resolve a key to an absolute path, refusing anything that escapes the
    // backend root (path traversal) or targets the root itself. Used by the
    // container (directory) mutation ops.
    #safeResolve(key) {
        const target = path.resolve(this.#root, String(key || ''));
        const rel = path.relative(this.#root, target);
        if (rel === '' || rel.startsWith('..') || path.isAbsolute(rel)) {
            throw new Error(`Path escapes backend root: ${key}`);
        }
        return target;
    }

    // ── Container (directory) ops — only meaningful for a real filesystem, so
    // they live on the file driver (StorageBackend throws by default). Watched
    // backends reflect the byte changes; empty directories are surfaced by the
    // caller (Workspace) inserting a tree node.
    async createContainer(key) {
        const dir = this.#safeResolve(key);
        await fs.ensureDir(dir);
        debug(`MKDIR ${key}`);
        return { key };
    }

    async deleteContainer(key) {
        const dir = this.#safeResolve(key);
        if (!await fs.pathExists(dir)) return false;
        await fs.remove(dir);
        debug(`RMDIR ${key}`);
        return true;
    }

    async renameContainer(fromKey, toKey) {
        const from = this.#safeResolve(fromKey);
        const to = this.#safeResolve(toKey);
        if (!await fs.pathExists(from)) throw new Error(`Container not found: ${fromKey}`);
        await fs.ensureDir(path.dirname(to));
        await fs.move(from, to, { overwrite: false });
        debug(`MV ${fromKey} -> ${toKey}`);
        return { from: fromKey, to: toKey };
    }

    async put(key, data) {
        const filePath = this.#resolvePath(key);
        await fs.ensureDir(path.dirname(filePath));
        await fs.writeFile(filePath, data);
        const stats = await fs.stat(filePath);
        debug(`PUT ${key} (${stats.size} bytes)`);
        return { key, size: stats.size };
    }

    // Place an already-written file (e.g. a streamed temp) at `key`. Hardlinks
    // when on the same filesystem (zero copy, shared inode), falling back to a
    // byte copy across filesystems. Used by the streaming put path.
    async commit(key, srcPath) {
        const dest = this.#resolvePath(key);
        await fs.ensureDir(path.dirname(dest));
        await fs.remove(dest);
        try {
            await fs.link(srcPath, dest);
        } catch (err) {
            if (err.code === 'EXDEV') await fs.copyFile(srcPath, dest);
            else throw err;
        }
        const stats = await fs.stat(dest);
        debug(`COMMIT ${key} (${stats.size} bytes)`);
        return { key, size: stats.size };
    }

    async get(key, options = {}) {
        const filePath = this.#resolvePath(key);
        if (!await fs.pathExists(filePath)) return null;
        return options.stream ? fs.createReadStream(filePath) : fs.readFile(filePath);
    }

    // Ranged read for HTTP Range/streaming. `end` is inclusive (matches HTTP
    // Range and createReadStream). Null on miss → caller falls back to full read.
    async getRange(key, { start, end }) {
        const filePath = this.#resolvePath(key);
        if (!await fs.pathExists(filePath)) return null;
        return fs.createReadStream(filePath, { start, end });
    }

    async delete(key) {
        const filePath = this.#resolvePath(key);
        if (!await fs.pathExists(filePath)) return false;
        await fs.remove(filePath);
        debug(`DELETE ${key}`);
        return true;
    }

    async stat(key) {
        const filePath = this.#resolvePath(key);
        if (!await fs.pathExists(filePath)) return null;
        const stats = await fs.stat(filePath);
        if (!stats.isFile()) return null;
        return { key, size: stats.size, modified: stats.mtimeMs, created: stats.birthtimeMs };
    }

    async *list(options = {}) {
        const { prefix = '', recursive = true } = options;
        const searchPath = this.#resolvePath(prefix);
        if (!await fs.pathExists(searchPath)) return;

        const entries = await fs.readdir(searchPath, { withFileTypes: true });
        for (const entry of entries) {
            const relativePath = path.join(prefix, entry.name);
            if (this.#isIgnored(relativePath)) continue;
            if (entry.isFile()) {
                yield { key: relativePath, ...(await this.stat(relativePath)) };
            } else if (entry.isDirectory() && recursive) {
                yield* this.list({ ...options, prefix: relativePath });
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Watch & Scan
    // ─────────────────────────────────────────────────────────────────────────

    async watch() {
        if (this.#watcher) return true;

        const watchOpts = {
            persistent: true,
            ignoreInitial: true,
            awaitWriteFinish: { stabilityThreshold: 200, pollInterval: 50 },
        };
        // Chokidar hands us absolute paths; route them through the shared
        // rel-path matcher so watch and list/scan agree on exclusions.
        watchOpts.ignored = (p) => {
            if (TMP_IGNORE.test(p)) return true;
            const rel = path.relative(this.#root, p);
            if (!rel || rel.startsWith('..')) return false;
            return this.#isIgnored(rel);
        };

        this.#watcher = chokidar.watch(this.#root, watchOpts);

        const toKey = p => path.relative(this.#root, p);

        this.#watcher
            .on('add', async p => {
                const key = toKey(p);
                const [checksums, mimeType, stats] = await Promise.all([
                    checksumFile(p, this.#defaultAlgorithms).catch(() => null),
                    detectMimeType(p).catch(() => null),
                    fs.stat(p).catch(() => null),
                ]);
                this.emit('file:add', { backend: this.name, key, path: p, checksums, mimeType, size: stats?.size, modified: stats?.mtimeMs });
            })
            .on('change', async p => {
                const key = toKey(p);
                const [checksums, mimeType, stats] = await Promise.all([
                    checksumFile(p, this.#defaultAlgorithms).catch(() => null),
                    detectMimeType(p).catch(() => null),
                    fs.stat(p).catch(() => null),
                ]);
                this.emit('file:change', { backend: this.name, key, path: p, checksums, mimeType, size: stats?.size, modified: stats?.mtimeMs });
            })
            .on('unlink', p => {
                this.emit('file:unlink', { backend: this.name, key: toKey(p), path: p });
            })
            .on('error', err => this.emit('error', err));

        debug(`Watching ${this.#root}`);
        return true;
    }

    async scan(options = {}) {
        const algorithms = options.algorithms || this.#defaultAlgorithms;
        // Optional skip-hash predicate: (key, { size, mtime }) => { checksums, mimeType } | null.
        // When it returns a cached descriptor the (expensive) file read is skipped.
        const known = typeof options.knownChecksums === 'function' ? options.knownChecksums : null;
        // Optional streaming consumer: awaited per file as soon as its checksums
        // are ready, so large scans (a whole photo archive on a network mount)
        // surface results incrementally instead of after the full walk.
        const onFile = typeof options.onFile === 'function' ? options.onFile : null;
        const results = [];
        debug(`Scanning ${this.#root}...`);
        this.emit('scan:start', { backend: this.name });

        for await (const entry of this.list(options)) {
            const cached = known ? known(entry.key, { size: entry.size, mtime: entry.modified }) : null;
            let checksums, mimeType;
            if (cached) {
                checksums = cached.checksums;
                mimeType = cached.mimeType ?? null;
            } else {
                const filePath = this.#resolvePath(entry.key);
                [checksums, mimeType] = await Promise.all([
                    checksumFile(filePath, algorithms).catch(() => null),
                    detectMimeType(filePath).catch(() => null),
                ]);
            }
            const row = { ...entry, checksums, mimeType, backend: this.name };
            results.push(row);
            if (onFile) await onFile(row);
        }

        this.emit('scan:complete', { backend: this.name, count: results.length });
        debug(`Scan complete: ${results.length} files`);
        return results;
    }

    /**
     * Cheap structural walk: directory keys + file count, honoring the shared
     * ignore matcher — readdir only, no stat/hash. Lets callers mirror the
     * folder skeleton (and size a progress bar) before the expensive scan.
     */
    async shape(prefix = '') {
        const dirs = [];
        let files = 0;
        const walk = async (rel) => {
            const entries = await fs.readdir(this.#resolvePath(rel), { withFileTypes: true }).catch(() => []);
            for (const entry of entries) {
                const relativePath = path.join(rel, entry.name);
                if (TMP_IGNORE.test(relativePath) || this.#isIgnored(relativePath)) continue;
                if (entry.isDirectory()) {
                    dirs.push(relativePath.split(path.sep).join('/'));
                    await walk(relativePath);
                } else if (entry.isFile()) {
                    files += 1;
                }
            }
        };
        await walk(prefix);
        return { dirs, files };
    }

    async stop() {
        if (this.#watcher) {
            await this.#watcher.close();
            this.#watcher = null;
            debug(`Stopped watching ${this.#root}`);
        }
    }
}
