import fs from 'fs-extra';
import path from 'path';
import { pathToFileURL } from 'url';
import chokidar from 'chokidar';
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
    #defaultAlgorithms = ['sha256'];

    constructor(name, config = {}) {
        super(name, config);
        if (!config.root) throw new Error('FileBackend requires root path');
        this.#root = path.resolve(config.root);
        this.#watchEnabled = config.watch ?? false;
        this.#ignored = config.ignored || null;
        this.#defaultAlgorithms = config.algorithms || ['sha256'];
        this.type = 'local';
        fs.ensureDirSync(this.#root);
        debug(`FileBackend "${name}" initialized at ${this.#root}`);
    }

    get root() { return this.#root; }
    get tempDir() { return path.join(this.#root, TMP_DIR); }
    get watching() { return !!this.#watcher; }

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
        watchOpts.ignored = this.#ignored ? [TMP_IGNORE, this.#ignored] : [TMP_IGNORE];

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
            results.push({ ...entry, checksums, mimeType, backend: this.name });
        }

        this.emit('scan:complete', { backend: this.name, count: results.length });
        debug(`Scan complete: ${results.length} files`);
        return results;
    }

    async stop() {
        if (this.#watcher) {
            await this.#watcher.close();
            this.#watcher = null;
            debug(`Stopped watching ${this.#root}`);
        }
    }
}
