import fs from 'fs-extra';
import path from 'path';
import { pathToFileURL } from 'url';
import chokidar from 'chokidar';
import picomatch from 'picomatch';
import Debug from 'debug';
import StorageBackend from '../StorageBackend.js';
import { checksumFile } from '../../utils/checksum.js';
import { detectMimeType } from '../../utils/mime.js';
import { detectMountSync } from '../../utils/mount.js';

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
    #remote = false;
    #transport = null;
    #mountSource = null;

    constructor(name, config = {}) {
        super(name, config);
        if (!config.root) throw new Error('FileBackend requires root path');
        this.#root = path.resolve(config.root);
        this.#watchEnabled = config.watch ?? false;
        this.#ignored = config.ignored || null;
        this.#ignoreMatcher = this.#buildIgnoreMatcher(this.#ignored);
        this.#defaultAlgorithms = config.algorithms || ['sha256'];
        this.type = 'local';
        // Managed stores (workspace:home/data) own their directory and may
        // create it. External mounts (createRoot:false) must NOT: silently
        // creating an empty dir at an unmounted mountpoint makes an absent
        // drive indistinguishable from an empty one — verifyRoot() + the
        // resync liveness gate rely on the root being genuinely missing.
        if (config.createRoot !== false) fs.ensureDirSync(this.#root);

        // Network mount detection. A CIFS/NFS share reached through a POSIX path
        // is indistinguishable from local disk to every fs call we make, so the
        // kernel mount table is the only honest source. Explicit config always
        // wins — detection only fills the gaps (and is a no-op off Linux).
        const detected = detectMountSync(this.#root);
        this.#remote = config.remote ?? detected.remote;
        this.#transport = config.transport ?? detected.transport;
        this.#mountSource = config.mountSource ?? detected.source;

        debug(`FileBackend "${name}" initialized at ${this.#root}${this.#remote ? ` (remote, ${this.#transport || 'unknown transport'})` : ''}`);
    }

    get root() { return this.#root; }
    get tempDir() { return path.join(this.#root, TMP_DIR); }
    get watching() { return !!this.#watcher; }
    get capabilities() { return { ...super.capabilities, canEnumerate: true }; }
    get remote() { return this.#remote; }
    get transport() { return this.#transport; }
    get mountSource() { return this.#mountSource; }

    /**
     * Absolute on-disk path for `key`. Public because cross-backend copy/move
     * needs it to detect a shared filesystem (same `dev` ⇒ rename instead of a
     * byte-for-byte stream copy) — callers outside stored should still address
     * bytes via `stored://<backend>/<key>`.
     */
    resolveKeyPath(key) { return this.#resolvePath(key); }

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

    /**
     * Liveness gate for resync: is the root present, a directory, and the same
     * filesystem it was when the mount was configured? `expected` is the fsid
     * snapshot ({ dev, ino }) recorded at mount creation (falls back to
     * config.fsid). An absent mountpoint or a dev mismatch means the backend
     * must go offline — a scan would read "empty", not "deleted".
     * @returns {Promise<{ok: boolean, reason?: string, fsid: {dev:number, ino:number}|null}>}
     */
    async verifyRoot(expected = null) {
        const snapshot = expected || this.config.fsid || null;
        let stats;
        try {
            stats = await fs.stat(this.#root);
        } catch {
            return { ok: false, reason: 'root-missing', fsid: null };
        }
        if (!stats.isDirectory()) return { ok: false, reason: 'root-not-directory', fsid: null };
        const fsid = { dev: stats.dev, ino: stats.ino };
        if (snapshot?.dev != null && Number(snapshot.dev) !== stats.dev) {
            return { ok: false, reason: 'filesystem-changed', fsid };
        }
        if (snapshot?.ino != null && Number(snapshot.ino) !== stats.ino) {
            return { ok: false, reason: 'root-replaced', fsid };
        }
        return { ok: true, fsid };
    }

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

    /**
     * Move an already-existing file at `srcPath` to `key` on this backend via a
     * single `rename(2)` — no bytes cross userspace, so a 10GB file moves in
     * microseconds instead of minutes. Only valid within one filesystem: throws
     * EXDEV otherwise, which callers treat as "fall back to a stream copy".
     */
    async renameFrom(key, srcPath) {
        const dest = this.#resolvePath(key);
        await fs.ensureDir(path.dirname(dest));
        await fs.rename(srcPath, dest);
        const stats = await fs.stat(dest);
        debug(`RENAME ${srcPath} -> ${key} (${stats.size} bytes)`);
        return { key, size: stats.size, modified: stats.mtimeMs, dev: stats.dev, ino: stats.ino };
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
        // dev/ino feed rename matching (same inode at a new path) and the
        // liveness checks — stored keeps them in location metadata.
        return { key, size: stats.size, modified: stats.mtimeMs, created: stats.birthtimeMs, dev: stats.dev, ino: stats.ino };
    }

    // `onError(relPath, err)` (optional): an unreadable directory (EACCES, I/O)
    // is reported and skipped instead of aborting the walk — one denied subtree
    // must not kill (or worse, truncate) a whole-mount enumeration.
    async *list(options = {}) {
        const { prefix = '', recursive = true, onError = null } = options;
        const searchPath = this.#resolvePath(prefix);
        if (!await fs.pathExists(searchPath)) return;

        let entries;
        try {
            entries = await fs.readdir(searchPath, { withFileTypes: true });
        } catch (err) {
            if (typeof onError === 'function') { onError(prefix, err); return; }
            throw err;
        }
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

        // inotify does not cross the wire: on cifs/nfs the kernel only reports
        // changes made through THIS client, so a watched share silently misses
        // everything another machine writes — worse than not watching, because
        // it looks live. Polling is the only correct option and it is expensive
        // on a large share, so it must be asked for explicitly.
        if (this.#remote && this.config.usePolling !== true) {
            debug(`Refusing to watch remote backend "${this.name}" (${this.#transport || 'unknown transport'}) without usePolling — use scan()/resync instead`);
            this.emit('backend:state', {
                backend: this.name,
                watching: false,
                remote: true,
                transport: this.#transport,
                reason: 'remote-watch-requires-polling',
            });
            return false;
        }

        const watchOpts = {
            persistent: true,
            ignoreInitial: true,
            awaitWriteFinish: { stabilityThreshold: 200, pollInterval: 50 },
        };
        // Opt-in polling (the only way to see other clients' writes on a network
        // mount). Deliberately slow by default: each tick stats every watched
        // file, which on a NAS is real traffic.
        if (this.config.usePolling === true) {
            watchOpts.usePolling = true;
            watchOpts.interval = this.config.pollInterval ?? 30000;
            watchOpts.binaryInterval = this.config.pollInterval ?? 30000;
        }
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
                this.emit('file:add', { backend: this.name, key, path: p, checksums, mimeType, size: stats?.size, modified: stats?.mtimeMs, dev: stats?.dev, ino: stats?.ino });
            })
            .on('change', async p => {
                const key = toKey(p);
                const [checksums, mimeType, stats] = await Promise.all([
                    checksumFile(p, this.#defaultAlgorithms).catch(() => null),
                    detectMimeType(p).catch(() => null),
                    fs.stat(p).catch(() => null),
                ]);
                this.emit('file:change', { backend: this.name, key, path: p, checksums, mimeType, size: stats?.size, modified: stats?.mtimeMs, dev: stats?.dev, ino: stats?.ino });
            })
            .on('unlink', p => {
                this.emit('file:unlink', { backend: this.name, key: toKey(p), path: p });
            })
            .on('error', err => this.emit('error', err));

        debug(`Watching ${this.#root}`);
        return true;
    }

    /**
     * Full-snapshot scan. Returns { files, complete, errors } — a diff against
     * the previous state may only run on a completed snapshot:
     *   - root liveness is verified first; a missing/replaced root yields
     *     complete:false and NO rows (absent mount ≠ empty mount)
     *   - unreadable directories are recorded in errors.dirs and skipped; their
     *     prior entries must be carried forward by the differ, not deleted
     *   - files that fail to hash keep a row (checksums:null) and are recorded
     *     in errors.files: present-but-unverified, never "deleted"
     */
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
        const errors = { root: null, dirs: [], files: [] };
        debug(`Scanning ${this.#root}...`);
        this.emit('scan:start', { backend: this.name });

        const liveness = await this.verifyRoot(options.fsid || null);
        if (!liveness.ok) {
            errors.root = liveness.reason;
            debug(`Scan aborted: root ${liveness.reason}`);
            this.emit('scan:complete', { backend: this.name, count: 0, complete: false, errors });
            return { files: [], complete: false, errors, fsid: liveness.fsid };
        }

        const listOptions = {
            ...options,
            onError: (relPath, err) => errors.dirs.push({ prefix: relPath, code: err?.code || 'EUNKNOWN' }),
        };
        for await (const entry of this.list(listOptions)) {
            const cached = known ? known(entry.key, { size: entry.size, mtime: entry.modified, dev: entry.dev, ino: entry.ino }) : null;
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
                if (!checksums) errors.files.push({ key: entry.key });
            }
            const row = { ...entry, checksums, mimeType, backend: this.name };
            results.push(row);
            if (onFile) await onFile(row);
        }

        const complete = errors.dirs.length === 0;
        this.emit('scan:complete', { backend: this.name, count: results.length, complete, errors });
        debug(`Scan complete: ${results.length} files (${complete ? 'full' : `partial — ${errors.dirs.length} unreadable dirs`})`);
        return { files: results, complete, errors, fsid: liveness.fsid };
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
