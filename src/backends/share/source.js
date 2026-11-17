import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';
import Debug from 'debug';
import Stored from '../../index.js';
import { detectMountSync } from '../../utils/mount.js';

const debug = Debug('stored:share-source');

const BACKEND = 'share';
const LIVENESS_CACHE_MS = 1000;
// Deletions the watcher reports wait this long, then are checked against the
// folder: an unplugged drive reports every file deleted in one burst.
const UNLINK_HOLD_MS = 1500;
const MAX_PAGE = 5000;

function fault(code, message) { return Object.assign(new Error(message), { code }); }

/**
 * What names the filesystem behind `root`, independent of where it is mounted
 * and of the device numbers the kernel hands out: `uuid:<filesystem uuid>`, or
 * `src:<mount source>` for a network mount or when the uuid cannot be read.
 * Null when the mount cannot be identified (not Linux, root missing).
 */
export function filesystemIdentity(root) {
    let real;
    try { real = fs.realpathSync(root); } catch { return null; }
    const mount = detectMountSync(real);
    if (!mount?.source) return null;
    try {
        const device = fs.realpathSync(mount.source);
        for (const name of fs.readdirSync('/dev/disk/by-uuid')) {
            try { if (fs.realpathSync(path.join('/dev/disk/by-uuid', name)) === device) return `uuid:${name}`; } catch { /* dangling link */ }
        }
    } catch { /* not a block device, or no udev */ }
    return `src:${mount.source}`;
}

/**
 * The serving half of a share: one folder, hashed and (optionally) watched
 * into its own Stored index under `stateDir`, answering the requests a
 * `ShareBackend` queues. The folder is opened read-only and nothing is ever
 * written into it.
 *
 * `stateDir/share.json` records the filesystem the index was built from. When
 * the folder is missing, sits on another filesystem, or is empty while the
 * index lists files (what an unplugged drive leaves behind: no mountpoint, or
 * an empty one), the source is not live: it serves nothing, a scan removes
 * nothing and the deletions the watcher reports are dropped, so an unmounted
 * drive is never read as "every file was deleted". Emptying a share for real
 * therefore does not empty its catalog; removing the share does.
 */
export default class ShareSource extends EventEmitter {
    #root;
    #stateDir;
    #watch;
    #ignored;
    #stored = null;
    #record = null;
    #scanning = null;
    #files = null;
    #walks = 0;
    #live = { at: 0, ok: false, reason: null };
    #heldUnlinks = [];
    // Watches do not survive the folder going away; they are set up again when it is back.
    #watchStale = false;
    #unlinkTimer = null;

    constructor({ root, stateDir, watch = false, ignored = [] } = {}) {
        super();
        if (!root || !stateDir) throw new Error('ShareSource requires root and stateDir');
        this.#root = path.resolve(root);
        this.#stateDir = path.resolve(stateDir);
        this.#watch = watch === true;
        this.#ignored = ignored;
    }

    get root() { return this.#root; }
    get scanning() { return this.#scanning != null; }

    /** `{ ok, reason }`: is the folder there, and on the filesystem the index describes? */
    liveness({ fresh = false } = {}) {
        if (!fresh && Date.now() - this.#live.at < LIVENESS_CACHE_MS) return this.#live;
        let reason = null;
        try { if (!fs.statSync(this.#root).isDirectory()) reason = 'root-not-directory'; }
        catch { reason = 'root-missing'; }
        if (!reason && this.#record?.fsid && filesystemIdentity(this.#root) !== this.#record.fsid) reason = 'filesystem-changed';
        if (!reason && this.#stored?.index.size > 0) {
            try {
                const dir = fs.opendirSync(this.#root);
                const first = dir.readSync();
                dir.closeSync();
                if (!first) reason = 'root-empty';
            } catch { reason = 'root-unreadable'; }
        }
        if (reason) this.#watchStale = true;
        this.#live = { at: Date.now(), ok: !reason, reason };
        return this.#live;
    }

    /** Open the index and start hashing. Throws when the folder is not there to bind to. */
    async start() {
        fs.mkdirSync(this.#stateDir, { recursive: true });
        const recordFile = path.join(this.#stateDir, 'share.json');
        try { this.#record = JSON.parse(fs.readFileSync(recordFile, 'utf8')); } catch { this.#record = null; }
        if (!this.#record) {
            if (!fs.existsSync(this.#root) || !fs.statSync(this.#root).isDirectory()) throw fault('ROOT_MISSING', `${this.#root} is not a directory`);
            // A leftover index without its record describes nothing we can trust.
            fs.rmSync(path.join(this.#stateDir, 'db'), { recursive: true, force: true });
            this.#record = { epoch: randomUUID(), fsid: filesystemIdentity(this.#root), root: this.#root, createdAt: new Date().toISOString() };
            fs.writeFileSync(recordFile, `${JSON.stringify(this.#record, null, 2)}\n`);
        }
        this.#stored = new Stored({ root: path.join(this.#stateDir, 'db'), cache: { path: path.join(this.#stateDir, 'cache') }, checksums: ['sha256'] });
        this.#stored.on('error', (error) => this.emit('error', error));
        this.#stored.on('change', () => this.emit('change'));
        const backend = this.#stored.addBackend(BACKEND, {
            driver: 'file', root: this.#root, createRoot: false, readOnly: true, watch: false,
            tempDir: path.join(this.#stateDir, 'tmp'), followSymlinks: false, stabilityThreshold: 2000, ignored: this.#ignored,
        });
        const emit = backend.emit.bind(backend);
        backend.emit = (event, ...args) => {
            if (event !== 'file:unlink') return emit(event, ...args);
            this.#heldUnlinks.push(args[0]);
            this.#unlinkTimer ??= setTimeout(() => {
                this.#unlinkTimer = null;
                const held = this.#heldUnlinks.splice(0);
                if (!this.liveness({ fresh: true }).ok) { this.emit('offline', this.#live.reason); return; }
                // A path that exists again was replaced, not deleted; its add or change says so.
                for (const unlink of held) if (!fs.existsSync(path.join(this.#root, unlink.key))) emit('file:unlink', unlink);
            }, UNLINK_HOLD_MS);
            return true;
        };
        // A walk that ends on a folder that is no longer live found nothing
        // because nothing was there to find: report it unusable, so no
        // location is removed on its account.
        const scan = backend.scan.bind(backend);
        backend.scan = async (options) => {
            const result = await scan(options);
            const live = this.liveness({ fresh: true });
            return live.ok ? result : { ...result, complete: false, errors: { ...result.errors, root: live.reason } };
        };
        if (this.#watch && this.liveness({ fresh: true }).ok) await backend.watch();
        void this.rescan();
    }

    /**
     * `{ epoch, fsid, head, scanning, files, walks }` for `ShareBackend.poll()`, or null
     * when not live. `walks` counts the finished passes over the folder: a new
     * value tells the backend to list again.
     */
    describe() {
        if (!this.#stored || !this.liveness().ok) return null;
        return {
            epoch: this.#record.epoch, fsid: this.#record.fsid, head: this.#stored.index.head(),
            scanning: this.scanning, files: this.#files, walks: this.#walks,
        };
    }

    /**
     * Walk and hash the folder; unchanged files (size + mtime) keep their
     * checksums, so an interrupted pass resumes where it stopped. One pass at
     * a time. Resolves with `{ ok, count, complete }`, or `{ ok:false, reason }`
     * when the folder is not live.
     */
    rescan() {
        if (this.#scanning) return this.#scanning;
        this.#scanning = (async () => {
            const live = this.liveness({ fresh: true });
            if (!live.ok) { this.emit('offline', live.reason); return { ok: false, reason: live.reason }; }
            const backend = this.#stored.getBackend(BACKEND);
            if (this.#watch && (this.#watchStale || !backend.watching)) {
                await backend.stop();
                await backend.watch();
                this.#watchStale = false;
            }
            let seen = 0;
            const result = await this.#stored.scan(BACKEND, {
                // The drive can go away mid-walk: stop before the walk reports its files gone.
                isCancelled: () => !this.liveness().ok,
                onFile: () => { if (++seen % 500 === 0) this.emit('progress', { scanned: seen }); },
            });
            const unusable = result.errors?.[BACKEND]?.root;
            if (unusable) { this.emit('offline', unusable); return { ok: false, reason: unusable }; }
            this.#files = result.count;
            this.#walks += 1;
            debug(`Scan of ${this.#root}: ${result.count} files (${result.complete ? 'complete' : 'partial'})`);
            return { ok: true, count: result.count, complete: result.complete };
        })().catch((error) => {
            if (error.code !== 'RESYNC_CANCELLED') this.emit('error', error);
            return { ok: false, reason: error.code || error.message };
        }).finally(() => { this.#scanning = null; this.emit('change'); });
        return this.#scanning;
    }

    /**
     * Answer one request of a `ShareBackend`: `{ body }` for list / changes /
     * shape / stat / rescan, `{ stream }` for read. `rescan` starts a pass over
     * the folder and answers at once. Throws with `code` NOT_FOUND,
     * OFFLINE or INVALID_REQUEST.
     */
    async handle(request = {}) {
        const live = this.liveness();
        if (!this.#stored || !live.ok) throw fault('OFFLINE', `Share folder is not available (${live.reason || 'stopped'})`);
        const index = this.#stored.index;
        const limit = Math.min(MAX_PAGE, Math.max(1, Number(request.limit) || 1000));
        switch (request.op) {
        case 'list': {
            const page = index.locationsByBackend(BACKEND, { prefix: request.prefix || '', after: request.after ?? null, limit });
            return { body: {
                objects: page.objects.filter(o => o.checksums).map(o => ({ key: o.key, size: o.size, modified: o.mtime, mimeType: o.mimeType, checksums: o.checksums })),
                cursor: page.cursor,
            } };
        }
        case 'changes': {
            const page = index.changes({ backend: BACKEND, since: request.since, limit });
            const changes = page.changes.map((c) => {
                const meta = c.op === 'delete' ? null : index.get(c.id);
                const [algorithm, digest] = String(c.id || '').split(':');
                return {
                    seq: c.seq, op: c.op, key: c.key, ...(c.from != null ? { from: c.from } : {}),
                    size: c.size, modified: c.mtime, mimeType: meta?.mimeType || null,
                    checksums: meta?.checksums || (digest ? { [algorithm]: digest } : null),
                };
            });
            return { body: { changes, head: page.head, cursor: page.cursor, cursorTooOld: page.cursorTooOld } };
        }
        case 'rescan': void this.rescan(); return { body: { scanning: true } };
        case 'shape': return { body: await this.#stored.getBackend(BACKEND).shape() };
        case 'stat': {
            const stat = await this.#stored.getBackend(BACKEND).stat(this.#key(request.key));
            if (!stat) return { body: null };
            return { body: { key: stat.key, size: stat.size, modified: stat.modified, mimeType: index.get(`${BACKEND}:${stat.key}`)?.mimeType || null } };
        }
        case 'read': {
            const backend = this.#stored.getBackend(BACKEND);
            const key = this.#key(request.key);
            const ranged = request.start != null || request.end != null;
            const stream = ranged
                ? await backend.getRange(key, { start: Number(request.start) || 0, ...(request.end != null ? { end: Number(request.end) } : {}) })
                : await backend.get(key, { stream: true });
            if (!stream) throw fault('NOT_FOUND', `No such file: ${key}`);
            return { stream };
        }
        default: throw fault('INVALID_REQUEST', `Unknown share request: ${request.op}`);
        }
    }

    // Only files the index lists are served: an excluded file is not reachable by guessing its key.
    #key(key) {
        if (typeof key !== 'string' || !this.#stored.index.has(`${BACKEND}:${key}`)) throw fault('NOT_FOUND', `No such file: ${key}`);
        return key;
    }

    async stop() {
        const stored = this.#stored;
        this.#stored = null;
        clearTimeout(this.#unlinkTimer);
        this.#unlinkTimer = null;
        await this.#scanning;
        await stored?.stop().catch(() => {});
    }
}
