import Debug from 'debug';
import { randomUUID } from 'node:crypto';
import { Readable } from 'node:stream';
import StorageBackend from '../StorageBackend.js';

const debug = Debug('stored:backend:share');

export const SHARE_DEV = 'canvas-share';
// A source that has not polled for this long is offline.
const OFFLINE_AFTER_MS = 60_000;
const REQUEST_TIMEOUT_MS = 30_000;
const PAGE = 1000;
// Bytes are fetched in windows of this size: each is one short request the
// source answers in full, however slowly the reader consumes the file.
export const READ_WINDOW = 4 * 1024 * 1024;
const MAX_WAIT_MS = 50_000;
// Windows asked of the source at one time. Further reads wait here, before
// their timeout starts, instead of piling up on the machine that holds the files.
const READ_CONCURRENCY = 4;

function fault(code, message) { return Object.assign(new Error(message), { code }); }

/**
 * A folder on another machine, served by a `ShareSource` running there (the
 * canvas-edge share unit). The source dials in: it calls `poll()` to collect
 * the requests this backend has queued and `respond()` with each answer, so
 * the machine that holds the files needs no inbound connection. Whoever hosts
 * this backend carries those two calls over its own transport (the workspace
 * API: `POST …/backends/share/<name>/poll` and `…/requests/<id>`).
 *
 * The source hashes and watches its folder and keeps its own index; this
 * backend lists that index (`scan`), tails its change log while the source is
 * connected (`watch: true`) and reads bytes on demand. Nothing is copied here.
 * A source that stops polling is offline: scans answer `errors.root` so
 * nothing is removed, reads fail with `OFFLINE`.
 *
 * Read-only. `config.share` is the share's name, the authority of its
 * `file://<share>/<key>` locations.
 */
export default class ShareBackend extends StorageBackend {
    #queue = [];
    #pending = new Map();
    #waiter = null;
    #source = null;
    #seenAt = 0;
    #offlineTimer = null;
    #cursor = null;
    #tailing = null;
    #stopped = false;
    #reading = 0;
    #readWaiters = [];

    constructor(name, config = {}) {
        super(name, config);
        this.type = 'remote';
        debug(`ShareBackend "${name}" registered`);
    }

    static getCapabilities(config = {}) {
        return { ...super.getCapabilities(config), write: false, delete: false, canEnumerate: true, remote: true, paths: true };
    }
    get remote() { return true; }
    get transport() { return 'canvas-edge'; }
    get online() { return !this.#stopped && this.#seenAt > 0 && Date.now() - this.#seenAt < OFFLINE_AFTER_MS; }
    /** Changes arrive on their own while the source is connected. */
    get watching() { return this.config.watch === true && this.online; }
    /** What the source last said about itself: `{ epoch, fsid, head, scanning, files, walks, device, version, seenAt }`. */
    get source() { return this.#source ? { ...this.#source, seenAt: this.#seenAt, online: this.online } : null; }
    nativeUrl(key) { return `file://${this.config.share || this.name}/${String(key).split('/').map(encodeURIComponent).join('/')}`; }

    // ─────────────────────────────────────────────────────────────────────────
    // Source side: the two calls the host's transport carries
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * The source checks in and collects queued requests. `source` is
     * `{ epoch, fsid?, head, scanning?, files?, walks?, device?, version? }`: `epoch`
     * names the source's index (a new one invalidates the cursor), `fsid` the
     * filesystem behind the folder, `head` the newest entry of its change log,
     * `walks` how many passes over the folder it has finished.
     * Resolves with `{ requests }` at once when requests are queued, otherwise
     * after `wait` ms or as soon as one is.
     */
    async poll(source = {}, { wait = 25_000 } = {}) {
        if (this.#stopped) throw fault('STOPPED', 'Backend is stopped');
        if (!source.epoch) throw fault('INVALID_SOURCE', 'poll requires source.epoch');
        const expected = this.config.fsid?.id || null;
        if (expected && source.fsid && source.fsid !== expected) {
            throw fault('SHARE_REPLACED', `Share "${this.name}" is bound to another filesystem (${expected}); this one is ${source.fsid}. Remove the share to bind it again`);
        }
        const wasOnline = this.online;
        const previous = this.#source;
        this.#source = {
            epoch: String(source.epoch), fsid: source.fsid || null, head: Number(source.head) || 0,
            scanning: source.scanning === true, files: source.files ?? null, walks: Number(source.walks) || 0,
            device: source.device || null, version: source.version || null,
        };
        this.#seenAt = Date.now();
        clearTimeout(this.#offlineTimer);
        this.#offlineTimer = setTimeout(() => this.#wentOffline(), OFFLINE_AFTER_MS);
        this.#offlineTimer.unref?.();

        if (previous && previous.epoch !== this.#source.epoch) this.#cursor = null;
        // A listing is due when nothing is known about the source's log, and
        // after every pass the source finishes over its folder (only then is
        // a listing complete enough to tell what is gone).
        const finishedScan = previous != null && previous.walks !== this.#source.walks;
        if (!wasOnline || this.#cursor == null || finishedScan) {
            if (!wasOnline) debug(`"${this.name}" source online (epoch ${this.#source.epoch}, head ${this.#source.head})`);
            this.emit('backend:state', { backend: this.name, reason: wasOnline ? 'rescan' : 'attached', online: true, rescan: this.#cursor == null || finishedScan });
        }
        if (this.config.watch === true && this.#cursor != null && this.#source.head > this.#cursor) this.#tail();

        if (this.#queue.length) return { requests: this.#queue.splice(0) };
        if (!(Number(wait) > 0)) return { requests: [] };
        // One waiting poll at a time: a newer one releases the older.
        this.#waiter?.([]);
        const requests = await new Promise((resolve) => {
            const timer = setTimeout(() => { if (this.#waiter === done) this.#waiter = null; resolve([]); }, Math.min(MAX_WAIT_MS, Number(wait)));
            timer.unref?.();
            const done = (list) => { clearTimeout(timer); resolve(list); };
            this.#waiter = done;
        });
        return { requests };
    }

    /**
     * The source answers request `id`: `{ body }` (JSON result), `{ stream }`
     * (the bytes of a read, at most READ_WINDOW of them) or `{ error, code }`.
     * Resolves `true` once the answer is taken, `false` when nobody waits for
     * `id` any more.
     */
    async respond(id, { body = null, stream = null, error = null, code = null } = {}) {
        const pending = this.#pending.get(id);
        if (!pending) { stream?.resume?.(); return false; }
        this.#pending.delete(id);
        clearTimeout(pending.timer);
        if (error) { stream?.resume?.(); pending.reject(fault(code || 'SOURCE_ERROR', String(error))); return true; }
        if (!stream) { pending.resolve(body); return true; }
        try {
            const chunks = [];
            let size = 0;
            for await (const chunk of stream) {
                size += chunk.length;
                if (size > pending.limit) throw fault('TOO_LARGE', `Share "${this.name}" sent more than the ${pending.limit} bytes asked for`);
                chunks.push(chunk);
            }
            pending.resolve(Buffer.concat(chunks, size));
        } catch (failure) {
            stream.destroy?.();
            pending.reject(failure.code ? failure : fault('SOURCE_ERROR', failure.message));
        }
        return true;
    }

    /** The source says it stops serving: offline now, not a minute of silence later. */
    leave() {
        if (!this.online) return;
        this.#seenAt = 0;
        clearTimeout(this.#offlineTimer);
        this.#waiter?.([]);
        this.#waiter = null;
        this.#wentOffline();
    }

    #request(op, args = {}, { timeout = REQUEST_TIMEOUT_MS, limit = READ_WINDOW } = {}) {
        if (!this.online) return Promise.reject(fault('OFFLINE', `Share "${this.name}" is offline`));
        return new Promise((resolve, reject) => {
            const id = randomUUID();
            const timer = setTimeout(() => {
                this.#pending.delete(id);
                this.#queue = this.#queue.filter(r => r.id !== id);
                reject(fault('TIMEOUT', `Share "${this.name}" did not answer ${op} within ${timeout} ms`));
            }, timeout);
            timer.unref?.();
            this.#pending.set(id, { resolve, reject, timer, limit });
            this.#queue.push({ id, op, ...args });
            if (this.#waiter) { const deliver = this.#waiter; this.#waiter = null; deliver(this.#queue.splice(0)); }
        });
    }

    #wentOffline() {
        if (this.online) return;
        debug(`"${this.name}" source offline`);
        this.#failPending(fault('OFFLINE', `Share "${this.name}" went offline`));
        this.emit('backend:state', { backend: this.name, reason: 'offline', online: false });
    }

    #failPending(error) {
        for (const { reject, timer } of this.#pending.values()) { clearTimeout(timer); reject(error); }
        this.#pending.clear();
        this.#queue = [];
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Backend API
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Online, and still the filesystem this share was bound to (`expected` or
     * `config.fsid`, `{ id }`). `fsid` is null when the source cannot name its
     * filesystem; the share is then not bound.
     */
    async verifyRoot(expected = null) {
        if (!this.online) return { ok: false, reason: 'offline', fsid: null };
        const fsid = this.#source.fsid ? { id: this.#source.fsid } : null;
        const bound = expected?.id || this.config.fsid?.id || null;
        if (bound && fsid && fsid.id !== bound) return { ok: false, reason: 'filesystem-changed', fsid };
        return { ok: true, fsid };
    }

    /**
     * Ask the source to walk and hash its folder again now. Resolves once the
     * pass has started; the listing follows by itself when it is done.
     */
    async rescanSource() {
        await this.#request('rescan');
    }

    #row(o) {
        const sha256 = o.checksums?.sha256 || null;
        return {
            backend: this.name, key: o.key, size: o.size ?? null, modified: o.modified ?? null,
            mimeType: o.mimeType || 'application/octet-stream', checksums: o.checksums || null,
            // Content identity as the inode: a moved file keeps its document.
            dev: SHARE_DEV, ino: sha256,
        };
    }

    async *list({ prefix = '' } = {}) {
        let after = null;
        for (;;) {
            const page = await this.#request('list', { prefix, after, limit: PAGE });
            for (const o of page?.objects || []) yield this.#row(o);
            after = page?.cursor ?? null;
            if (after == null) return;
        }
    }

    /**
     * Everything the source has indexed. `complete` is false while the source
     * is still hashing its folder for the first time, and `errors.root` is set
     * when it is offline or failed mid-listing; neither may be read as "the
     * rest is gone".
     */
    async scan({ onFile } = {}) {
        const files = [], errors = { root: null, dirs: [], files: [] };
        this.emit('scan:start', { backend: this.name });
        const before = this.#source;
        if (!this.online) errors.root = 'offline';
        else {
            try {
                for await (const row of this.list()) {
                    files.push(row);
                    if (onFile) await onFile(row);
                }
                // Entries logged while the listing ran are replayed by the tail.
                if (this.#source?.epoch === before.epoch) this.#cursor = before.head;
            } catch (error) {
                if (error.code === 'RESYNC_CANCELLED') throw error;
                errors.root = error.code || 'unreachable';
            }
        }
        const complete = !errors.root && before?.scanning !== true && this.#source?.epoch === before?.epoch;
        this.emit('scan:complete', { backend: this.name, count: files.length, complete, errors });
        return { files, complete, errors, fsid: this.#source?.fsid ? { id: this.#source.fsid } : null };
    }

    async shape() {
        const shape = await this.#request('shape', {}, { timeout: 120_000 });
        return { dirs: shape?.dirs || [], files: shape?.files ?? 0 };
    }

    async stat(key) {
        const o = await this.#request('stat', { key });
        return o ? { key: o.key, size: o.size ?? null, modified: o.modified ?? null, created: null, mimeType: o.mimeType || null } : null;
    }

    async get(key, { stream = false } = {}) {
        const body = await this.#read(key);
        if (!body || stream) return body;
        const chunks = [];
        for await (const chunk of body) chunks.push(chunk);
        return Buffer.concat(chunks);
    }

    async getRange(key, { start = 0, end } = {}) {
        if (!Number.isSafeInteger(start) || start < 0 || (end != null && (!Number.isSafeInteger(end) || end < start))) throw fault('INVALID_RANGE', 'Invalid byte range');
        return this.#read(key, start, end);
    }

    // Bytes `start`..`end` (inclusive, to the end of the file when absent) as
    // a stream fed window by window; the next window is asked for when the
    // reader wants it. Null when the source has no such file.
    async #read(key, start = 0, end = null) {
        const window = async (from) => {
            const last = end != null ? Math.min(end, from + READ_WINDOW - 1) : from + READ_WINDOW - 1;
            if (this.#reading < READ_CONCURRENCY) this.#reading += 1;
            else await new Promise((resolve) => this.#readWaiters.push(resolve));
            try {
                return { bytes: await this.#request('read', { key, start: from, end: last }, { timeout: 120_000 }), asked: last - from + 1 };
            } finally {
                // The slot goes straight to the next waiting read, if any.
                const next = this.#readWaiters.shift();
                if (next) next(); else this.#reading -= 1;
            }
        };
        let first;
        try { first = await window(start); }
        catch (error) { if (error.code === 'NOT_FOUND') return null; throw error; }
        return Readable.from((async function* () {
            let position = start;
            for (let part = first; ; part = await window(position)) {
                if (part.bytes.length) yield part.bytes;
                position += part.bytes.length;
                // A short window is the end of the file.
                if (part.bytes.length < part.asked || (end != null && position > end)) return;
            }
        })(), { objectMode: false });
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Change tail
    // ─────────────────────────────────────────────────────────────────────────

    #tail() {
        if (this.#tailing) return this.#tailing;
        this.#tailing = this.#tailOnce()
            .catch((error) => { debug(`"${this.name}" tail failed: ${error.message}`); })
            .finally(() => { this.#tailing = null; });
        return this.#tailing;
    }

    async #tailOnce() {
        const epoch = this.#source.epoch;
        let since = this.#cursor;
        let applied = 0;
        for (;;) {
            const page = await this.#request('changes', { since, limit: PAGE });
            if (this.#source?.epoch !== epoch || this.#cursor == null) return;
            if (page?.cursorTooOld) {
                this.#cursor = null;
                this.emit('backend:state', { backend: this.name, reason: 'cursor-too-old', online: true, rescan: true });
                return;
            }
            for (const change of page.changes || []) { this.#apply(change); applied += 1; }
            since = Math.max(since, Number(page.cursor) || since);
            this.#cursor = since;
            if ((page.changes || []).length < PAGE) break;
        }
        if (applied) debug(`"${this.name}" applied ${applied} changes (cursor ${this.#cursor})`);
    }

    #apply(change) {
        if (!change?.key) return;
        const sha256 = change.checksums?.sha256 || null;
        if (change.op === 'delete') {
            this.emit('file:unlink', { backend: this.name, key: change.key, dev: SHARE_DEV, ino: sha256 });
            return;
        }
        if (!sha256) return;
        // unlink(from) + add(to) with one inode: Stored pairs them as a move.
        if (change.op === 'rename' && change.from) this.emit('file:unlink', { backend: this.name, key: change.from, dev: SHARE_DEV, ino: sha256 });
        this.emit('file:add', this.#row(change));
    }

    async stop() {
        this.#stopped = true;
        clearTimeout(this.#offlineTimer);
        this.#waiter?.([]);
        this.#waiter = null;
        this.#failPending(fault('STOPPED', 'Backend is stopped'));
        await this.#tailing;
    }
}
