import { Readable } from 'stream';
import { createReadStream, promises as fsp } from 'fs';
import Debug from 'debug';
import StorageBackend from '../StorageBackend.js';
import { normalizeKey, matchesPrefixes } from '../../sync/keys.js';

const debug = Debug('stored:backend:canvas');

/**
 * Canvas hub storage backend — one path-addressed backend (`workspace:home`
 * by default) of one workspace on a canvas-server, spoken over the objects
 * protocol (`canvas-server/docs/sync-protocol.md`).
 *
 * Shaped like the gdrive driver: `type:'remote'`, reads stream from the API,
 * `list()/scan()` page the hub's listing, `watch()` polls the change feed and
 * synthesizes the file driver's `file:add|change|unlink` events so Stored
 * indexes the hub's keys like any other backend. Identity on the wire is the
 * sha256 (`ETag`), so rows carry `ino = sha256` and `dev = canvas:<instanceId>`
 * — a hub-side rename (`unlink(from) + add(to)`, same ino) is paired by
 * Stored's rename window into an in-place location rewrite.
 *
 * Writes are direct (`putStream` — streaming `PUT` with the protocol's
 * preconditions and conflict headers); there is no cache/SyncQueue hop. Every
 * mutation is stamped `X-Canvas-Origin: <deviceId>`, and the poller applies
 * the resulting change-log echoes to its own state without re-emitting them.
 *
 * Errors are typed on `err.code`: `PRECONDITION_FAILED` (`err.current`),
 * `CURSOR_TOO_OLD` (`err.oldest`, `err.head`), `TARGET_EXISTS`, `NOT_FOUND`,
 * `UNAUTHORIZED`, `REFUSED` (a 4xx that will not change on retry), `RETRYABLE`
 * (429/5xx after the in-call retries) and `OFFLINE` (transport failure).
 */

const DEV_PREFIX = 'canvas';
const DEFAULT_POLL_INTERVAL = 30_000;
const PING_CACHE_MS = 30_000;
const LIST_PAGE = 1000;
const CHANGES_PAGE = 1000;
const RETRY_DELAYS_MS = [500, 1500, 4000];
const JSON_HEADERS = { Accept: 'application/json' };

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const encodeSegment = (s) => encodeURIComponent(String(s));
const encodeKey = (key) => normalizeKey(key).split('/').map(encodeSegment).join('/');
const quote = (sha) => (String(sha).startsWith('"') ? String(sha) : `"${sha}"`);
const lower = (v) => (v == null ? null : String(v).toLowerCase());
const num = (v, fallback = null) => {
    if (v == null || v === '') return fallback;
    const n = Number(v);
    return Number.isFinite(n) ? n : fallback;
};
const parseMs = (v) => {
    if (v == null || v === '') return null;
    if (typeof v === 'number') return Number.isFinite(v) ? v : null;
    const n = Number(v);
    if (Number.isFinite(n)) return n;
    const t = Date.parse(String(v));
    return Number.isFinite(t) ? t : null;
};

export class CanvasHubError extends Error {
    constructor(message, { code = 'OFFLINE', status = null, hubCode = null, current, oldest, head, key } = {}) {
        super(message);
        this.name = 'CanvasHubError';
        this.code = code;
        this.status = status;
        this.hubCode = hubCode;
        if (current !== undefined) this.current = current;
        if (oldest !== undefined) this.oldest = oldest;
        if (head !== undefined) this.head = head;
        if (key !== undefined) this.key = key;
    }

    get offline() { return this.code === 'OFFLINE'; }
    /** Retrying with the same inputs cannot succeed. */
    get permanent() { return ['REFUSED', 'NOT_FOUND', 'TARGET_EXISTS', 'UNAUTHORIZED'].includes(this.code); }
}

export default class CanvasBackend extends StorageBackend {
    #fetch;
    #url;
    #workspaceId;
    #backend;
    #token;
    #deviceId;
    #deviceName;
    #expectedInstanceId;
    #instanceId = null;
    #prefixes;
    #retryDelays;

    // key → row { key, size, modified, mimeType, checksums:{sha256}, dev, ino, docId }
    #files = new Map();
    #cursor = null;
    #head = 0;
    #lastPing = null;

    #pollTimer = null;
    #polling = null;

    constructor(name, config = {}) {
        super(name, config);
        this.type = 'remote';
        if (!config.url) throw new Error('canvas backend requires url');
        if (!config.workspaceId) throw new Error('canvas backend requires workspaceId');
        this.#fetch = typeof config.fetch === 'function' ? config.fetch : (...args) => globalThis.fetch(...args);
        this.#url = String(config.url).replace(/\/+$/, '');
        this.#workspaceId = String(config.workspaceId);
        this.#backend = String(config.backend || 'workspace:home');
        this.#token = config.token || null;
        this.#deviceId = config.deviceId || null;
        this.#deviceName = config.deviceName || null;
        this.#expectedInstanceId = config.instanceId || null;
        this.#prefixes = Array.isArray(config.prefixes) ? config.prefixes.filter(Boolean) : [];
        this.#retryDelays = Array.isArray(config.retryDelays) ? config.retryDelays : RETRY_DELAYS_MS;
        this.#cursor = config.cursor == null ? null : Number(config.cursor);
        debug(`CanvasBackend "${name}" initialized (${this.#url} ws=${this.#workspaceId} backend=${this.#backend})`);
    }

    get capabilities() { return { read: true, write: true, delete: true, canEnumerate: true, remote: true }; }
    get remote() { return true; }
    /** Own writes never come back as events (origin-filtered) — Stored need not hold keys. */
    get suppressEchoes() { return false; }
    get transport() { return DEV_PREFIX; }
    get watching() { return !!this.#pollTimer; }
    get instanceId() { return this.#instanceId; }
    get deviceId() { return this.#deviceId; }
    get deviceName() { return this.#deviceName; }
    get workspaceId() { return this.#workspaceId; }
    get hubBackend() { return this.#backend; }
    get prefixes() { return [...this.#prefixes]; }
    /** Change-feed position the poller will continue from (null = unset). */
    get cursor() { return this.#cursor; }
    set cursor(seq) { this.#cursor = seq == null ? null : Number(seq); }
    /** Hub change-log head as of the last listing/poll. */
    get head() { return this.#head; }
    get dev() { return `${DEV_PREFIX}:${this.#instanceId || this.#expectedInstanceId || 'unknown'}`; }

    nativeUrl(key) { return this.#objectUrl(key); }

    // ─────────────────────────────────────────────────────────────────────────
    // Transport
    // ─────────────────────────────────────────────────────────────────────────

    #backendUrl() {
        return `${this.#url}/rest/v2/workspaces/${encodeSegment(this.#workspaceId)}/backends/file/${encodeSegment(this.#backend)}`;
    }

    #objectUrl(key) { return `${this.#backendUrl()}/objects/${encodeKey(key)}`; }

    #authHeaders() {
        return {
            ...(this.#token ? { Authorization: `Bearer ${this.#token}` } : {}),
            ...(this.#deviceId ? { 'X-Canvas-Origin': this.#deviceId } : {}),
        };
    }

    // Typed error from a non-success response (body consumed here).
    async #classify(res, key = null) {
        const body = await res.json().catch(() => null);
        const hubCode = body?.code || null;
        const message = body?.message || `HTTP ${res.status}`;
        const payload = body?.payload || {};
        const status = res.status;
        const detail = `${message}${key ? ` (${key})` : ''}`;
        if (status === 401) return new CanvasHubError(detail, { code: 'UNAUTHORIZED', status, hubCode, key });
        if (status === 404) return new CanvasHubError(detail, { code: 'NOT_FOUND', status, hubCode, key });
        if (status === 409 && hubCode === 'TARGET_EXISTS') return new CanvasHubError(detail, { code: 'TARGET_EXISTS', status, hubCode, key });
        if (status === 410) return new CanvasHubError(detail, { code: 'CURSOR_TOO_OLD', status, hubCode, oldest: num(payload.oldest, 0), head: num(payload.head, 0) });
        if (status === 412) {
            const c = payload.current;
            const current = c ? { sha256: lower(c.sha256), size: num(c.size, 0), mtime: parseMs(c.mtime) } : null;
            return new CanvasHubError(detail, { code: 'PRECONDITION_FAILED', status, hubCode, current, key });
        }
        if (status === 429 || status >= 500) return new CanvasHubError(detail, { code: 'RETRYABLE', status, hubCode, key });
        return new CanvasHubError(detail, { code: 'REFUSED', status, hubCode, key });
    }

    /**
     * One request with bounded retries on transport failure / 429 / 5xx. A
     * body given as a function is re-created per attempt; a one-shot stream
     * body is sent once and a retryable status surfaces as `RETRYABLE`.
     * `okStatuses` widens success (304, 404 for stat-like calls).
     */
    async #call(url, init = {}, { okStatuses = [], key = null } = {}) {
        const { body: bodySpec, ...rest } = init;
        const streaming = bodySpec != null && typeof bodySpec !== 'function' && !Buffer.isBuffer(bodySpec) && typeof bodySpec !== 'string';
        let last = null;
        for (let attempt = 0; ; attempt += 1) {
            const body = typeof bodySpec === 'function' ? bodySpec() : bodySpec;
            const options = { ...rest, headers: { ...this.#authHeaders(), ...(rest.headers || {}) } };
            if (body != null) {
                options.body = body;
                if (typeof body === 'object' && typeof body.getReader === 'function') options.duplex = 'half';
            }
            let res;
            try {
                res = await this.#fetch(url, options);
            } catch (err) {
                last = new CanvasHubError(`canvas hub unreachable: ${err?.cause?.message || err.message}`, { code: 'OFFLINE', key });
                this.#lastPing = null;   // a transport failure voids the cached liveness
                if (streaming || attempt >= this.#retryDelays.length) throw last;
                await sleep(this.#retryDelays[attempt]);
                continue;
            }
            if (res.ok || okStatuses.includes(res.status)) return res;
            if (res.status === 429 || res.status >= 500) {
                last = await this.#classify(res, key);
                if (streaming || attempt >= this.#retryDelays.length) throw last;
                const retryAfter = Number(res.headers?.get?.('retry-after')) * 1000;
                await sleep(Number.isFinite(retryAfter) && retryAfter > 0 ? retryAfter : this.#retryDelays[attempt]);
                continue;
            }
            throw await this.#classify(res, key);
        }
    }

    async #json(url, init = {}, extra = {}) {
        const res = await this.#call(url, { ...init, headers: { ...JSON_HEADERS, ...(init.headers || {}) } }, extra);
        if (res.status === 204) return { payload: null, status: 204 };
        const body = await res.json().catch(() => null);
        return { payload: body?.payload ?? null, body, status: res.status };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Liveness
    // ─────────────────────────────────────────────────────────────────────────

    /** `GET /rest/v2/ping` → `{ instanceId, ... }` (uncached). */
    async ping() {
        const { payload } = await this.#json(`${this.#url}/rest/v2/ping`);
        const id = payload?.instanceId || null;
        if (id) this.#instanceId = id;
        this.#lastPing = { at: Date.now(), payload };
        return payload || {};
    }

    /**
     * Liveness gate (file-driver contract): the hub answers and is the
     * instance we were configured for. A successful ping is cached 30 s so a
     * burst of transfers does not ping per object.
     */
    async verifyRoot() {
        try {
            if (!this.#lastPing || Date.now() - this.#lastPing.at > PING_CACHE_MS) await this.ping();
            if (this.#expectedInstanceId && this.#instanceId && this.#instanceId !== this.#expectedInstanceId) {
                return { ok: false, reason: 'instance-mismatch', fsid: null, instanceId: this.#instanceId, expected: this.#expectedInstanceId };
            }
            return { ok: true, fsid: null, instanceId: this.#instanceId };
        } catch (err) {
            this.#lastPing = null;
            const reason = err.code === 'UNAUTHORIZED' ? 'unauthorized' : (err.code === 'OFFLINE' ? 'offline' : 'unreachable');
            return { ok: false, reason, error: err.message, fsid: null };
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Rows
    // ─────────────────────────────────────────────────────────────────────────

    #row(key, { sha256, size, mtime, mimeType = null, docId = null }) {
        const sha = lower(sha256);
        return {
            key,
            size: num(size, 0),
            modified: parseMs(mtime),
            mimeType: mimeType || null,
            checksums: sha ? { sha256: sha } : null,
            dev: this.dev,
            ino: sha,
            docId: docId ?? null,
        };
    }

    #rowFromHeaders(key, headers) {
        const etag = headers.get('x-canvas-sha256') || headers.get('etag');
        const sha256 = etag ? etag.replace(/^W\//, '').replace(/^"|"$/g, '') : null;
        return this.#row(key, {
            sha256,
            size: headers.get('x-canvas-size') ?? headers.get('content-length'),
            mtime: headers.get('x-canvas-mtime') || headers.get('last-modified'),
            mimeType: headers.get('content-type'),
            docId: headers.get('x-canvas-doc-id'),
        });
    }

    #inScope(key) { return matchesPrefixes(key, this.#prefixes); }

    #eventPayload(row) {
        return {
            backend: this.name,
            key: row.key,
            checksums: row.checksums,
            mimeType: row.mimeType,
            size: row.size,
            modified: row.modified,
            dev: row.dev,
            ino: row.ino,
        };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Reads
    // ─────────────────────────────────────────────────────────────────────────

    /** HEAD → row | null. */
    async stat(key) {
        const clean = normalizeKey(key);
        if (!clean) return null;
        const res = await this.#call(this.#objectUrl(clean), { method: 'HEAD' }, { okStatuses: [404], key: clean });
        if (res.status === 404) {
            this.#files.delete(clean);
            return null;
        }
        const row = this.#rowFromHeaders(clean, res.headers);
        this.#files.set(clean, row);
        return { ...row };
    }

    async get(key, options = {}) {
        const clean = normalizeKey(key);
        if (!clean) return null;
        const res = await this.#call(this.#objectUrl(clean), { method: 'GET' }, { okStatuses: [404], key: clean });
        if (res.status === 404) return null;
        this.#files.set(clean, this.#rowFromHeaders(clean, res.headers));
        if (options.stream) return Readable.fromWeb(res.body);
        return Buffer.from(await res.arrayBuffer());
    }

    /** Ranged read; null when the hub ignored the Range (caller falls back). */
    async getRange(key, { start, end }) {
        const clean = normalizeKey(key);
        if (!clean) return null;
        const res = await this.#call(this.#objectUrl(clean), { method: 'GET', headers: { Range: `bytes=${start}-${end}` } }, { okStatuses: [404, 416], key: clean });
        if (res.status === 404 || res.status === 416) return null;
        if (res.status !== 206) {
            await res.arrayBuffer().catch(() => {});
            return null;
        }
        return Readable.fromWeb(res.body);
    }

    /** One page of the hub's listing: `{ objects, cursor, head }`. */
    async listObjects({ prefix = '', cursor = null, limit = LIST_PAGE } = {}) {
        const u = new URL(`${this.#backendUrl()}/objects`);
        if (prefix) u.searchParams.set('prefix', normalizeKey(prefix));
        if (cursor) u.searchParams.set('cursor', cursor);
        u.searchParams.set('limit', String(limit));
        const { payload } = await this.#json(u.toString());
        const objects = (payload?.objects || []).map((o) => this.#row(normalizeKey(o.key), o));
        const head = num(payload?.head, this.#head) ?? 0;
        this.#head = Math.max(this.#head, head);
        return { objects, cursor: payload?.cursor || null, head };
    }

    /** Every object under `prefix` (all pages), scoped to `config.prefixes`. */
    async *list(options = {}) {
        const prefix = normalizeKey(options.prefix || '');
        let cursor = null;
        do {
            const page = await this.listObjects({ prefix, cursor, limit: options.limit || LIST_PAGE });
            for (const row of page.objects) {
                if (!this.#inScope(row.key)) continue;
                yield { ...row };
            }
            cursor = page.cursor;
        } while (cursor);
    }

    /**
     * Full-snapshot scan (file-driver contract): `{ files, complete, errors }`.
     * Rebuilds the row map; when no feed cursor is set yet the listing's head
     * becomes it, so a `watch()` that follows tails from this snapshot.
     */
    async scan(options = {}) {
        const onFile = typeof options.onFile === 'function' ? options.onFile : null;
        const errors = { root: null, dirs: [], files: [] };
        const files = new Map();
        const results = [];
        this.emit('scan:start', { backend: this.name });
        let head = this.#head;
        try {
            let cursor = null;
            do {
                const page = await this.listObjects({ prefix: options.prefix || '', cursor });
                if (page.head) head = Math.max(head, page.head);
                for (const row of page.objects) {
                    if (!this.#inScope(row.key)) continue;
                    files.set(row.key, row);
                    const out = { ...row, backend: this.name };
                    results.push(out);
                    if (onFile) await onFile(out);
                }
                cursor = page.cursor;
            } while (cursor);
        } catch (err) {
            errors.root = err.code || 'unreachable';
            debug(`Scan aborted: ${err.message}`);
            this.emit('scan:complete', { backend: this.name, count: 0, complete: false, errors });
            return { files: [], complete: false, errors, fsid: null };
        }
        this.#files = files;
        this.#head = head;
        if (this.#cursor == null) this.#cursor = head;
        this.emit('scan:complete', { backend: this.name, count: results.length, complete: true, errors });
        debug(`Scan complete: ${results.length} objects (head ${head})`);
        return { files: results, complete: true, errors, fsid: null, head };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Writes
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Streaming `PUT objects/<key>`. `source` is a Buffer | string | Readable |
     * web ReadableStream | `() => Readable` (retryable). Options map 1:1 onto
     * the protocol headers: `ifMatch`, `ifNoneMatch`, `sha256`, `mtime`,
     * `origin`, `conflictOf`, `conflictMode`, `baseSha256`, `deviceName`,
     * `mimeType`, `size`.
     *
     * Returns `{ key, sha256, size, mtime, seq, docId, unchanged, previous,
     * created }`, or for a conflict upload `{ conflict:true, docId, key,
     * conflictOf, sha256, hubDocId, hubSha256 }`. Throws CanvasHubError.
     */
    async putStream(key, source, options = {}) {
        const clean = normalizeKey(key);
        if (!clean) throw new CanvasHubError('canvas put: key is required', { code: 'REFUSED', key });
        const headers = {
            'Content-Type': options.mimeType || 'application/octet-stream',
            ...JSON_HEADERS,
        };
        if (options.origin) headers['X-Canvas-Origin'] = options.origin;
        if (options.ifMatch != null) headers['If-Match'] = quote(options.ifMatch);
        if (options.ifNoneMatch != null) headers['If-None-Match'] = String(options.ifNoneMatch);
        if (options.sha256) headers['X-Canvas-Sha256'] = String(options.sha256).toLowerCase();
        if (options.mtime != null) headers['X-Canvas-Mtime'] = String(Math.round(Number(options.mtime)));
        if (options.conflictOf) {
            headers['X-Canvas-Conflict-Of'] = normalizeKey(options.conflictOf);
            headers['X-Canvas-Conflict-Mode'] = options.conflictMode === 'rename' ? 'rename' : 'inbox';
            if (options.baseSha256) headers['X-Canvas-Base-Sha256'] = String(options.baseSha256).toLowerCase();
            const deviceName = options.deviceName || this.#deviceName;
            if (deviceName) headers['X-Canvas-Device-Name'] = deviceName;
        }
        let body;
        let size = Number.isFinite(options.size) ? options.size : null;
        if (Buffer.isBuffer(source)) { body = source; size = source.length; }
        else if (typeof source === 'string') { body = Buffer.from(source); size = body.length; }
        else if (typeof source === 'function') body = () => this.#toWeb(source());
        else body = this.#toWeb(source);
        if (size != null) headers['Content-Length'] = String(size);

        const { payload, status } = await this.#json(this.#objectUrl(clean), { method: 'PUT', headers, body }, { key: clean });
        if (options.conflictOf) {
            debug(`CONFLICT PUT ${clean} (of ${options.conflictOf})`);
            return { conflict: true, ...(payload || {}), key: clean };
        }
        const result = {
            key: clean,
            sha256: lower(payload?.sha256) || (options.sha256 ? String(options.sha256).toLowerCase() : null),
            size: num(payload?.size, size ?? 0),
            mtime: parseMs(payload?.mtime) ?? (options.mtime != null ? Number(options.mtime) : null),
            seq: num(payload?.seq, 0),
            docId: payload?.docId ?? null,
            unchanged: payload?.unchanged === true,
            previous: payload?.previous || null,
            created: status === 201,
        };
        this.#files.set(clean, this.#row(clean, { sha256: result.sha256, size: result.size, mtime: result.mtime, mimeType: options.mimeType, docId: result.docId }));
        if (result.seq) this.#head = Math.max(this.#head, result.seq);
        debug(`PUT ${clean} (${result.size} bytes, seq ${result.seq}${result.unchanged ? ', unchanged' : ''})`);
        return result;
    }

    #toWeb(source) {
        if (source && typeof source.getReader === 'function') return source;
        if (source && typeof source.pipe === 'function') return Readable.toWeb(source);
        if (source && typeof source[Symbol.asyncIterator] === 'function') return Readable.toWeb(Readable.from(source));
        throw new CanvasHubError('canvas put: unsupported source', { code: 'REFUSED' });
    }

    async put(key, data, options = {}) {
        const result = await this.putStream(key, data, options);
        return { key: result.key, size: result.size, sha256: result.sha256, seq: result.seq };
    }

    /** SyncQueue/commit entry point: stream a file from disk (retryable). */
    async commit(key, srcPath, options = {}) {
        const { size } = await fsp.stat(srcPath);
        return this.putStream(key, () => createReadStream(srcPath), { ...options, size });
    }

    /** `DELETE objects/<key>` (`If-Match` optional). false when already gone. */
    async delete(key, options = {}) {
        const clean = normalizeKey(key);
        if (!clean) return false;
        const headers = { ...JSON_HEADERS };
        if (options.ifMatch != null) headers['If-Match'] = quote(options.ifMatch);
        if (options.origin) headers['X-Canvas-Origin'] = options.origin;
        const res = await this.#call(this.#objectUrl(clean), { method: 'DELETE', headers }, { okStatuses: [404], key: clean });
        this.#files.delete(clean);
        if (res.status === 404) { await res.arrayBuffer().catch(() => {}); return false; }
        const body = await res.json().catch(() => null);
        if (body?.payload?.seq) this.#head = Math.max(this.#head, Number(body.payload.seq) || 0);
        debug(`DELETE ${clean}`);
        return true;
    }

    /** `POST objects/rename` → `{ from, to, sha256, seq, docId }`. */
    async rename(from, to, options = {}) {
        const fromKey = normalizeKey(from);
        const toKey = normalizeKey(to);
        if (!fromKey || !toKey) throw new CanvasHubError('canvas rename: from and to are required', { code: 'REFUSED' });
        const body = { from: fromKey, to: toKey };
        if (options.ifMatch != null) body.ifMatch = String(options.ifMatch).replace(/^W\//, '').replace(/^"|"$/g, '');
        const origin = options.origin || this.#deviceId;
        if (origin) body.origin = origin;
        const { payload } = await this.#json(`${this.#backendUrl()}/objects/rename`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        }, { key: fromKey });
        const row = this.#files.get(fromKey);
        this.#files.delete(fromKey);
        if (row) this.#files.set(toKey, { ...row, key: toKey });
        const seq = num(payload?.seq, 0);
        if (seq) this.#head = Math.max(this.#head, seq);
        debug(`RENAME ${fromKey} -> ${toKey} (seq ${seq})`);
        return { from: fromKey, to: toKey, sha256: lower(payload?.sha256) || row?.checksums?.sha256 || null, seq, docId: payload?.docId ?? null, size: row?.size ?? null, modified: row?.modified ?? null };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Change feed
    // ─────────────────────────────────────────────────────────────────────────

    /** `GET changes?since=&limit=` → `{ changes, head, oldest, cursor }`; throws CURSOR_TOO_OLD on 410. */
    async changes(since = 0, limit = CHANGES_PAGE) {
        const u = new URL(`${this.#backendUrl()}/changes`);
        u.searchParams.set('since', String(Math.max(0, Number(since) || 0)));
        u.searchParams.set('limit', String(limit));
        const { payload } = await this.#json(u.toString());
        const changes = (payload?.changes || []).map((c) => ({
            seq: num(c.seq, 0),
            ts: num(c.ts, null),
            op: c.op,
            key: normalizeKey(c.key),
            from: c.from != null ? normalizeKey(c.from) : null,
            sha256: lower(c.sha256) || null,
            size: num(c.size, 0),
            mtime: parseMs(c.mtime),
            origin: c.origin ?? null,
        }));
        const head = num(payload?.head, 0);
        this.#head = Math.max(this.#head, head);
        return { changes, head, oldest: num(payload?.oldest, null), cursor: num(payload?.cursor, null) };
    }

    /**
     * Poll the feed from the cursor, synthesizing watcher events. Own-origin
     * entries (our writes echoing back) update the row map but are not
     * emitted. Returns `{ ok, applied, cursor, head }` or
     * `{ ok:false, reason:'cursor-too-old'|'offline'|'no-cursor'|<code> }`;
     * a 410 also emits `backend:state { reason:'cursor-too-old' }` and clears
     * the cursor so the host rebuilds from a listing.
     */
    async poll() {
        if (this.#polling) return this.#polling;
        this.#polling = this.#pollOnce().finally(() => { this.#polling = null; });
        return this.#polling;
    }

    async #pollOnce() {
        if (this.#cursor == null) return { ok: false, reason: 'no-cursor' };
        let since = this.#cursor;
        let applied = 0;
        let head = this.#head;
        try {
            for (;;) {
                const page = await this.changes(since, CHANGES_PAGE);
                head = page.head;
                for (const change of page.changes) {
                    this.#applyChange(change);
                    applied += 1;
                    since = Math.max(since, change.seq);
                }
                if (page.changes.length === 0) since = Math.max(since, page.head);
                else if (page.cursor != null && page.changes.length < CHANGES_PAGE) since = Math.max(since, page.cursor);
                this.#cursor = since;
                if (page.changes.length < CHANGES_PAGE) break;
            }
        } catch (err) {
            if (err.code === 'CURSOR_TOO_OLD') {
                debug(`cursor ${since} predates the retained log (oldest ${err.oldest}); rebuild required`);
                this.#cursor = null;
                this.emit('backend:state', { backend: this.name, reason: 'cursor-too-old', oldest: err.oldest, head: err.head, cursor: since });
                return { ok: false, reason: 'cursor-too-old', oldest: err.oldest, head: err.head };
            }
            const reason = err.code === 'OFFLINE' ? 'offline' : (err.code || 'error');
            this.emit('backend:state', { backend: this.name, reason, online: false, error: err.message, cursor: since });
            return { ok: false, reason, error: err.message };
        }
        this.emit('backend:state', { backend: this.name, reason: 'polled', online: true, cursor: this.#cursor, head, applied });
        return { ok: true, applied, cursor: this.#cursor, head };
    }

    #applyChange(change) {
        const own = !!this.#deviceId && change.origin === this.#deviceId;
        const key = change.key;
        if (!key) return;
        if (change.op === 'rename' && change.from) {
            const prev = this.#files.get(change.from) || null;
            this.#files.delete(change.from);
            const row = this.#row(key, { sha256: change.sha256 || prev?.checksums?.sha256, size: change.size ?? prev?.size, mtime: change.mtime ?? prev?.modified, mimeType: prev?.mimeType });
            if (this.#inScope(key)) this.#files.set(key, row);
            if (own) return;
            // unlink(from) + add(to) sharing ino = sha256: Stored pairs them.
            if (this.#inScope(change.from)) this.emit('file:unlink', { backend: this.name, key: change.from, dev: this.dev, ino: row.ino });
            if (this.#inScope(key)) this.emit('file:add', this.#eventPayload(row));
            return;
        }
        if (!this.#inScope(key)) return;
        if (change.op === 'delete') {
            const prev = this.#files.get(key) || null;
            this.#files.delete(key);
            if (own) return;
            this.emit('file:unlink', { backend: this.name, key, dev: this.dev, ino: change.sha256 || prev?.ino || null });
            return;
        }
        if (change.op === 'put') {
            const prev = this.#files.get(key) || null;
            const row = this.#row(key, { sha256: change.sha256, size: change.size, mtime: change.mtime, mimeType: prev?.mimeType });
            this.#files.set(key, row);
            if (own || !row.checksums) return;
            if (prev?.checksums?.sha256 === row.checksums.sha256) return;
            this.emit(prev ? 'file:change' : 'file:add', this.#eventPayload(row));
        }
    }

    /**
     * Start polling the change feed every `pollInterval` ms (default 30 s).
     * Needs a cursor: `config.cursor`, a prior `scan()`, or — when neither —
     * the current head (nothing before it is ever replayed).
     */
    async watch() {
        if (this.#pollTimer) return true;
        if (this.#cursor == null) {
            try {
                const page = await this.changes(0, 1).catch((err) => {
                    if (err.code === 'CURSOR_TOO_OLD') return { head: err.head };
                    throw err;
                });
                this.#cursor = num(page.head, 0);
            } catch (err) {
                this.emit('backend:state', { backend: this.name, reason: err.code === 'OFFLINE' ? 'offline' : 'error', online: false, error: err.message });
                return false;
            }
        }
        const interval = Math.max(1_000, Number(this.config.pollInterval) || DEFAULT_POLL_INTERVAL);
        this.#pollTimer = setInterval(() => { this.poll().catch(() => {}); }, interval);
        this.#pollTimer.unref?.();
        debug(`Watching ${this.#backendUrl()} from seq ${this.#cursor} (poll ${interval}ms)`);
        return true;
    }

    async stop() {
        if (this.#pollTimer) {
            clearInterval(this.#pollTimer);
            this.#pollTimer = null;
            debug('Stopped watching');
        }
        if (this.#polling) await this.#polling.catch(() => {});
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Mirror status
    // ─────────────────────────────────────────────────────────────────────────

    /** `POST /workspaces/:id/mirrors/:deviceId/status` → `{ mirror, head }`. */
    async reportStatus(status = {}) {
        if (!this.#deviceId) throw new CanvasHubError('reportStatus requires a deviceId', { code: 'REFUSED' });
        const url = `${this.#url}/rest/v2/workspaces/${encodeSegment(this.#workspaceId)}/mirrors/${encodeSegment(this.#deviceId)}/status`;
        const { payload } = await this.#json(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ backend: this.#backend, client: 'daemon', ...status }),
        });
        if (payload?.head) this.#head = Math.max(this.#head, Number(payload.head) || 0);
        return payload || {};
    }
}
