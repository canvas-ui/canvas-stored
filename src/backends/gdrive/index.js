import { Readable } from 'stream';
import { createReadStream, promises as fsp } from 'fs';
import Debug from 'debug';
import StorageBackend from '../StorageBackend.js';
import { checksumStream } from '../../utils/checksum.js';

const debug = Debug('stored:backend:gdrive');

/**
 * Google Drive storage backend.
 *
 * One backend = one Drive folder subtree (`config.folderId`, default `root` =
 * the account's My Drive) reached with an offline-access OAuth refresh token
 * (`clientId`, `clientSecret`, `refreshToken` — the same grant the gcal
 * connector uses, with the `drive` scope). Plain `fetch`, no googleapis dep.
 *
 * Key model: Drive is id-addressed, stored is path-addressed. A key is the
 * '/'-joined chain of Drive *names* from the configured root down to the file.
 * Drive allows duplicate names in one folder and '/' inside names — siblings
 * that collide get a ` (<id-prefix>)` suffix, '/' becomes '∕' (U+2215), so
 * every key is unique and round-trips. The key→id map is rebuilt by every
 * walk (scan/list/shape) and kept current by put/delete/watch; a cold lookup
 * resolves the path component-by-component against the API.
 *
 * Identity: Drive serves sha256/sha1/md5 for every binary file, so a scan
 * never downloads bytes unless an algorithm the API lacks is requested. Native
 * Google Docs/Sheets/Slides have no bytes and no checksums — skipped (an
 * export path is a follow-up).
 *
 * `type: 'remote'` — writes land in stored's cache and reach Drive through the
 * SyncQueue (`commit(key, srcPath)`), reads stream straight from the API.
 * `watch()` polls `changes.list` with a page token (true incremental sync;
 * renames/moves surface as unlink+add sharing `ino` = fileId, which stored's
 * rename window collapses into an in-place URL rewrite).
 */

const TOKEN_URL = 'https://oauth2.googleapis.com/token';
const API = 'https://www.googleapis.com/drive/v3';
const UPLOAD_API = 'https://www.googleapis.com/upload/drive/v3';
const FOLDER_MIME = 'application/vnd.google-apps.folder';
const NATIVE_PREFIX = 'application/vnd.google-apps.';
const FILE_FIELDS = 'id,name,mimeType,size,modifiedTime,createdTime,md5Checksum,sha1Checksum,sha256Checksum,parents,trashed';
const PAGE_SIZE = 1000;
// Resumable-upload chunk: Drive requires non-final chunks to be a multiple of
// 256 KiB; 8 MiB keeps request count low without holding much in RAM.
const UPLOAD_CHUNK = 8 * 1024 * 1024;
const DEFAULT_POLL_INTERVAL = 60_000;
// Walks are memoized briefly: a resync calls shape() then scan() back to back
// and must not enumerate a large Drive twice.
const WALK_CACHE_MS = 30_000;
const DEV = 'gdrive';
const API_HASH_FIELDS = { sha256: 'sha256Checksum', sha1: 'sha1Checksum', md5: 'md5Checksum' };
const RETRY_DELAYS_MS = [500, 1500, 4000];

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const qEscape = (s) => String(s).replace(/\\/g, '\\\\').replace(/'/g, "\\'");
const safeName = (name) => String(name).replace(/\//g, '∕');
const splitKey = (key) => {
    const k = String(key || '').replace(/\/{2,}/g, '/').replace(/^\/+|\/+$/g, '');
    const slash = k.lastIndexOf('/');
    return slash < 0 ? { dir: '', name: k, key: k } : { dir: k.slice(0, slash), name: k.slice(slash + 1), key: k };
};

class DriveError extends Error {
    constructor(message, { code = 'unreachable', status = null } = {}) {
        super(message);
        this.name = 'DriveError';
        this.code = code;
        this.status = status;
    }
}

export default class GdriveBackend extends StorageBackend {
    #fetch;
    #rootRef;
    #rootId = null;
    #accessToken = null;
    #tokenExpiresAt = 0;
    #defaultAlgorithms;

    // key → row { key, id, size, modified, created, mimeType, checksums, dev, ino }
    #files = new Map();
    // id → key (files only)
    #keyById = new Map();
    // folder path ('' = root) ↔ folder id
    #folderIdByPath = new Map();
    #folderPathById = new Map();
    #lastWalk = null;

    #pollTimer = null;
    #polling = false;
    #pageToken = null;

    constructor(name, config = {}) {
        super(name, config);
        this.type = 'remote';
        this.#fetch = typeof config.fetch === 'function' ? config.fetch : (...args) => globalThis.fetch(...args);
        this.#rootRef = String(config.folderId || 'root').trim() || 'root';
        this.#defaultAlgorithms = config.algorithms || ['sha256'];
        debug(`GdriveBackend "${name}" initialized (folder=${this.#rootRef})`);
    }

    get capabilities() { return { read: true, write: true, delete: true, canEnumerate: true, remote: true }; }
    get remote() { return true; }
    get transport() { return DEV; }
    get watching() { return !!this.#pollTimer; }
    get rootFolderId() { return this.#rootId; }

    nativeUrl(key) {
        const row = this.#files.get(splitKey(key).key);
        return row ? `https://drive.google.com/file/d/${row.id}/view` : null;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Auth + transport
    // ─────────────────────────────────────────────────────────────────────────

    async #token({ force = false } = {}) {
        if (!force && this.#accessToken && Date.now() < this.#tokenExpiresAt - 60_000) return this.#accessToken;
        const { clientId, clientSecret, refreshToken } = this.config;
        if (!clientId || !clientSecret || !refreshToken) {
            throw new DriveError('gdrive backend requires clientId, clientSecret and refreshToken', { code: 'unauthorized' });
        }
        let res;
        try {
            res = await this.#fetch(TOKEN_URL, {
                method: 'POST',
                headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                body: new URLSearchParams({
                    client_id: clientId,
                    client_secret: clientSecret,
                    refresh_token: refreshToken,
                    grant_type: 'refresh_token',
                }),
            });
        } catch (err) {
            throw new DriveError(`gdrive token endpoint unreachable: ${err.message}`, { code: 'unreachable' });
        }
        const json = await res.json().catch(() => null);
        if (!res.ok || !json?.access_token) {
            const detail = json?.error_description || json?.error || `HTTP ${res.status}`;
            throw new DriveError(`gdrive token refresh failed: ${detail}`, { code: 'unauthorized', status: res.status });
        }
        this.#accessToken = json.access_token;
        this.#tokenExpiresAt = Date.now() + (Number(json.expires_in) || 3600) * 1000;
        return this.#accessToken;
    }

    /**
     * Authenticated request with one forced token refresh on 401 and bounded
     * backoff on 429/5xx. Returns the Response; `okStatuses` widens success
     * (resumable uploads answer 308 between chunks). Throws DriveError.
     */
    async #call(url, init = {}, { okStatuses = [], allow404 = false } = {}) {
        let refreshed = false;
        for (let attempt = 0; ; attempt += 1) {
            const token = await this.#token();
            let res;
            try {
                res = await this.#fetch(url, { ...init, headers: { Authorization: `Bearer ${token}`, ...(init.headers || {}) } });
            } catch (err) {
                if (attempt < RETRY_DELAYS_MS.length) { await sleep(RETRY_DELAYS_MS[attempt]); continue; }
                throw new DriveError(`gdrive request failed: ${err.message}`, { code: 'unreachable' });
            }
            if (res.ok || okStatuses.includes(res.status)) return res;
            if (res.status === 404 && allow404) return res;
            if (res.status === 401 && !refreshed) {
                refreshed = true;
                await this.#token({ force: true });
                continue;
            }
            if ((res.status === 429 || res.status >= 500) && attempt < RETRY_DELAYS_MS.length) {
                const retryAfter = Number(res.headers?.get?.('retry-after')) * 1000;
                await sleep(Number.isFinite(retryAfter) && retryAfter > 0 ? retryAfter : RETRY_DELAYS_MS[attempt]);
                continue;
            }
            const body = await res.json().catch(() => null);
            const detail = body?.error?.message || body?.error_description || `HTTP ${res.status}`;
            const code = res.status === 401 || res.status === 403 ? 'unauthorized' : (res.status === 404 ? 'not-found' : 'unreachable');
            throw new DriveError(`gdrive ${init.method || 'GET'} ${new URL(url).pathname}: ${detail}`, { code, status: res.status });
        }
    }

    #url(path, query = {}) {
        const u = new URL(path.startsWith('http') ? path : `${API}${path}`);
        for (const [k, v] of Object.entries(query)) if (v !== undefined && v !== null) u.searchParams.set(k, String(v));
        return u.toString();
    }

    async #json(path, { query = {}, ...init } = {}) {
        const res = await this.#call(this.#url(path, query), init);
        return res.status === 204 ? null : res.json();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Root + liveness
    // ─────────────────────────────────────────────────────────────────────────

    async #root() {
        if (this.#rootId) return this.#rootId;
        const res = await this.#call(this.#url(`/files/${encodeURIComponent(this.#rootRef)}`, { fields: 'id,name,mimeType,trashed', supportsAllDrives: true }), {}, { allow404: true });
        if (res.status === 404) throw new DriveError(`gdrive root folder not found: ${this.#rootRef}`, { code: 'root-missing', status: 404 });
        const f = await res.json();
        if (f.trashed) throw new DriveError(`gdrive root folder is trashed: ${this.#rootRef}`, { code: 'root-missing' });
        if (f.mimeType !== FOLDER_MIME) throw new DriveError(`gdrive root is not a folder: ${this.#rootRef}`, { code: 'root-missing' });
        this.#rootId = f.id;
        this.#rememberFolder('', f.id);
        return f.id;
    }

    /**
     * Liveness gate (same contract as the file driver's): token grant works and
     * the root folder exists. `fsid` is null — Drive has no filesystem identity
     * to snapshot; the folder id already is one.
     */
    async verifyRoot() {
        try {
            await this.#root();
            return { ok: true, fsid: null };
        } catch (err) {
            return { ok: false, reason: err.code || 'unreachable', error: err.message, fsid: null };
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Path ↔ id bookkeeping
    // ─────────────────────────────────────────────────────────────────────────

    #rememberFolder(path, id) {
        this.#folderIdByPath.set(path, id);
        this.#folderPathById.set(id, path);
    }

    #remember(row) {
        this.#files.set(row.key, row);
        this.#keyById.set(row.id, row.key);
    }

    #forget(key) {
        const row = this.#files.get(key);
        if (!row) return null;
        this.#files.delete(key);
        if (this.#keyById.get(row.id) === key) this.#keyById.delete(row.id);
        return row;
    }

    #apiChecksums(f) {
        const out = {};
        for (const [algo, field] of Object.entries(API_HASH_FIELDS)) if (f[field]) out[algo] = String(f[field]).toLowerCase();
        return Object.keys(out).length ? out : null;
    }

    #row(key, f) {
        return {
            key,
            id: f.id,
            size: Number(f.size) || 0,
            modified: f.modifiedTime ? Date.parse(f.modifiedTime) : null,
            created: f.createdTime ? Date.parse(f.createdTime) : null,
            mimeType: f.mimeType || null,
            checksums: this.#apiChecksums(f),
            // Inode-equivalent: the Drive id survives renames/moves, so stored
            // can pair an unlink+add into a rename instead of delete+create.
            dev: DEV,
            ino: f.id,
        };
    }

    // Every non-trashed child of a folder, all pages.
    async #children(folderId, { foldersOnly = false } = {}) {
        const rows = [];
        let pageToken;
        const q = `'${qEscape(folderId)}' in parents and trashed = false${foldersOnly ? ` and mimeType = '${FOLDER_MIME}'` : ''}`;
        do {
            const res = await this.#json('/files', {
                query: {
                    q,
                    fields: `nextPageToken,files(${FILE_FIELDS})`,
                    pageSize: PAGE_SIZE,
                    pageToken,
                    supportsAllDrives: true,
                    includeItemsFromAllDrives: true,
                },
            });
            rows.push(...(res?.files || []));
            pageToken = res?.nextPageToken;
        } while (pageToken);
        return rows;
    }

    // Deterministic sibling naming: sorted by (name, id); a repeated name gets
    // an id-derived suffix so keys stay unique and stable across walks.
    #nameSiblings(entries) {
        const sorted = [...entries].sort((a, b) => a.name.localeCompare(b.name) || a.id.localeCompare(b.id));
        const seen = new Map();
        return sorted.map((f) => {
            const base = safeName(f.name);
            const taken = seen.get(base) || 0;
            seen.set(base, taken + 1);
            if (!taken) return { f, name: base };
            const dot = base.lastIndexOf('.');
            const tag = ` (${f.id.slice(0, 6)})`;
            return { f, name: dot > 0 ? `${base.slice(0, dot)}${tag}${base.slice(dot)}` : `${base}${tag}` };
        });
    }

    /**
     * Enumerate the whole subtree (breadth-first, one list call per folder) and
     * rebuild the path maps from it. Unreadable folders are reported through
     * `onError(prefix, err)` and skipped — never fatal, never "empty".
     */
    async #walk({ onError = null, maxAge = 0 } = {}) {
        if (maxAge > 0 && this.#lastWalk && Date.now() - this.#lastWalk.at < maxAge) return this.#lastWalk.result;
        const rootId = await this.#root();
        const files = new Map();
        const keyById = new Map();
        const folderIdByPath = new Map([['', rootId]]);
        const folderPathById = new Map([[rootId, '']]);
        const dirs = [];
        const queue = [{ prefix: '', id: rootId }];
        while (queue.length) {
            const { prefix, id } = queue.shift();
            let entries;
            try {
                entries = await this.#children(id);
            } catch (err) {
                if (typeof onError === 'function') { onError(prefix, err); continue; }
                throw err;
            }
            for (const { f, name } of this.#nameSiblings(entries)) {
                const key = prefix ? `${prefix}/${name}` : name;
                if (f.mimeType === FOLDER_MIME) {
                    folderIdByPath.set(key, f.id);
                    folderPathById.set(f.id, key);
                    dirs.push(key);
                    queue.push({ prefix: key, id: f.id });
                    continue;
                }
                // Native Google documents have no bytes (and no checksums).
                if (f.mimeType && f.mimeType.startsWith(NATIVE_PREFIX)) continue;
                const row = this.#row(key, f);
                files.set(key, row);
                keyById.set(f.id, key);
            }
        }
        this.#files = files;
        this.#keyById = keyById;
        this.#folderIdByPath = folderIdByPath;
        this.#folderPathById = folderPathById;
        const result = { files, dirs };
        this.#lastWalk = { at: Date.now(), result };
        return result;
    }

    #invalidateWalk() { this.#lastWalk = null; }

    /**
     * Folder id for a '/'-path under the root. Walks down component by
     * component against the API on a cache miss; `create` mkdir -p's the chain.
     * Returns null when a component is missing and `create` is false.
     */
    async #folderId(dirPath, { create = false } = {}) {
        const clean = String(dirPath || '').replace(/\/{2,}/g, '/').replace(/^\/+|\/+$/g, '');
        if (!clean) return this.#root();
        const cached = this.#folderIdByPath.get(clean);
        if (cached) return cached;
        const { dir, name } = splitKey(clean);
        const parentId = await this.#folderId(dir, { create });
        if (!parentId) return null;
        const found = await this.#json('/files', {
            query: {
                q: `'${qEscape(parentId)}' in parents and name = '${qEscape(name)}' and mimeType = '${FOLDER_MIME}' and trashed = false`,
                fields: 'files(id,name)',
                pageSize: 10,
                supportsAllDrives: true,
                includeItemsFromAllDrives: true,
            },
        });
        const match = (found?.files || []).sort((a, b) => a.id.localeCompare(b.id))[0];
        if (match) {
            this.#rememberFolder(clean, match.id);
            return match.id;
        }
        if (!create) return null;
        const created = await this.#json('/files', {
            method: 'POST',
            query: { fields: 'id', supportsAllDrives: true },
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name, mimeType: FOLDER_MIME, parents: [parentId] }),
        });
        this.#rememberFolder(clean, created.id);
        this.#invalidateWalk();
        debug(`MKDIR ${clean} (${created.id})`);
        return created.id;
    }

    /**
     * Path of a folder id relative to the root ('' = root), or null when the
     * folder is not under it (shared-with-me, another drive, trashed).
     * Resolved upward through `parents`, memoized per folder.
     */
    async #folderPath(folderId) {
        if (folderId === (await this.#root())) return '';
        if (this.#folderPathById.has(folderId)) return this.#folderPathById.get(folderId);
        const res = await this.#call(this.#url(`/files/${encodeURIComponent(folderId)}`, { fields: 'id,name,parents,trashed,mimeType', supportsAllDrives: true }), {}, { allow404: true });
        if (res.status === 404) return null;
        const f = await res.json();
        if (f.trashed || !f.parents?.length) return null;
        const parentPath = await this.#folderPath(f.parents[0]);
        if (parentPath === null) return null;
        const path = parentPath ? `${parentPath}/${safeName(f.name)}` : safeName(f.name);
        this.#rememberFolder(path, f.id);
        return path;
    }

    // Key of a file resource, or null when it is outside the root subtree.
    async #keyFor(f) {
        const parent = f.parents?.[0];
        if (!parent) return null;
        const dir = await this.#folderPath(parent);
        if (dir === null) return null;
        return dir ? `${dir}/${safeName(f.name)}` : safeName(f.name);
    }

    // Cold lookup of one key against the API (no walk). Caches the row.
    async #lookup(key) {
        const { dir, name, key: clean } = splitKey(key);
        if (!clean) return null;
        const cached = this.#files.get(clean);
        if (cached) return cached;
        const parentId = await this.#folderId(dir, { create: false });
        if (!parentId) return null;
        const found = await this.#json('/files', {
            query: {
                q: `'${qEscape(parentId)}' in parents and name = '${qEscape(name)}' and mimeType != '${FOLDER_MIME}' and trashed = false`,
                fields: `files(${FILE_FIELDS})`,
                pageSize: 10,
                supportsAllDrives: true,
                includeItemsFromAllDrives: true,
            },
        });
        const match = (found?.files || [])
            .filter((f) => !(f.mimeType || '').startsWith(NATIVE_PREFIX))
            .sort((a, b) => a.id.localeCompare(b.id))[0];
        if (!match) return null;
        const row = this.#row(clean, match);
        this.#remember(row);
        return row;
    }

    async #ensureChecksums(row, algorithms) {
        const have = row.checksums || {};
        if (algorithms.every((a) => have[a])) {
            return Object.fromEntries(algorithms.map((a) => [a, have[a]]));
        }
        const stream = await this.#download(row.id, { stream: true });
        if (!stream) return null;
        const hashed = await checksumStream(stream, algorithms);
        return { ...have, ...hashed };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CRUD
    // ─────────────────────────────────────────────────────────────────────────

    async #download(id, { stream = false, range = null } = {}) {
        const headers = range ? { Range: `bytes=${range.start}-${range.end}` } : {};
        const res = await this.#call(
            this.#url(`/files/${encodeURIComponent(id)}`, { alt: 'media', supportsAllDrives: true }),
            { headers },
            { allow404: true },
        );
        if (res.status === 404) return null;
        if (range && res.status !== 206) {
            // Server ignored the Range — drain and report "not ranged".
            await res.arrayBuffer().catch(() => {});
            return null;
        }
        if (stream) return Readable.fromWeb(res.body);
        return Buffer.from(await res.arrayBuffer());
    }

    async get(key, options = {}) {
        const row = await this.#lookup(key);
        if (!row) return null;
        return this.#download(row.id, { stream: !!options.stream });
    }

    // Ranged read for HTTP Range / media streaming; null → caller falls back
    // to a full stream.
    async getRange(key, { start, end }) {
        const row = await this.#lookup(key);
        if (!row) return null;
        return this.#download(row.id, { stream: true, range: { start, end } });
    }

    async stat(key) {
        const row = await this.#lookup(key);
        return row ? { ...row } : null;
    }

    /**
     * Upload `data` (Buffer | string | Readable) to `key`, creating missing
     * folders. An existing file at the key is updated in place (same id →
     * sharing/links survive). Resumable upload throughout; `options.size` lets
     * a stream be announced up front but is not required.
     */
    async put(key, data, options = {}) {
        const { dir, name, key: clean } = splitKey(key);
        if (!clean) throw new Error('gdrive put: key is required');
        const parentId = await this.#folderId(dir, { create: true });
        const existing = await this.#lookup(clean);
        const mimeType = options.mimeType || 'application/octet-stream';
        const source = Buffer.isBuffer(data) ? data : (typeof data === 'string' ? Buffer.from(data) : data);
        const size = Buffer.isBuffer(source) ? source.length : (Number.isFinite(options.size) ? options.size : null);

        const initUrl = existing
            ? this.#url(`${UPLOAD_API}/files/${encodeURIComponent(existing.id)}`, { uploadType: 'resumable', supportsAllDrives: true, fields: FILE_FIELDS })
            : this.#url(`${UPLOAD_API}/files`, { uploadType: 'resumable', supportsAllDrives: true, fields: FILE_FIELDS });
        const init = await this.#call(initUrl, {
            method: existing ? 'PATCH' : 'POST',
            headers: {
                'Content-Type': 'application/json; charset=UTF-8',
                'X-Upload-Content-Type': mimeType,
                ...(size != null ? { 'X-Upload-Content-Length': String(size) } : {}),
            },
            body: JSON.stringify(existing ? {} : { name, parents: [parentId] }),
        });
        const session = init.headers.get('location');
        if (!session) throw new DriveError('gdrive upload: no resumable session URL returned');

        const file = await this.#uploadChunks(session, source, mimeType);
        const row = this.#row(clean, file);
        this.#remember(row);
        this.#invalidateWalk();
        debug(`PUT ${clean} (${row.id}, ${row.size} bytes)`);
        return { key: clean, id: row.id, size: row.size };
    }

    async #putChunk(session, buf, offset, total, mimeType) {
        const last = total != null;
        const range = buf.length
            ? `bytes ${offset}-${offset + buf.length - 1}/${last ? total : '*'}`
            : `bytes */${last ? total : '*'}`;
        const res = await this.#call(session, {
            method: 'PUT',
            headers: { 'Content-Length': String(buf.length), 'Content-Range': range, 'Content-Type': mimeType },
            body: buf,
        }, { okStatuses: [308] });
        if (last) {
            if (res.status === 308) throw new DriveError('gdrive upload: session incomplete after final chunk');
            return res.json();
        }
        if (res.status !== 308) throw new DriveError(`gdrive upload: unexpected ${res.status} on intermediate chunk`);
        return null;
    }

    async #uploadChunks(session, source, mimeType) {
        if (Buffer.isBuffer(source)) return this.#putChunk(session, source, 0, source.length, mimeType);
        // Stream of unknown length: buffer one chunk ahead so the final chunk
        // can carry the total (Drive only learns the size at the end).
        let offset = 0;
        let pending = [];
        let pendingLen = 0;
        for await (const chunk of source) {
            const c = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
            pending.push(c);
            pendingLen += c.length;
            while (pendingLen > UPLOAD_CHUNK) {
                const all = Buffer.concat(pending, pendingLen);
                const head = all.subarray(0, UPLOAD_CHUNK);
                const rest = all.subarray(UPLOAD_CHUNK);
                await this.#putChunk(session, head, offset, null, mimeType);
                offset += head.length;
                pending = rest.length ? [rest] : [];
                pendingLen = rest.length;
            }
        }
        const tail = Buffer.concat(pending, pendingLen);
        return this.#putChunk(session, tail, offset, offset + tail.length, mimeType);
    }

    // SyncQueue entry point: stream an already-written file (stored cache
    // content) to Drive. Size is known, so the upload is announced up front.
    async commit(key, srcPath) {
        const { size } = await fsp.stat(srcPath);
        return this.put(key, createReadStream(srcPath), { size });
    }

    /**
     * Remove the bytes at `key`. Trashes by default — a user's Drive is shared
     * with their other tools and the trash is their safety net; set
     * `config.permanentDelete: true` to bypass it. Trashed files are invisible
     * to every listing here, so stored's semantics hold either way.
     */
    async delete(key) {
        const row = await this.#lookup(key);
        if (!row) return false;
        await this.#trashOrDelete(row.id);
        this.#forget(row.key);
        this.#invalidateWalk();
        debug(`DELETE ${row.key} (${row.id})`);
        return true;
    }

    async #trashOrDelete(id) {
        if (this.config.permanentDelete === true) {
            await this.#call(this.#url(`/files/${encodeURIComponent(id)}`, { supportsAllDrives: true }), { method: 'DELETE' }, { allow404: true });
            return;
        }
        await this.#call(this.#url(`/files/${encodeURIComponent(id)}`, { supportsAllDrives: true, fields: 'id' }), {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ trashed: true }),
        }, { allow404: true });
    }

    async *list(options = {}) {
        const prefix = String(options.prefix || '').replace(/^\/+|\/+$/g, '');
        const { files } = await this.#walk({ onError: options.onError, maxAge: options.maxAge ?? 0 });
        for (const row of files.values()) {
            if (prefix && row.key !== prefix && !row.key.startsWith(`${prefix}/`)) continue;
            yield { ...row };
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Containers (folders)
    // ─────────────────────────────────────────────────────────────────────────

    #dropSubtree(prefix) {
        for (const key of [...this.#files.keys()]) {
            if (key === prefix || key.startsWith(`${prefix}/`)) this.#forget(key);
        }
        for (const [path, id] of [...this.#folderIdByPath]) {
            if (path === prefix || path.startsWith(`${prefix}/`)) {
                this.#folderIdByPath.delete(path);
                this.#folderPathById.delete(id);
            }
        }
    }

    #moveSubtree(from, to) {
        const rekey = (k) => (k === from ? to : `${to}${k.slice(from.length)}`);
        const moved = [];
        for (const [key, row] of [...this.#files]) {
            if (key !== from && !key.startsWith(`${from}/`)) continue;
            this.#forget(key);
            const next = { ...row, key: rekey(key) };
            this.#remember(next);
            moved.push({ from: key, row: next });
        }
        for (const [path, id] of [...this.#folderIdByPath]) {
            if (path !== from && !path.startsWith(`${from}/`)) continue;
            this.#folderIdByPath.delete(path);
            this.#rememberFolder(rekey(path), id);
        }
        return moved;
    }

    async createContainer(key) {
        const clean = splitKey(key).key;
        if (!clean) throw new Error('Container key is required');
        await this.#folderId(clean, { create: true });
        return { key: clean };
    }

    async deleteContainer(key) {
        const clean = splitKey(key).key;
        if (!clean) throw new Error('Cannot delete the backend root');
        const id = await this.#folderId(clean, { create: false });
        if (!id) return false;
        await this.#trashOrDelete(id);
        this.#dropSubtree(clean);
        this.#invalidateWalk();
        debug(`RMDIR ${clean} (${id})`);
        return true;
    }

    async renameContainer(fromKey, toKey) {
        const from = splitKey(fromKey).key;
        const { dir: toDir, name: toName, key: to } = splitKey(toKey);
        if (!from || !to) throw new Error('Container keys are required');
        const id = await this.#folderId(from, { create: false });
        if (!id) throw new Error(`Container not found: ${fromKey}`);
        if (await this.#folderId(to, { create: false })) throw new Error(`Container already exists: ${toKey}`);
        const oldParent = await this.#folderId(splitKey(from).dir, { create: false });
        const newParent = await this.#folderId(toDir, { create: true });
        await this.#call(this.#url(`/files/${encodeURIComponent(id)}`, {
            supportsAllDrives: true,
            fields: 'id',
            ...(oldParent !== newParent ? { addParents: newParent, removeParents: oldParent } : {}),
        }), {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name: toName }),
        });
        this.#moveSubtree(from, to);
        this.#invalidateWalk();
        debug(`MV ${from} -> ${to}`);
        return { from, to };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Shape / scan / watch
    // ─────────────────────────────────────────────────────────────────────────

    async shape() {
        const { files, dirs } = await this.#walk({ onError: () => {}, maxAge: WALK_CACHE_MS });
        return { dirs: [...dirs], files: files.size };
    }

    /**
     * Full-snapshot scan, same contract as the file driver:
     * { files, complete, errors:{root,dirs,files} }. Checksums come from the
     * API (sha256/sha1/md5) — bytes are only pulled for algorithms Drive does
     * not serve, and never when `knownChecksums` already vouches for the key.
     */
    async scan(options = {}) {
        const algorithms = options.algorithms || this.#defaultAlgorithms;
        const known = typeof options.knownChecksums === 'function' ? options.knownChecksums : null;
        const onFile = typeof options.onFile === 'function' ? options.onFile : null;
        const errors = { root: null, dirs: [], files: [] };
        const results = [];
        this.emit('scan:start', { backend: this.name });

        let walk;
        try {
            walk = await this.#walk({
                maxAge: WALK_CACHE_MS,
                onError: (prefix, err) => errors.dirs.push({ prefix, code: err?.code || 'EUNKNOWN' }),
            });
        } catch (err) {
            errors.root = err.code || 'unreachable';
            debug(`Scan aborted: ${err.message}`);
            this.emit('scan:complete', { backend: this.name, count: 0, complete: false, errors });
            return { files: [], complete: false, errors, fsid: null };
        }

        for (const row of walk.files.values()) {
            let checksums = null;
            const have = row.checksums || {};
            if (algorithms.every((a) => have[a])) {
                checksums = Object.fromEntries(algorithms.map((a) => [a, have[a]]));
            } else {
                const cached = known ? known(row.key, { size: row.size, mtime: row.modified, dev: row.dev, ino: row.ino }) : null;
                if (cached) {
                    checksums = cached.checksums;
                } else {
                    checksums = await this.#ensureChecksums(row, algorithms).catch(() => null);
                    if (!checksums) errors.files.push({ key: row.key });
                }
            }
            const out = { ...row, checksums, backend: this.name };
            results.push(out);
            if (onFile) await onFile(out);
        }

        const complete = errors.dirs.length === 0;
        this.emit('scan:complete', { backend: this.name, count: results.length, complete, errors });
        debug(`Scan complete: ${results.length} files (${complete ? 'full' : `partial — ${errors.dirs.length} unreadable folders`})`);
        return { files: results, complete, errors, fsid: null };
    }

    /**
     * Incremental change feed: `changes.list` from a start page token, polled
     * every `config.pollInterval` ms (default 60s). Emits file:add /
     * file:change / file:unlink with the same payload shape as the file
     * driver's watcher. Returns false (and emits 'error') when the feed can't
     * be opened — a watch that looks live but sees nothing is worse than none.
     */
    async watch() {
        if (this.#pollTimer) return true;
        try {
            await this.#root();
            if (!this.#pageToken) {
                const res = await this.#json('/changes/startPageToken', { query: { supportsAllDrives: true } });
                this.#pageToken = res?.startPageToken || null;
            }
            if (!this.#pageToken) throw new DriveError('gdrive changes feed returned no start token');
        } catch (err) {
            this.emit('error', err);
            return false;
        }
        const interval = Math.max(5_000, Number(this.config.pollInterval) || DEFAULT_POLL_INTERVAL);
        this.#pollTimer = setInterval(() => {
            this.poll().catch((err) => this.emit('error', err));
        }, interval);
        this.#pollTimer.unref?.();
        debug(`Watching ${this.#rootRef} (poll ${interval}ms)`);
        return true;
    }

    /** One pass over pending changes. Public so a host can trigger it on demand. */
    async poll() {
        if (this.#polling || !this.#pageToken) return false;
        this.#polling = true;
        try {
            let token = this.#pageToken;
            while (token) {
                const res = await this.#json('/changes', {
                    query: {
                        pageToken: token,
                        pageSize: PAGE_SIZE,
                        fields: `nextPageToken,newStartPageToken,changes(changeType,removed,fileId,file(${FILE_FIELDS}))`,
                        supportsAllDrives: true,
                        includeItemsFromAllDrives: true,
                        includeRemoved: true,
                    },
                });
                for (const change of res?.changes || []) {
                    try { await this.#applyChange(change); }
                    catch (err) { this.emit('error', err); }
                }
                if (res?.newStartPageToken) { this.#pageToken = res.newStartPageToken; break; }
                token = res?.nextPageToken || null;
            }
            return true;
        } finally {
            this.#polling = false;
        }
    }

    async #applyChange(change) {
        if (change.changeType && change.changeType !== 'file') return;
        const id = change.fileId;
        const f = change.file;
        const oldKey = this.#keyById.get(id);
        const gone = change.removed || !f || f.trashed;

        if (gone) {
            if (oldKey) {
                this.#forget(oldKey);
                this.#invalidateWalk();
                this.emit('file:unlink', { backend: this.name, key: oldKey, dev: DEV, ino: id });
            } else if (this.#folderPathById.has(id)) {
                const prefix = this.#folderPathById.get(id);
                const rows = [...this.#files.values()].filter((r) => r.key.startsWith(`${prefix}/`));
                this.#dropSubtree(prefix);
                this.#invalidateWalk();
                for (const row of rows) this.emit('file:unlink', { backend: this.name, key: row.key, dev: DEV, ino: row.id });
            }
            return;
        }

        if (f.mimeType === FOLDER_MIME) {
            // A folder rename/move re-keys everything under it. Emit the pairs
            // (unlink old + add new, same ino) so stored rewrites in place.
            const oldPath = this.#folderPathById.get(id);
            if (oldPath === undefined || oldPath === '') return;
            this.#folderPathById.delete(id);
            this.#folderIdByPath.delete(oldPath);
            const newPath = await this.#keyFor(f);
            if (newPath === null) {
                const rows = [...this.#files.values()].filter((r) => r.key.startsWith(`${oldPath}/`));
                this.#dropSubtree(oldPath);
                this.#invalidateWalk();
                for (const row of rows) this.emit('file:unlink', { backend: this.name, key: row.key, dev: DEV, ino: row.id });
                return;
            }
            this.#rememberFolder(oldPath, id); // restore for #moveSubtree's rekey pass
            if (newPath === oldPath) return;
            const moved = this.#moveSubtree(oldPath, newPath);
            this.#invalidateWalk();
            for (const { from, row } of moved) {
                this.emit('file:unlink', { backend: this.name, key: from, dev: DEV, ino: row.id });
                this.emit('file:add', this.#eventPayload(row));
            }
            return;
        }

        if (f.mimeType && f.mimeType.startsWith(NATIVE_PREFIX)) return;

        const newKey = await this.#keyFor(f);
        if (newKey === null) {
            if (oldKey) {
                this.#forget(oldKey);
                this.#invalidateWalk();
                this.emit('file:unlink', { backend: this.name, key: oldKey, dev: DEV, ino: id });
            }
            return;
        }

        const prev = oldKey === newKey ? this.#files.get(oldKey) : null;
        const row = this.#row(newKey, f);
        row.checksums = await this.#ensureChecksums(row, this.#defaultAlgorithms).catch(() => row.checksums);
        if (oldKey && oldKey !== newKey) {
            this.#forget(oldKey);
            this.emit('file:unlink', { backend: this.name, key: oldKey, dev: DEV, ino: id });
        }
        this.#remember(row);
        this.#invalidateWalk();

        if (prev) {
            const same = prev.modified === row.modified
                && JSON.stringify(prev.checksums || null) === JSON.stringify(row.checksums || null);
            if (same) return;
            this.emit('file:change', this.#eventPayload(row));
            return;
        }
        this.emit('file:add', this.#eventPayload(row));
    }

    #eventPayload(row) {
        return {
            backend: this.name,
            key: row.key,
            checksums: row.checksums,
            mimeType: row.mimeType,
            size: row.size,
            modified: row.modified,
            dev: DEV,
            ino: row.id,
        };
    }

    async stop() {
        if (this.#pollTimer) {
            clearInterval(this.#pollTimer);
            this.#pollTimer = null;
            debug(`Stopped watching ${this.#rootRef}`);
        }
    }
}
