import EventEmitter2 from 'eventemitter2';
import path from 'path';
import crypto from 'crypto';
import { createReadStream, createWriteStream, promises as fsp } from 'fs';
import { once } from 'events';
import { pipeline } from 'stream/promises';
import Debug from 'debug';
import Cache from './cache/index.js';
import BackendManager from './backends/BackendManager.js';
import Index from './index/index.js';
import SyncQueue from './sync/SyncQueue.js';
import { isBuffer, isFile, isStream, resolveStoredPaths } from './utils/common.js';
import { checksumBuffer, formatId } from './utils/checksum.js';
import { detectMimeType, detectMimeFromHead } from './utils/mime.js';

const debug = Debug('stored');

const HEAD_BYTES = 4096;

export default class Stored extends EventEmitter2 {
    #cache;
    #backends;
    #index;
    #config;
    #paths;
    #syncQueue;

    constructor(config = {}) {
        // Wildcards (':' delimiter) let consumers bind `object:*` across backends.
        super({ wildcard: true, delimiter: ':', maxListeners: 100, verboseMemoryLeak: false });
        this.#paths = resolveStoredPaths(config);
        this.#config = {
            checksums: config.checksums || ['sha256'],
            primaryChecksum: config.primaryChecksum || 'sha256',
            ...config,
        };

        this.#cache = new Cache({ path: this.#paths.cache, algorithms: this.#config.checksums });
        this.#backends = new BackendManager();
        this.#index = new Index(this.#paths.index);

        // Background sync queue for remote backends (worker spawned lazily)
        this.#syncQueue = new SyncQueue();
        this.#syncQueue.on('synced', ({ id, results }) => this.#handleSyncResult(id, results));
        this.#syncQueue.on('error', (err) => this.emit('error', err));

        debug('Stored initialized');
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Getters
    // ─────────────────────────────────────────────────────────────────────────

    get root() { return this.#paths.root; }
    get paths() { return { ...this.#paths }; }
    get dataPath() { return this.#paths.data; }
    get cache() { return this.#cache; }
    get index() { return this.#index; }
    get backends() { return this.#backends; }

    // ─────────────────────────────────────────────────────────────────────────
    // Backend Management
    // ─────────────────────────────────────────────────────────────────────────

    addBackend(name, config) {
        const backend = this.#backends.add(name, config);

        backend.on('file:add', e => this.#handleFileEvent('file:add', e));
        backend.on('file:change', e => this.#handleFileEvent('file:change', e));
        backend.on('file:unlink', e => this.#handleFileEvent('file:unlink', e));
        // Generic change events from non-file backends (e.g. imap message:add) —
        // forwarded as-is for the workspace indexer; they carry {backend, kind, key, ...}.
        backend.on('object:add', e => this.emit('object:add', e));
        backend.on('object:change', e => this.emit('object:change', e));
        backend.on('object:unlink', e => this.emit('object:unlink', e));
        backend.on('backend:state', e => this.emit('backend:state', e));
        backend.on('scan:start', e => this.emit('scan:start', e));
        backend.on('scan:complete', e => this.emit('scan:complete', e));
        backend.on('error', e => this.emit('error', e));

        if (config.watch) backend.watch();
        return backend;
    }

    // Unregister a backend and drop its now-dangling locations from the index.
    // Entries left with no locations are removed (the index is derived state —
    // synapsd documents are durable and rebuildable via scan()).
    async removeBackend(name) {
        const removed = await this.#backends.remove(name);
        if (removed) this.#removeMissingLocations(name, new Set());
        return removed;
    }

    listBackends() { return this.#backends.list(); }
    getBackend(name) { return this.#backends.get(name); }

    /**
     * Parse a canonical `stored://<backend>/<key>` URL. The backend name may
     * itself contain colons (e.g. `fs:data:email`), so we split on the first
     * `/` after the scheme. Returns { backend, key } or null if malformed.
     */
    #parseStoredUrl(url) {
        const prefix = 'stored://';
        if (typeof url !== 'string' || !url.startsWith(prefix)) return null;
        const rest = url.slice(prefix.length);
        const slash = rest.indexOf('/');
        if (slash < 0) return null;
        return { backend: rest.slice(0, slash), key: rest.slice(slash + 1) };
    }

    // Fetch bytes directly by `stored://` URL, bypassing the content index.
    // Returns Buffer | null (never throws on bad input).
    async getByUrl(url) {
        const p = this.#parseStoredUrl(url);
        const backend = p && this.#backends.get(p.backend);
        return backend ? backend.get(p.key) : null;
    }

    // Same as getByUrl but returns a Readable | null.
    async getStreamByUrl(url) {
        const p = this.#parseStoredUrl(url);
        const backend = p && this.#backends.get(p.backend);
        return backend ? backend.get(p.key, { stream: true }) : null;
    }

    /**
     * Delete the bytes behind a `stored://<backend>/<key>` URL. Does not touch
     * the document index (synapsd owns that) — callers trim `locations[]`.
     * Returns { ok:boolean, reason?:'malformed-url'|'unknown-backend'|'read-only-backend' }.
     */
    async deleteByUrl(url) {
        const p = this.#parseStoredUrl(url);
        if (!p) return { ok: false, reason: 'malformed-url' };
        const backend = this.#backends.get(p.backend);
        if (!backend) return { ok: false, reason: 'unknown-backend' };
        if (!backend.canDelete) return { ok: false, reason: 'read-only-backend' };
        return { ok: !!(await backend.delete(p.key)) };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Core API — cache-first writes, cache-first reads
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Store a blob. Accepts Buffer | string (content) | filesystem path |
     * Readable. Paths and streams are ingested without ever materializing the
     * whole blob in memory. `backends` is required (no implicit fan-out).
     * Returns { ok:true, id, key, size, mimeType, checksums, locations, ... }
     * or { ok:false, reason:'no-targets'|'unknown-backend' }.
     */
    async put(blob, options = {}) {
        const { key, backends, metadata = {} } = options;
        if (!Array.isArray(backends) || backends.length === 0) return { ok: false, reason: 'no-targets' };

        const targets = backends
            .map(name => ({ name, backend: this.#backends.get(name) }))
            .filter(t => t.backend);
        if (!targets.length) return { ok: false, reason: 'unknown-backend' };

        const ingest = (isBuffer(blob) || (typeof blob === 'string' && !isFile(blob)))
            ? await this.#ingestMemory(blob, targets, key, options.mimeType)
            : await this.#ingestStream(blob, targets, key, options.mimeType);

        const meta = this.#index.put(ingest.id, {
            checksums: ingest.checksums,
            size: ingest.size,
            mimeType: ingest.mimeType,
            locations: ingest.locations,
            custom: metadata,
        });

        if (ingest.remoteTargets.length) {
            this.#syncQueue.enqueue({ id: ingest.id, cacheRoot: this.#cache.root, cacheKey: ingest.id, targets: ingest.remoteTargets });
        }

        this.emit('put', { id: ingest.id, key: ingest.finalKey, metadata: meta });
        debug(`PUT ${ingest.id.slice(0, 19)}... → ${targets.map(t => t.name).join(', ')}`);
        return { ok: true, key: ingest.finalKey, ...meta };
    }

    async get(idOrKey) { return this.#read(idOrKey, false); }
    async getStream(idOrKey) { return this.#read(idOrKey, true); }

    async delete(idOrKey, options = {}) {
        const meta = this.#index.get(idOrKey);
        if (!meta) return { ok: false, reason: 'not-found' };

        this.#cache.delete(meta.id).catch(() => {});

        let targets = meta.locations || [];
        if (Array.isArray(options.urls)) {
            const set = new Set(options.urls);
            targets = targets.filter(l => set.has(`stored://${l.backend}/${l.key}`));
        } else if (Array.isArray(options.backends)) {
            targets = targets.filter(l => options.backends.includes(l.backend));
        }

        const deleted = [];
        const removed = new Set();
        for (const loc of targets) {
            const backend = this.#backends.get(loc.backend);
            if (backend && await backend.delete(loc.key)) {
                deleted.push(loc.backend);
                removed.add(loc);
            }
        }

        const remaining = (meta.locations || []).filter(l => !removed.has(l));
        if (remaining.length === 0) {
            this.#index.delete(meta.id);
        } else {
            meta.locations = remaining;
            this.#index.put(meta.id, meta);
        }

        this.emit('delete', { id: meta.id, backends: deleted });
        return { ok: true, deleted, kept: remaining.map(l => l.backend) };
    }

    async stat(idOrKey) { return this.#index.get(idOrKey); }
    async has(idOrKey) { return this.#index.has(idOrKey); }

    /**
     * Where does the content behind `idOrKey` actually live? Returns the
     * canonical, resolvable `stored://<backend>/<key>` URLs plus per-location
     * metadata. Single source of truth for the URL grammar — consumers map
     * these straight into synapsd documents and read them back via getByUrl.
     */
    async locations(idOrKey) {
        const meta = this.#index.get(idOrKey);
        return (meta?.locations || []).map(l => {
            const backend = this.#backends.get(l.backend);
            return {
                url: `stored://${l.backend}/${l.key}`,
                nativeUrl: backend ? backend.nativeUrl(l.key) : null,
                backend: l.backend,
                key: l.key,
                driver: l.driver,
                synced: l.synced,
                size: l.size,
                source: l.source,
            };
        });
    }

    // Iterate all indexed metadata entries.
    async *list() {
        for (const [, meta] of this.#index.entries()) yield meta;
    }

    // Iterate a single backend's native listing (raw keys, not index entries).
    async *listBackend(name, options = {}) {
        const backend = this.#backends.get(name);
        if (backend) yield* backend.list(options);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Scan / Index
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Index existing objects from one backend (or all). `options` is forwarded
     * verbatim to each backend's `scan()` so each driver defines its own knobs.
     * File backends get a size+mtime skip predicate so unchanged files are not
     * re-hashed. Returns { ok:true, backend, count, files } | { ok:false, reason }.
     */
    async scan(backendName, options = {}) {
        const list = backendName
            ? [this.#backends.get(backendName)].filter(Boolean)
            : this.#backends.all();
        if (backendName && !list.length) return { ok: false, reason: 'unknown-backend' };

        const files = [];
        for (const backend of list) {
            const rows = await backend.scan({
                algorithms: this.#config.checksums,
                knownChecksums: (k, st) => this.#knownIfUnchanged(backend.name, k, st),
                ...options,
            });
            // Non-array results (e.g. imap returns { inserted, lastUid } and
            // indexes via events) are not content-addressable here.
            if (!Array.isArray(rows)) continue;

            const presentKeys = new Set(rows.map(file => file.key));
            for (const file of rows) {
                if (!file.checksums) continue;
                const id = formatId(file.checksums, this.#config.primaryChecksum);
                const existing = this.#index.get(id);
                const locations = existing?.locations || [];
                const match = locations.find(l => l.backend === file.backend && l.key === file.key);
                if (match) {
                    match.size = file.size;
                    match.mtime = file.modified;
                } else {
                    locations.push(this.#buildLocation(file.backend, file.key, true, { size: file.size, mtime: file.modified }));
                }
                this.#index.put(id, { checksums: file.checksums, size: file.size, mimeType: file.mimeType, locations });
            }
            this.#removeMissingLocations(backend.name, presentKeys);
            files.push(...rows);
        }
        return { ok: true, backend: backendName ?? null, count: files.length, files };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Lifecycle
    // ─────────────────────────────────────────────────────────────────────────

    async stop() {
        await this.#syncQueue.stop();
        await this.#backends.stopAll();
        this.#index.close();
        debug('Stopped');
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Private — sync result handling
    // ─────────────────────────────────────────────────────────────────────────

    #handleSyncResult(id, results) {
        const meta = this.#index.get(id);
        if (!meta) return;

        for (const r of results) {
            if (!r.success) continue;
            const loc = meta.locations.find(l => l.backend === r.backend);
            if (loc) loc.synced = true;
        }

        this.#index.put(meta.id, meta);
        this.emit('synced', { id, results });
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Private — reads
    // ─────────────────────────────────────────────────────────────────────────

    async #read(idOrKey, stream) {
        const meta = this.#index.get(idOrKey);
        if (!meta) return null;

        // 1. Cache by content id.
        if (stream) {
            const info = await this.#cache.getInfo(meta.id).catch(() => null);
            if (info) return this.#cache.getStream(meta.id);
        } else {
            try { return (await this.#cache.get(meta.id)).data; } catch { /* miss */ }
        }

        // 2. First synced backend location.
        const location = meta.locations?.find(l => l.synced);
        const backend = location && this.#backends.get(location.backend);
        if (!backend) return null;

        const data = await backend.get(location.key, { stream });
        if (!stream && Buffer.isBuffer(data)) this.#cache.put(meta.id, data).catch(() => {});
        return data;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Private — ingest (streaming + in-memory)
    // ─────────────────────────────────────────────────────────────────────────

    // Buffer / string: already resident, write directly (no temp file).
    async #ingestMemory(blob, targets, key, mimeHint) {
        const data = isBuffer(blob) ? blob : Buffer.from(blob);
        const checksums = checksumBuffer(data, this.#config.checksums);
        const id = formatId(checksums, this.#config.primaryChecksum);
        const finalKey = key || this.#generateKey(checksums);
        const mimeType = mimeHint || await detectMimeType(data);

        const { locations, remoteTargets } = await this.#commit(targets, finalKey, id, { data }, { checksums, size: data.length, mimeType });
        return { id, finalKey, checksums, size: data.length, mimeType, locations, remoteTargets };
    }

    // Path / stream: stream through a hash pass into a temp file on the primary
    // local backend's filesystem, then commit (hardlink/rename) to targets.
    async #ingestStream(blob, targets, key, mimeHint) {
        const source = isStream(blob) ? blob : createReadStream(blob);
        const firstLocal = targets.find(t => t.backend.type === 'local')?.backend;
        const tempDir = firstLocal ? firstLocal.tempDir : path.join(this.#paths.cache, '.tmp');
        const tempPath = path.join(tempDir, `${Date.now()}-${crypto.randomUUID()}`);
        await fsp.mkdir(tempDir, { recursive: true });

        let result;
        try {
            const { checksums, size, head } = await this.#hashToFile(source, tempPath);
            const id = formatId(checksums, this.#config.primaryChecksum);
            const finalKey = key || this.#generateKey(checksums);
            const mimeType = mimeHint || await detectMimeFromHead(head, finalKey);

            const { locations, remoteTargets } = await this.#commit(targets, finalKey, id, { file: tempPath }, { checksums, size, mimeType });
            result = { id, finalKey, checksums, size, mimeType, locations, remoteTargets };
        } finally {
            await fsp.rm(tempPath, { force: true }).catch(() => {});
        }
        return result;
    }

    // Single streaming pass: hash (all algorithms), measure size, peek the head
    // for mime sniffing, and write to `tempPath`. Never buffers the whole blob.
    async #hashToFile(source, tempPath) {
        const hashes = this.#config.checksums.map(algo => ({ algo, hash: crypto.createHash(algo) }));
        const ws = createWriteStream(tempPath);
        let size = 0;
        const headChunks = [];
        let headLen = 0;

        try {
            for await (const chunk of source) {
                const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
                for (const { hash } of hashes) hash.update(buf);
                size += buf.length;
                if (headLen < HEAD_BYTES) {
                    const slice = buf.subarray(0, HEAD_BYTES - headLen);
                    headChunks.push(slice);
                    headLen += slice.length;
                }
                if (!ws.write(buf)) await once(ws, 'drain');
            }
            ws.end();
            await once(ws, 'finish');
        } catch (err) {
            ws.destroy();
            throw err;
        }

        const checksums = {};
        for (const { algo, hash } of hashes) checksums[algo] = hash.digest('hex');
        return { checksums, size, head: Buffer.concat(headChunks) };
    }

    // Place ingested bytes on every target: local backends get the bytes now
    // (buffer write or hardlink/copy from temp); remote targets get a cache
    // entry + a SyncQueue placeholder. Returns { locations, remoteTargets }.
    async #commit(targets, finalKey, id, source, meta) {
        const locations = [];
        const remoteTargets = [];

        for (const { name, backend } of targets) {
            if (backend.type === 'local') {
                if (source.data) await backend.put(finalKey, source.data);
                else await backend.commit(finalKey, source.file);
                locations.push(this.#buildLocation(name, finalKey, true, { size: meta.size }));
            } else {
                locations.push(this.#buildLocation(name, finalKey, false, { size: meta.size }));
                remoteTargets.push({ name, driver: backend.config.driver, root: backend.config.root, key: finalKey });
            }
        }

        if (remoteTargets.length) {
            const cacheMeta = { key: finalKey, checksums: meta.checksums, size: meta.size, mimeType: meta.mimeType };
            if (source.data) await this.#cache.put(id, source.data, cacheMeta);
            else await pipeline(createReadStream(source.file), this.#cache.putStream(id, cacheMeta));
        }

        return { locations, remoteTargets };
    }

    // Skip-hash predicate for scan: returns the cached descriptor when a file's
    // size+mtime match an existing indexed location, else null.
    #knownIfUnchanged(backendName, key, stat) {
        const meta = this.#index.get(`${backendName}:${key}`);
        const loc = meta?.locations?.find(l => l.backend === backendName && l.key === key);
        if (!loc || loc.size !== stat.size || loc.mtime !== stat.mtime) return null;
        return { checksums: meta.checksums, mimeType: meta.mimeType };
    }

    #generateKey(checksums) {
        const hash = checksums[this.#config.primaryChecksum];
        return `${hash.slice(0, 2)}/${hash.slice(2, 4)}/${hash}`;
    }

    #buildLocation(backendName, key, synced, extra = {}) {
        const backend = this.#backends.get(backendName);
        const config = backend?.config || {};

        return {
            backend: backendName,
            driver: config.driver || null,
            key,
            synced,
            ...extra,
            source: this.#buildSourceDescriptor(backendName, key, config),
        };
    }

    #buildSourceDescriptor(backendName, key, config = {}) {
        const [providerHint, ...accountHintParts] = String(backendName || '').split(':').filter(Boolean);
        const provider = config.provider || providerHint || config.driver || 'unknown';
        const account = config.account
            || (accountHintParts.length > 0 ? accountHintParts.join(':') : (providerHint || backendName || 'default'));
        const container = config.container
            || config.bucket
            || config.share
            || config.folder
            || (config.root ? path.basename(path.resolve(config.root)) : 'root');

        return {
            provider,
            account,
            container,
            path: key,
        };
    }

    #removeMissingLocations(backendName, presentKeys) {
        for (const [id, meta] of this.#index.entries()) {
            const nextLocations = (meta.locations || []).filter(location =>
                location.backend !== backendName || presentKeys.has(location.key)
            );

            if (nextLocations.length === (meta.locations || []).length) continue;

            if (nextLocations.length === 0) {
                this.#index.delete(id);
                continue;
            }

            this.#index.put(id, { ...meta, locations: nextLocations });
        }
    }

    // Emit a file event under both its typed name (`file:<suffix>`) and the
    // generic `object:<suffix>` (kind:'file') so consumers can bind either.
    #emitObject(suffix, payload) {
        this.emit(`file:${suffix}`, payload);
        this.emit(`object:${suffix}`, { kind: 'file', ...payload });
    }

    #handleFileEvent(event, data) {
        const pathKey = `${data.backend}:${data.key}`;
        const location = this.#buildLocation(data.backend, data.key, true, { size: data.size, mtime: data.modified });

        if (event === 'file:add' && data.checksums) {
            const id = formatId(data.checksums, this.#config.primaryChecksum);
            const existing = this.#index.get(id);

            const locations = existing?.locations || [];
            if (!locations.some(l => l.backend === data.backend && l.key === data.key)) {
                locations.push(location);
            }

            this.#index.put(id, {
                checksums: data.checksums,
                size: data.size,
                mimeType: data.mimeType,
                locations,
            });
            this.#emitObject('add', { ...data, id, locations });

        } else if (event === 'file:change' && data.checksums) {
            const oldMeta = this.#index.get(pathKey);
            if (oldMeta) {
                oldMeta.locations = oldMeta.locations.filter(l =>
                    !(l.backend === data.backend && l.key === data.key)
                );
                if (oldMeta.locations.length === 0) {
                    this.#index.delete(oldMeta.id);
                } else {
                    this.#index.put(oldMeta.id, oldMeta);
                }
                this.#emitObject('unlink', { ...data, id: oldMeta.id, checksums: oldMeta.checksums });
            }

            const newId = formatId(data.checksums, this.#config.primaryChecksum);
            const existing = this.#index.get(newId);
            const locations = existing?.locations || [];
            if (!locations.some(l => l.backend === data.backend && l.key === data.key)) {
                locations.push(location);
            }

            this.#index.put(newId, {
                checksums: data.checksums,
                size: data.size,
                mimeType: data.mimeType,
                locations,
            });
            this.#emitObject('add', { ...data, id: newId, locations });

        } else if (event === 'file:unlink') {
            const meta = this.#index.get(pathKey);
            if (meta) {
                meta.locations = meta.locations.filter(l =>
                    !(l.backend === data.backend && l.key === data.key)
                );
                if (meta.locations.length === 0) {
                    this.#index.delete(meta.id);
                } else {
                    this.#index.put(meta.id, meta);
                }
                this.#emitObject('unlink', { ...data, id: meta.id, checksums: meta.checksums, locations: meta.locations });
            } else {
                this.#emitObject('unlink', data);
            }
        }
    }
}
