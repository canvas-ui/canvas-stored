import EventEmitter2 from 'eventemitter2';
import path from 'path';
import Debug from 'debug';
import Cache from './cache/index.js';
import BackendManager from './backends/BackendManager.js';
import Index from './index/index.js';
import SyncQueue from './sync/SyncQueue.js';
import { isBuffer, isFile, isStream } from './utils/common.js';
import { checksumBuffer, checksumFile, formatId } from './utils/checksum.js';
import { detectMimeType } from './utils/mime.js';

const debug = Debug('stored');

export default class Stored extends EventEmitter2 {
    #cache;
    #backends;
    #index;
    #config;
    #syncQueue;

    constructor(config = {}) {
        // Wildcards (':' delimiter) let consumers bind `object:*` across backends.
        super({ wildcard: true, delimiter: ':', maxListeners: 100, verboseMemoryLeak: false });
        this.#config = {
            defaultBackends: config.defaultBackends || [],
            checksums: config.checksums || ['sha256'],
            primaryChecksum: config.primaryChecksum || 'sha256',
            ...config,
        };

        // Cache is mandatory — derive path from index path if not provided
        const cachePath = config.cache?.path || (config.index?.path ? config.index.path + '-cache' : './.stored-cache');
        this.#cache = new Cache({ path: cachePath, algorithms: config.checksums || ['sha256'] });

        this.#backends = new BackendManager();
        this.#index = new Index(config.index?.path);

        // Background sync queue for remote backends (worker spawned lazily)
        this.#syncQueue = new SyncQueue();
        this.#syncQueue.on('synced', ({ id, results }) => this.#handleSyncResult(id, results));
        this.#syncQueue.on('error', (err) => this.emit('error', err));

        debug('Stored initialized');
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Getters
    // ─────────────────────────────────────────────────────────────────────────

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

    removeBackend(name) { return this.#backends.remove(name); }
    listBackends() { return this.#backends.list(); }
    getBackend(name) { return this.#backends.get(name); }

    /**
     * Fetch a blob directly by its canonical `stored://<backend>/<key>` URL,
     * bypassing the content index. The backend name may itself contain colons
     * (e.g. `fs:data:email`), so we split on the first `/` after the scheme.
     * Returns the backend's `get` result (Buffer, or stream with {stream:true}),
     * or null if the URL is malformed or the backend is unknown.
     */
    async getByUrl(url, options = {}) {
        const prefix = 'stored://';
        if (typeof url !== 'string' || !url.startsWith(prefix)) {
            throw new Error(`getByUrl expects a stored:// URL, got: ${url}`);
        }
        const rest = url.slice(prefix.length);
        const slash = rest.indexOf('/');
        if (slash < 0) return null;
        const backendName = rest.slice(0, slash);
        const key = rest.slice(slash + 1);
        const backend = this.#backends.get(backendName);
        if (!backend) return null;
        return backend.get(key, options);
    }

    /**
     * Delete the bytes behind a `stored://<backend>/<key>` URL.
     * Returns { deleted:boolean, reason?:string }. Does not touch the document
     * index (synapsd owns that) — callers trim `locations[]` themselves.
     */
    async deleteByUrl(url) {
        const prefix = 'stored://';
        if (typeof url !== 'string' || !url.startsWith(prefix)) {
            throw new Error(`deleteByUrl expects a stored:// URL, got: ${url}`);
        }
        const rest = url.slice(prefix.length);
        const slash = rest.indexOf('/');
        if (slash < 0) return { deleted: false, reason: 'malformed-url' };
        const backendName = rest.slice(0, slash);
        const key = rest.slice(slash + 1);
        const backend = this.#backends.get(backendName);
        if (!backend) return { deleted: false, reason: 'unknown-backend' };
        if (!backend.canDelete) return { deleted: false, reason: 'read-only-backend' };
        const ok = await backend.delete(key);
        return { deleted: !!ok };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Core API — cache-first writes, cache-first reads
    // ─────────────────────────────────────────────────────────────────────────

    async put(blob, options = {}) {
        const { key, backends = this.#config.defaultBackends, metadata = {} } = options;

        const { data, checksums, size, mimeType } = await this.#normalizeBlob(blob);
        const id = formatId(checksums, this.#config.primaryChecksum);
        const finalKey = key || this.#generateKey(checksums);

        // 1. Write to cache first (always, fast)
        await this.#cache.put(id, data, { key: finalKey, checksums, size, mimeType });

        // 2. Write to backends — local immediately, remote via queue
        const targetNames = backends.length ? backends : this.#backends.list();
        const locations = [];
        const remoteTargets = [];

        for (const name of targetNames) {
            const backend = this.#backends.get(name);
            if (!backend) continue;

            if (backend.type === 'local') {
                await backend.put(finalKey, data);
                locations.push(this.#buildLocation(name, finalKey, true));
            } else {
                locations.push(this.#buildLocation(name, finalKey, false));
                remoteTargets.push({ name, driver: backend.config.driver, root: backend.config.root, key: finalKey });
            }
        }

        // 3. Update index
        const meta = this.#index.put(id, { checksums, size, mimeType, locations, custom: metadata });

        // 4. Enqueue remote backend sync
        if (remoteTargets.length) {
            this.#syncQueue.enqueue({ id, cacheRoot: this.#cache.root, cacheKey: id, targets: remoteTargets });
        }

        this.emit('put', { id, key: finalKey, metadata: meta });
        debug(`PUT ${id.slice(0, 19)}... → cache + ${targetNames.join(', ')}`);
        return meta;
    }

    async get(idOrKey, options = {}) {
        const meta = this.#index.get(idOrKey);
        if (!meta) return null;

        // 1. Cache by content ID
        if (options.stream) {
            try {
                return this.#cache.getStream(meta.id);
            } catch { /* cache miss */ }
        }
        try {
            const { data } = await this.#cache.get(meta.id);
            return data;
        } catch { /* cache miss */ }

        // 2. Backend fallback
        const location = meta.locations?.find(l => l.synced);
        if (!location) return null;

        const backend = this.#backends.get(location.backend);
        if (!backend) return null;

        const data = await backend.get(location.key, options);

        // 3. Cache on read (buffer only)
        if (data && Buffer.isBuffer(data)) {
            this.#cache.put(meta.id, data).catch(() => {});
        }

        return data;
    }

    async delete(idOrKey, options = {}) {
        const meta = this.#index.get(idOrKey);
        if (!meta) return { deleted: [] };

        // Remove from cache
        this.#cache.delete(meta.id).catch(() => {});

        const targets = options.backends
            ? meta.locations.filter(l => options.backends.includes(l.backend))
            : meta.locations;

        const deleted = [];
        for (const loc of targets) {
            const backend = this.#backends.get(loc.backend);
            if (backend && await backend.delete(loc.key)) deleted.push(loc.backend);
        }

        if (!options.backends || deleted.length === meta.locations.length) {
            this.#index.delete(meta.id);
        } else {
            meta.locations = meta.locations.filter(l => !deleted.includes(l.backend));
            this.#index.put(meta.id, meta);
        }

        this.emit('delete', { id: meta.id, backends: deleted });
        return { deleted };
    }

    stat(idOrKey) { return this.#index.get(idOrKey); }
    has(idOrKey) { return this.#index.has(idOrKey); }

    async *list(options = {}) {
        const { backend: backendName, prefix } = options;

        if (backendName) {
            const backend = this.#backends.get(backendName);
            if (backend) yield* backend.list({ prefix });
        } else {
            for (const [, meta] of this.#index.entries()) {
                yield meta;
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Scan / Index
    // ─────────────────────────────────────────────────────────────────────────

    async scan(backendName) {
        const backends = backendName
            ? [this.#backends.get(backendName)].filter(Boolean)
            : this.#backends.all();

        const results = [];
        for (const backend of backends) {
            const files = await backend.scan({ algorithms: this.#config.checksums });
            const presentKeys = new Set(files.map(file => file.key));

            for (const file of files) {
                if (file.checksums) {
                    const id = formatId(file.checksums, this.#config.primaryChecksum);
                    const existing = this.#index.get(id);
                    const location = this.#buildLocation(file.backend, file.key, true);

                    const locations = existing?.locations || [];
                    if (!locations.some(l => l.backend === file.backend && l.key === file.key)) {
                        locations.push(location);
                    }

                    this.#index.put(id, {
                        checksums: file.checksums,
                        size: file.size,
                        mimeType: file.mimeType,
                        locations,
                    });
                }
            }
            this.#removeMissingLocations(backend.name, presentKeys);
            results.push(...files);
        }
        return results;
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
    // Private — blob normalization & helpers
    // ─────────────────────────────────────────────────────────────────────────

    async #normalizeBlob(blob) {
        const algos = this.#config.checksums;
        let data, checksums, mimeType;

        if (isBuffer(blob)) {
            data = blob;
            checksums = checksumBuffer(blob, algos);
            mimeType = await detectMimeType(blob);
        } else if (isFile(blob)) {
            const fs = await import('fs');
            data = await fs.promises.readFile(blob);
            checksums = await checksumFile(blob, algos);
            mimeType = await detectMimeType(blob);
        } else if (isStream(blob)) {
            const chunks = [];
            for await (const chunk of blob) chunks.push(chunk);
            data = Buffer.concat(chunks);
            checksums = checksumBuffer(data, algos);
            mimeType = await detectMimeType(data);
        } else if (typeof blob === 'string') {
            data = Buffer.from(blob);
            checksums = checksumBuffer(data, algos);
            mimeType = 'text/plain';
        } else {
            throw new Error('Invalid blob type');
        }

        return { data, checksums, size: data.length, mimeType };
    }

    #generateKey(checksums) {
        const hash = checksums[this.#config.primaryChecksum];
        return `${hash.slice(0, 2)}/${hash.slice(2, 4)}/${hash}`;
    }

    #buildLocation(backendName, key, synced) {
        const backend = this.#backends.get(backendName);
        const config = backend?.config || {};

        return {
            backend: backendName,
            driver: config.driver || null,
            key,
            synced,
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
        const location = this.#buildLocation(data.backend, data.key, true);

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
