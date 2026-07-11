import fs from 'fs-extra';
import path from 'path';
import { createReadStream } from 'fs';
import { pipeline } from 'stream/promises';
import cacache from 'cacache';
import Debug from 'debug';
import StorageBackend from '../StorageBackend.js';

const debug = Debug('stored:backend:cacache');

// memoize:false — this store fronts blobs up to ~20GB across 1M+ entries; never
// let cacache's in-memory LRU hold content. All bytes flow through streams.
const CACHE_OPTS = { memoize: false };

/**
 * cacache storage backend — a content-addressable local blob store.
 *
 * Same key/value CRUD surface as FileBackend, but bytes live in a cacache
 * content store (sha-keyed, deduped, integrity-checked) instead of a plain
 * directory tree. Keys are arbitrary `stored://<backend>/<key>` keys mapped to
 * content by cacache's index. `type = 'local'` so the Stored ingest path writes
 * bytes immediately (no SyncQueue).
 *
 * There is no meaningful protocol-native URL (the store is internal), so
 * `nativeUrl` is null and `stored://<backend>/<key>` is the only address.
 *
 * Not watched/scanned: it is a managed write target, not an external source —
 * `watch`/`scan` inherit the no-op base behaviour.
 *
 * `config`: { root } — the cacache store directory ("the data route").
 */
export default class CacacheBackend extends StorageBackend {
    #root;
    #algorithms;

    constructor(name, config = {}) {
        super(name, config);
        if (!config.root) throw new Error('CacacheBackend requires root path');
        this.#root = path.resolve(config.root);
        this.#algorithms = config.algorithms || ['sha256'];
        this.type = 'local';
        fs.ensureDirSync(this.#root);
        debug(`CacacheBackend "${name}" initialized at ${this.#root}`);
    }

    get root() { return this.#root; }
    // list() streams the cacache index, so full-key enumeration works even
    // though the store is opaque byte-wise.
    get capabilities() { return { ...super.capabilities, canEnumerate: true }; }
    // Staging dir for the Stored streaming-ingest hash pass (shares a filesystem
    // with the store so the temp file is cheap to read back on commit).
    get tempDir() { return path.join(this.#root, 'tmp'); }

    // Internal blob store: no protocol-native URL to surface.
    nativeUrl(key) { return null; }

    // ─────────────────────────────────────────────────────────────────────────
    // CRUD Operations
    // ─────────────────────────────────────────────────────────────────────────

    async put(key, data) {
        await cacache.put(this.#root, key, data, { algorithms: this.#algorithms, ...CACHE_OPTS });
        const size = Buffer.isBuffer(data) ? data.length : Buffer.byteLength(data);
        debug(`PUT ${key} (${size} bytes)`);
        return { key, size };
    }

    // Commit an already-written file (the Stored streaming-ingest temp) at `key`
    // by streaming it into the content store.
    async commit(key, srcPath) {
        await pipeline(createReadStream(srcPath), cacache.put.stream(this.#root, key, { algorithms: this.#algorithms, ...CACHE_OPTS }));
        const info = await cacache.get.info(this.#root, key);
        const size = info?.size ?? 0;
        debug(`COMMIT ${key} (${size} bytes)`);
        return { key, size };
    }

    async get(key, options = {}) {
        if (options.stream) {
            // get.stream emits ENOENT on miss; surface null instead of throwing.
            const exists = await cacache.get.info(this.#root, key);
            return exists ? cacache.get.stream(this.#root, key, CACHE_OPTS) : null;
        }
        try {
            const { data } = await cacache.get(this.#root, key, CACHE_OPTS);
            return data;
        } catch (err) {
            if (err.code === 'ENOENT') return null;
            throw err;
        }
    }

    async delete(key) {
        const info = await cacache.get.info(this.#root, key);
        if (!info) return false;
        await cacache.rm.entry(this.#root, key, { removeFully: true });
        debug(`DELETE ${key}`);
        return true;
    }

    async stat(key) {
        const info = await cacache.get.info(this.#root, key);
        if (!info) return null;
        return { key, size: info.size, modified: info.time, created: info.time };
    }

    async *list(options = {}) {
        const { prefix = '' } = options;
        // Stream the index so a 1M+ entry store is walked without ever building
        // the full key→info map in memory.
        for await (const info of cacache.ls.stream(this.#root)) {
            if (prefix && !info.key.startsWith(prefix)) continue;
            yield { key: info.key, size: info.size, modified: info.time, created: info.time };
        }
    }

    // Drop the whole content store (index + content). Used by Destroy/reset paths.
    async clear() { return cacache.rm.all(this.#root); }
}
