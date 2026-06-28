import cacache from 'cacache';
import Debug from 'debug';

const debug = Debug('stored:cache');

// memoize:false on every read/write — the cache fronts blobs up to ~20GB and is
// hit by 1M+ entries; cacache's in-memory LRU memoizer would defeat the point of
// a disk-backed store and risk OOM. Bytes always flow through the content store.
const CACHE_OPTS = { memoize: false };

export default class Cache {
    #root;
    #algorithms;

    constructor(config) {
        if (!config?.path) throw new Error('Cache path required');
        this.#root = config.path;
        this.#algorithms = config.algorithms || ['sha256'];
        debug(`Cache initialized at "${this.#root}"`);
    }

    get root() { return this.#root; }

    // Materializes the entire index into one object — fine for small stores, but
    // prefer listStream() at scale (1M+ entries) to avoid the in-RAM map.
    list() { return cacache.ls(this.#root); }

    // Streaming index walk: yields one entry at a time, never building the whole
    // index in memory. Use this for large stores.
    listStream() { return cacache.ls.stream(this.#root); }

    has(key) { return cacache.get.info(this.#root, key); }

    put(key, data, metadata = {}) {
        return cacache.put(this.#root, key, data, { algorithms: this.#algorithms, metadata, ...CACHE_OPTS });
    }

    putStream(key, metadata = {}) {
        return cacache.put.stream(this.#root, key, { algorithms: this.#algorithms, metadata, ...CACHE_OPTS });
    }

    get(key) { return cacache.get(this.#root, key, CACHE_OPTS); }

    getStream(key) { return cacache.get.stream(this.#root, key, CACHE_OPTS); }

    getInfo(key) { return cacache.get.info(this.#root, key); }

    delete(key) { return cacache.rm.entry(this.#root, key, { removeFully: true }); }

    clear() { return cacache.rm.all(this.#root); }

    verify() { return cacache.verify(this.#root); }

    // Stream the index so a 1M+ entry store is tallied without ever holding the
    // full key set in memory.
    async stats() {
        let entries = 0;
        let size = 0;
        for await (const entry of cacache.ls.stream(this.#root)) {
            entries += 1;
            size += entry.size || 0;
        }
        return { entries, size };
    }
}
