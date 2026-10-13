import cacache from 'cacache';
import Debug from 'debug';
import { Writable } from 'node:stream';

const debug = Debug('stored:cache');

// memoize:false on every read/write — the cache fronts blobs up to ~20GB and is
// hit by 1M+ entries; cacache's in-memory LRU memoizer would defeat the point of
// a disk-backed store and risk OOM. Bytes always flow through the content store.
const CACHE_OPTS = { memoize: false };

export default class Cache {
    #root;
    #algorithms;
    #mutations = Promise.resolve();
    #dirty = false;
    #timer;

    constructor(config) {
        if (!config?.path) throw new Error('Cache path required');
        this.#root = config.path;
        this.#algorithms = config.algorithms || ['sha256'];
        this.#timer = setInterval(() => { if (this.#dirty) this.verify().catch(error => debug(`Cache collection failed: ${error.message}`)); }, 60_000);
        this.#timer.unref?.();
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
        return this.#serialize(() => cacache.put(this.#root, key, data, { algorithms: this.#algorithms, metadata, ...CACHE_OPTS }));
    }

    putStream(key, metadata = {}) {
        let sink, release;
        const self = this;
        return new Writable({
            construct(done) {
                const output = this;
                self.#serialize(() => new Promise(resolve => {
                    release = resolve;
                    sink = cacache.put.stream(self.#root, key, { algorithms: self.#algorithms, metadata, ...CACHE_OPTS });
                    sink.on('error', error => output.destroy(error));
                    sink.on('integrity', value => output.emit('integrity', value));
                    // cacache initializes content on the first write. An empty
                    // HTTP/file stream can reach EOF without emitting any chunk.
                    sink.write(Buffer.alloc(0));
                    done();
                })).catch(error => output.destroy(error));
            },
            write(chunk, encoding, done) { sink.write(chunk, encoding, done); },
            final(done) { sink.once('finish', () => { release(); done(); }); sink.end(); },
            destroy(error, done) { sink?.destroy(); release?.(); done(error); },
        });
    }

    get(key) { return cacache.get(this.#root, key, CACHE_OPTS); }

    getStream(key) { return cacache.get.stream(this.#root, key, CACHE_OPTS); }

    getInfo(key) { return cacache.get.info(this.#root, key); }

    delete(key) { return this.#serialize(async () => { await cacache.rm.entry(this.#root, key, { removeFully: true }); this.#dirty = true; }); }

    clear() { return this.#serialize(() => cacache.rm.all(this.#root)); }

    #serialize(action) { const next = this.#mutations.catch(() => {}).then(action); this.#mutations = next; return next; }
    verify() { return this.#serialize(async () => { const result = await cacache.verify(this.#root); this.#dirty = false; return result; }); }
    async stop() { clearInterval(this.#timer); await this.verify(); }

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
