import Debug from 'debug';
import StorageBackend from '../StorageBackend.js';

const debug = Debug('stored:backend:http');

/**
 * HTTP(S) storage backend — SKELETON.
 *
 * Registered so `stored://http:<account>/<key>` URLs parse and dispatch.
 * `get` has a minimal read-only fetch implementation (HTTP sources are
 * commonly read-only); `put`/`delete`/`list` inherit `Not implemented`.
 * `config` carries { baseUrl, account, headers? }.
 */
export default class HttpBackend extends StorageBackend {
    #baseUrl;

    constructor(name, config = {}) {
        super(name, config);
        this.type = 'remote';
        this.#baseUrl = config.baseUrl || '';
        debug(`HttpBackend "${name}" registered (skeleton; baseUrl=${this.#baseUrl || '?'})`);
    }

    async get(key, options = {}) {
        const url = this.#baseUrl ? new URL(key, this.#baseUrl).toString() : key;
        const res = await fetch(url, { headers: this.config.headers || {} });
        if (!res.ok) return null;
        if (options.stream) return res.body;
        return Buffer.from(await res.arrayBuffer());
    }

    async stat(key) {
        const url = this.#baseUrl ? new URL(key, this.#baseUrl).toString() : key;
        const res = await fetch(url, { method: 'HEAD', headers: this.config.headers || {} });
        if (!res.ok) return null;
        const size = Number(res.headers.get('content-length'));
        return { key, size: Number.isFinite(size) ? size : null, modified: null, created: null };
    }
}
