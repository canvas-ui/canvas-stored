import { open } from 'lmdb';
import Debug from 'debug';

const debug = Debug('stored:index');

/**
 * LMDB-backed index mapping id (sha256:xxx) to metadata
 */
export default class Index {
    #db;
    #pathDb;

    constructor(path = './.stored/index') {
        this.#db = open({ path, name: 'metadata', compression: true });
        this.#pathDb = this.#db.openDB('paths');
        debug(`Index opened at ${path}`);
    }

    get size() { return this.#db.getKeysCount(); }

    // ─────────────────────────────────────────────────────────────────────────
    // Core Operations
    // ─────────────────────────────────────────────────────────────────────────

    put(id, metadata) {
        const existing = this.#db.get(id);
        const meta = {
            ...existing,
            ...metadata,
            id,
            modified: Date.now(),
            created: existing?.created || Date.now(),
        };

        const nextPathKeys = new Set((meta.locations || []).map(loc => `${loc.backend}:${loc.key}`));
        for (const loc of existing?.locations || []) {
            const pathKey = `${loc.backend}:${loc.key}`;
            if (!nextPathKeys.has(pathKey)) {
                this.#pathDb.removeSync(pathKey);
            }
        }

        this.#db.putSync(id, meta);

        // Index by path for each location
        for (const loc of meta.locations || []) {
            this.#pathDb.putSync(`${loc.backend}:${loc.key}`, id);
        }

        debug(`Indexed ${id.slice(0, 19)}...`);
        return meta;
    }

    get(idOrPath) {
        // Try direct id lookup
        const direct = this.#db.get(idOrPath);
        if (direct) return direct;

        // Try path lookup
        const id = this.#pathDb.get(idOrPath);
        return id ? this.#db.get(id) : null;
    }

    has(idOrPath) {
        return this.#db.doesExist(idOrPath) || this.#pathDb.doesExist(idOrPath);
    }

    delete(id) {
        const meta = this.#db.get(id);
        if (!meta) return false;

        for (const loc of meta.locations || []) {
            this.#pathDb.removeSync(`${loc.backend}:${loc.key}`);
        }

        this.#db.removeSync(id);
        debug(`Removed ${id.slice(0, 19)}...`);
        return true;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Query
    // ─────────────────────────────────────────────────────────────────────────

    findByBackend(backendName) {
        const results = [];
        for (const { value } of this.#db.getRange()) {
            if (value.locations?.some(l => l.backend === backendName)) {
                results.push(value);
            }
        }
        return results;
    }

    *entries() {
        for (const { key, value } of this.#db.getRange()) {
            yield [key, value];
        }
    }

    clear() {
        this.#db.clearSync();
        this.#pathDb.clearSync();
    }

    close() {
        this.#db.close();
    }
}
