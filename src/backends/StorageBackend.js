import { EventEmitter } from 'events';

/**
 * Base class for storage backends.
 * All backends must implement: put, get, delete, stat, list
 * Optional: watch, scan, stop
 */
export default class StorageBackend extends EventEmitter {
    constructor(name, config = {}) {
        super();
        this.name = name;
        this.type = 'base';
        this.config = config;
    }

    /**
     * Capability flags consumed by the Destroy/lifecycle logic.
     * Default is fully read-write; read-only backends (e.g. HTTP) override.
     * `delete:false` means a location on this backend can only be dropped as a
     * reference — its bytes cannot be removed by us.
     */
    get capabilities() { return { read: true, write: true, delete: true }; }
    get canDelete() { return this.capabilities.delete !== false; }

    // Required methods - must be implemented by subclasses
    async put(key, data) { throw new Error('Not implemented'); }
    async get(key, options = {}) { throw new Error('Not implemented'); }
    async delete(key) { throw new Error('Not implemented'); }
    async stat(key) { throw new Error('Not implemented'); }
    async *list(options = {}) { throw new Error('Not implemented'); }

    // Optional methods
    async watch() { return false; }
    async scan() { return []; }
    async stop() { }
}
