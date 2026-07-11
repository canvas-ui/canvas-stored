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
     * `canEnumerate:true` means the backend implements the async-generator
     * `list()` (and usually `scan()`) so callers can mirror its whole tree;
     * drivers without real enumeration (http, s3 skeleton) leave it false.
     */
    get capabilities() { return { read: true, write: true, delete: true, canEnumerate: false }; }
    get canDelete() { return this.capabilities.delete !== false; }

    // The real, protocol-native URL for `key` (e.g. https://…, s3://…, smb://…,
    // file://…), used for provenance/UI. `stored://<backend>/<key>` remains the
    // canonical fetch form. Returns null when the backend has no meaningful one.
    nativeUrl(key) { return null; }

    // Required methods - must be implemented by subclasses
    async put(key, data) { throw new Error('Not implemented'); }
    // Place an already-written file at `key` (streaming put commit). Local
    // backends implement this; remote backends are fed via the cache + SyncQueue.
    async commit(key, srcPath) { throw new Error('Not implemented'); }
    async get(key, options = {}) { throw new Error('Not implemented'); }
    async delete(key) { throw new Error('Not implemented'); }
    async stat(key) { throw new Error('Not implemented'); }
    async *list(options = {}) { throw new Error('Not implemented'); }

    // Optional methods
    async watch() { return false; }
    async scan() { return []; }
    async stop() { }

    // Optional container (directory/folder) mutation — only backends with a real
    // hierarchical namespace (the file driver) implement these. Advertised via
    // the Workspace `mutableContainers` capability; callers must gate on it.
    async createContainer(key) { throw new Error('Container ops not supported by this backend'); }
    async deleteContainer(key) { throw new Error('Container ops not supported by this backend'); }
    async renameContainer(fromKey, toKey) { throw new Error('Container ops not supported by this backend'); }
}
