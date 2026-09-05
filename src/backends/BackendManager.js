import Debug from 'debug';
import FileBackend from './file/index.js';
import CacacheBackend from './cacache/index.js';
import S3Backend from './s3/index.js';
import HttpBackend from './http/index.js';
import GdriveBackend from './gdrive/index.js';
import CanvasBackend from './canvas/index.js';

const debug = Debug('stored:backends');

// `s3`/`http` are skeletons (see their index.js). `smb`/`webdav` are reserved
// scheme names with no driver yet — register a driver class here (or at
// runtime via BackendManager.register) when implemented. `gdrive` is the first
// real remote driver; `canvas` mirrors one backend of a workspace on another
// canvas-server over the objects protocol (docs/sync-protocol.md). Non-blob connectors (mail/git/…) are NOT stored drivers;
// they live in separate workspace services and only use stored to persist blobs.
const DRIVERS = { file: FileBackend, cacache: CacacheBackend, s3: S3Backend, http: HttpBackend, gdrive: GdriveBackend, canvas: CanvasBackend };

export default class BackendManager {
    #backends = new Map();

    /** Register (or override) a driver class under `driver` — plugin/test seam. */
    static register(driver, Driver) {
        if (!driver || typeof Driver !== 'function') throw new Error('register(driver, DriverClass) required');
        DRIVERS[driver] = Driver;
    }

    static drivers() { return Object.keys(DRIVERS); }

    get(name) { return this.#backends.get(name); }
    has(name) { return this.#backends.has(name); }
    list() { return [...this.#backends.keys()]; }
    all() { return [...this.#backends.values()]; }

    add(name, config) {
        if (this.#backends.has(name)) throw new Error(`Backend "${name}" already exists`);
        const Driver = DRIVERS[config.driver];
        if (!Driver) throw new Error(`Unknown driver: ${config.driver}`);

        const backend = new Driver(name, config);
        this.#backends.set(name, backend);
        debug(`Added backend "${name}" (${config.driver})`);
        return backend;
    }

    async remove(name) {
        const backend = this.#backends.get(name);
        if (!backend) return false;
        await backend.stop();
        this.#backends.delete(name);
        debug(`Removed backend "${name}"`);
        return true;
    }

    async stopAll() {
        for (const backend of this.#backends.values()) {
            await backend.stop();
        }
    }
}
