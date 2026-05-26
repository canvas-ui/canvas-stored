import Debug from 'debug';
import FileBackend from './file/index.js';
import S3Backend from './s3/index.js';
import HttpBackend from './http/index.js';
import ImapBackend from './imap/index.js';

const debug = Debug('stored:backends');

// `s3`/`http`/`imap` are skeletons (see their index.js). `smb`/`webdav` are
// reserved scheme names with no driver yet — register a driver class here when
// implemented.
const DRIVERS = { file: FileBackend, s3: S3Backend, http: HttpBackend, imap: ImapBackend };

export default class BackendManager {
    #backends = new Map();

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
