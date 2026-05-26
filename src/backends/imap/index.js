import Debug from 'debug';
import StorageBackend from '../StorageBackend.js';

const debug = Debug('stored:backend:imap');

/**
 * IMAP storage backend — SKELETON.
 *
 * Registered so `stored://imap:<account>/<key>` URLs parse and dispatch.
 * CRUD inherits `Not implemented`. A follow-up should fetch a raw message by
 * key (`<folder>;UID=<n>`, RFC 5092) via the existing IMAP connector used by
 * canvas-server's imap sync service. `config` carries { account, host, ... }.
 */
export default class ImapBackend extends StorageBackend {
    constructor(name, config = {}) {
        super(name, config);
        this.type = 'remote';
        debug(`ImapBackend "${name}" registered (skeleton; account=${config.account ?? '?'})`);
    }
}
