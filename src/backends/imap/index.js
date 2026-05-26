import Debug from 'debug';
import Imap from 'imap';
import StorageBackend from '../StorageBackend.js';

const debug = Debug('stored:backend:imap');

/**
 * IMAP storage backend.
 *
 * Addresses messages by `<folder>;UID=<n>` (RFC 5092 path part) — the `key`
 * portion of a `stored://imap:<account>/<folder>;UID=<n>` URL (or the path of
 * the equivalent `imap://<account>/<folder>;UID=<n>` provenance URL).
 *
 * Phase 1 implements `get` (fetch raw RFC 5322 message) and `delete`
 * (mark `\Deleted` + EXPUNGE). `list`/`watch`/`scan` (ingest) still live in the
 * canvas-server ImapService and move here in a later phase.
 *
 * config: { user, password, host, port=993, tls=true, allowSelfSigned=false, account }
 */
export default class ImapBackend extends StorageBackend {
    constructor(name, config = {}) {
        super(name, config);
        this.type = 'remote';
        debug(`ImapBackend "${name}" initialized (account=${config.account ?? config.user ?? '?'})`);
    }

    // Read messages + remove them; appending (write) is not supported here.
    get capabilities() { return { read: true, write: false, delete: true }; }

    #options() {
        const c = this.config;
        return {
            user: c.user,
            password: c.password,
            host: c.host,
            port: c.port || 993,
            tls: c.tls !== false,
            tlsOptions: { rejectUnauthorized: c.allowSelfSigned === false },
            authTimeout: c.authTimeout || 15000,
            connTimeout: c.connTimeout || 15000,
        };
    }

    // `<folder>;UID=<n>` → { folder, uid }. Folder segments are URL-decoded.
    #parseKey(key) {
        const m = String(key || '').match(/^(.*);UID=(\d+)$/i);
        if (!m) throw new Error(`imap key must be "<folder>;UID=<n>", got: ${key}`);
        const folder = m[1].split('/').map((s) => decodeURIComponent(s)).join('/');
        return { folder, uid: Number(m[2]) };
    }

    // Connect, run `fn(imap)`, always end the connection.
    #withConnection(fn) {
        return new Promise((resolve, reject) => {
            const imap = new Imap(this.#options());
            let settled = false;
            const done = (err, val) => {
                if (settled) return;
                settled = true;
                try { imap.end(); } catch { /* already closed */ }
                err ? reject(err) : resolve(val);
            };
            imap.once('ready', () => {
                Promise.resolve()
                    .then(() => fn(imap))
                    .then((v) => done(null, v))
                    .catch((e) => done(e));
            });
            imap.once('error', (e) => done(e));
            imap.connect();
        });
    }

    #openBox(imap, folder, readOnly) {
        return new Promise((resolve, reject) => {
            imap.openBox(folder, readOnly, (err, box) => (err ? reject(err) : resolve(box)));
        });
    }

    async get(key, options = {}) {
        const { folder, uid } = this.#parseKey(key);
        return this.#withConnection((imap) => new Promise((resolve, reject) => {
            this.#openBox(imap, folder, true).then(() => {
                // Top-level fetch is UID-based in node-imap.
                const f = imap.fetch(uid, { bodies: '' });
                let found = false;
                f.on('message', (msg) => {
                    const chunks = [];
                    msg.on('body', (stream) => stream.on('data', (c) => chunks.push(Buffer.from(c))));
                    msg.once('end', () => { found = true; resolve(Buffer.concat(chunks)); });
                });
                f.once('error', reject);
                f.once('end', () => { if (!found) resolve(null); });
            }).catch(reject);
            // `options.stream` not supported for IMAP; buffer is returned regardless.
            void options;
        }));
    }

    async delete(key) {
        const { folder, uid } = this.#parseKey(key);
        return this.#withConnection((imap) => new Promise((resolve, reject) => {
            this.#openBox(imap, folder, false) // read-write
                .then(() => new Promise((res, rej) => imap.addFlags(uid, '\\Deleted', (e) => (e ? rej(e) : res()))))
                .then(() => new Promise((res, rej) => imap.expunge([uid], (e) => (e ? rej(e) : res()))))
                .then(() => { debug(`EXPUNGE ${folder};UID=${uid}`); resolve(true); })
                .catch(reject);
        }));
    }

    async stat(key) {
        const data = await this.get(key);
        return data ? { key, size: data.length, modified: null, created: null } : null;
    }
}
