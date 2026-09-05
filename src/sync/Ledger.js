import { open } from 'lmdb';
import Debug from 'debug';

const debug = Debug('stored:sync:ledger');

/**
 * Per-mirror base ledger — what the device last agreed with the hub.
 *
 * One LMDB sub-database (`mirror`, in the stored index env) keyed by
 * `<kind>/<mirror>/<key>`:
 *   - `base/<mirror>/<key>` → `{ sha256, size, mtime, remoteSeq, at }`
 *   - `cursor/<mirror>`     → hub change-feed seq reconciled up to
 *   - `skip/<mirror>/<key>` → `{ reason, ts }` (keys the mirror refuses)
 *   - `state/<mirror>`      → free-form engine state (instanceId, head, …)
 *
 * Keys and digests are the identity; hub document ids are never stored.
 */
export default class Ledger {
    #db;
    #own = null;
    #mirror;

    constructor({ index = null, path = null, mirror, name = 'mirror' } = {}) {
        if (!mirror) throw new Error('Ledger requires a mirror id');
        this.#mirror = String(mirror);
        if (index && typeof index.openDB === 'function') {
            this.#db = index.openDB(name);
        } else if (path) {
            this.#own = open({ path, name, compression: true });
            this.#db = this.#own;
        } else {
            throw new Error('Ledger requires { index } or { path }');
        }
    }

    get mirror() { return this.#mirror; }

    #k(kind, key = null) { return key == null ? `${kind}/${this.#mirror}` : `${kind}/${this.#mirror}/${key}`; }

    // ── base ─────────────────────────────────────────────────────────────

    getBase(key) { return this.#db.get(this.#k('base', key)) || null; }

    setBase(key, { sha256, size = null, mtime = null, remoteSeq = 0 } = {}) {
        if (!sha256) throw new Error(`Ledger.setBase(${key}): sha256 required`);
        const base = { sha256: String(sha256).toLowerCase(), size, mtime, remoteSeq: Number(remoteSeq) || 0, at: Date.now() };
        this.#db.putSync(this.#k('base', key), base);
        debug(`base ${key} = ${base.sha256.slice(0, 12)} (seq ${base.remoteSeq})`);
        return base;
    }

    removeBase(key) {
        const had = this.#db.get(this.#k('base', key));
        if (had) this.#db.removeSync(this.#k('base', key));
        return !!had;
    }

    /** Re-key a base (rename): returns the moved base or null. */
    moveBase(from, to, patch = {}) {
        return this.#db.transactionSync(() => {
            const base = this.#db.get(this.#k('base', from));
            if (!base) return null;
            this.#db.removeSync(this.#k('base', from));
            const next = { ...base, ...patch, at: Date.now() };
            this.#db.putSync(this.#k('base', to), next);
            return next;
        });
    }

    /** All bases in key order: yields `[key, base]`. */
    *bases(prefix = '') {
        const start = this.#k('base', prefix);
        const end = `${this.#k('base', prefix)}￿`;
        const head = `${this.#k('base')}/`;
        for (const { key, value } of this.#db.getRange({ start, end })) {
            if (typeof key !== 'string' || !key.startsWith(head)) continue;
            yield [key.slice(head.length), value];
        }
    }

    // ── cursor / state ───────────────────────────────────────────────────

    get cursor() { const v = this.#db.get(this.#k('cursor')); return v == null ? null : Number(v); }
    set cursor(seq) { this.#db.putSync(this.#k('cursor'), Number(seq) || 0); }

    getState() { return this.#db.get(this.#k('state')) || {}; }
    setState(patch) {
        const next = { ...this.getState(), ...patch };
        this.#db.putSync(this.#k('state'), next);
        return next;
    }

    // ── skips ────────────────────────────────────────────────────────────

    skip(key, reason) {
        const entry = { reason: String(reason), ts: Date.now() };
        this.#db.putSync(this.#k('skip', key), entry);
        return entry;
    }

    unskip(key) {
        const had = this.#db.get(this.#k('skip', key));
        if (had) this.#db.removeSync(this.#k('skip', key));
        return !!had;
    }

    isSkipped(key) { return !!this.#db.get(this.#k('skip', key)); }

    *skips() {
        const head = `${this.#k('skip')}/`;
        for (const { key, value } of this.#db.getRange({ start: head, end: `${head}￿` })) {
            if (typeof key !== 'string' || !key.startsWith(head)) continue;
            yield [key.slice(head.length), value];
        }
    }

    /** Wipe this mirror's ledger (bases, cursor, skips, state). */
    clear() {
        this.#db.transactionSync(() => {
            for (const kind of ['base', 'skip']) {
                const head = `${this.#k(kind)}/`;
                for (const { key } of this.#db.getRange({ start: head, end: `${head}￿` })) {
                    if (typeof key === 'string' && key.startsWith(head)) this.#db.removeSync(key);
                }
            }
            this.#db.removeSync(this.#k('cursor'));
            this.#db.removeSync(this.#k('state'));
        });
    }

    close() {
        if (this.#own) { this.#own.close(); this.#own = null; }
    }
}
