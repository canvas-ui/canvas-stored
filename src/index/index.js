import { open } from 'lmdb';
import Debug from 'debug';

const debug = Debug('stored:index');

// Durable change log — one entry per location mutation, in commit order.
// `seq` is a monotonically increasing integer (LMDB ordered-binary keys sort
// numbers numerically), persisted alongside the entries so it survives restarts.
const CHANGES_SEQ_KEY = 'changes.seq';
const DEFAULT_TRIM = { keep: 200000, maxAgeMs: 30 * 24 * 60 * 60 * 1000 };
const TRIM_EVERY = 1000;

/**
 * LMDB-backed index mapping id (sha256:xxx) to metadata, plus a path index
 * (`${backend}:${key}` → id) and a durable change log.
 *
 * The change log is the feed remote mirrors tail: every time a location is
 * added, removed or renamed it gets an entry `{ seq, ts, backend, op, key,
 * from?, id, size, mtime, origin? }` written in the SAME transaction as the
 * index mutation, so a reader can never observe an index state the log does
 * not explain. Entries are dirty-key notifications carrying the last known
 * state — consumers re-stat the key rather than replaying ops blindly, which
 * is what makes coalescing and trimming safe.
 */
export default class Index {
    #db;
    #pathDb;
    #changesDb;
    #metaDb;
    #seq = 0;
    #onChange = null;
    #trim;
    // Transaction nesting: the outermost `transaction()` owns the LMDB txn and
    // flushes `#pending` (the entries appended inside it) to `onChange` after
    // commit, so listeners never see an entry that later rolled back.
    #txDepth = 0;
    #pending = [];
    #appendsSinceTrim = 0;

    constructor(path = './.stored/index', options = {}) {
        this.#db = open({ path, name: 'metadata', compression: true });
        this.#pathDb = this.#db.openDB('paths');
        this.#changesDb = this.#db.openDB('changes');
        this.#metaDb = this.#db.openDB('index-meta');
        this.#seq = Number(this.#metaDb.get(CHANGES_SEQ_KEY)) || 0;
        this.#onChange = typeof options.onChange === 'function' ? options.onChange : null;
        this.#trim = { ...DEFAULT_TRIM, ...(options.changes || {}) };
        debug(`Index opened at ${path} (changes head ${this.#seq})`);
    }

    get size() { return this.#db.getKeysCount(); }

    // ─────────────────────────────────────────────────────────────────────────
    // Transactions
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Run `fn` inside one LMDB write transaction. Nested calls join the outer
     * transaction; `put()`/`delete()` use this internally, so a caller that
     * wraps several of them makes the whole group atomic — and the change log
     * coalesces per key inside the group (a delete + put of the same key in
     * one transaction is logged as a single put).
     */
    transaction(fn) {
        if (this.#txDepth > 0) return fn();
        this.#txDepth = 1;
        const seqAtStart = this.#seq;
        let ok = false;
        try {
            const result = this.#db.transactionSync(fn);
            ok = true;
            return result;
        } finally {
            this.#txDepth = 0;
            const pending = this.#pending;
            this.#pending = [];
            if (!ok) {
                this.#seq = seqAtStart;
            } else {
                if (pending.length && this.#onChange) {
                    try { this.#onChange(pending); } catch (err) { debug(`onChange listener failed: ${err.message}`); }
                }
                if (this.#appendsSinceTrim >= TRIM_EVERY) {
                    this.#appendsSinceTrim = 0;
                    try { this.trimChanges(); } catch (err) { debug(`trimChanges failed: ${err.message}`); }
                }
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Core Operations
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Upsert an entry. `options.origin` stamps the resulting change-log
     * entries (who caused the mutation — a device id, a replication peer);
     * `options.rename = { backend, from, to }` folds a removed+added pair on
     * one backend into a single `rename` entry instead of delete + put.
     */
    put(id, metadata, options = {}) {
        // Both the metadata write and the path-index writes commit atomically:
        // a crash can never leave the path index pointing at a missing id.
        return this.transaction(() => {
            const existing = this.#db.get(id);
            const meta = {
                ...existing,
                ...metadata,
                id,
                modified: Date.now(),
                created: existing?.created || Date.now(),
            };

            const before = new Map((existing?.locations || []).map(loc => [`${loc.backend}:${loc.key}`, loc]));
            const after = new Map((meta.locations || []).map(loc => [`${loc.backend}:${loc.key}`, loc]));
            for (const pathKey of before.keys()) {
                if (!after.has(pathKey)) this.#pathDb.removeSync(pathKey);
            }

            this.#db.putSync(id, meta);

            // Index by path for each location
            for (const pathKey of after.keys()) {
                this.#pathDb.putSync(pathKey, id);
            }

            this.#logLocationDiff(meta, before, after, options);
            debug(`Indexed ${id.slice(0, 19)}...`);
            return meta;
        });
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

    delete(id, options = {}) {
        return this.transaction(() => {
            const meta = this.#db.get(id);
            if (!meta) return false;

            for (const loc of meta.locations || []) {
                this.#pathDb.removeSync(`${loc.backend}:${loc.key}`);
                this.#append({
                    backend: loc.backend, op: 'delete', key: loc.key, id,
                    size: loc.size ?? meta.size ?? null, mtime: loc.mtime ?? null, origin: options.origin,
                });
            }

            this.#db.removeSync(id);
            debug(`Removed ${id.slice(0, 19)}...`);
            return true;
        });
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Change log
    // ─────────────────────────────────────────────────────────────────────────

    #logLocationDiff(meta, before, after, { origin = null, rename = null } = {}) {
        const removed = [...before.keys()].filter(k => !after.has(k));
        const added = [...after.keys()].filter(k => !before.has(k));
        if (!removed.length && !added.length) return;

        let paired = null;
        if (rename?.backend && rename.from != null && rename.to != null) {
            const fromKey = `${rename.backend}:${rename.from}`;
            const toKey = `${rename.backend}:${rename.to}`;
            if (removed.includes(fromKey) && added.includes(toKey)) paired = { fromKey, toKey };
        }

        for (const pathKey of removed) {
            if (paired && pathKey === paired.fromKey) continue;
            const loc = before.get(pathKey);
            this.#append({
                backend: loc.backend, op: 'delete', key: loc.key, id: meta.id,
                size: loc.size ?? meta.size ?? null, mtime: loc.mtime ?? null, origin,
            });
        }
        for (const pathKey of added) {
            const loc = after.get(pathKey);
            const entry = {
                backend: loc.backend, op: 'put', key: loc.key, id: meta.id,
                size: loc.size ?? meta.size ?? null, mtime: loc.mtime ?? null, origin,
            };
            if (paired && pathKey === paired.toKey) { entry.op = 'rename'; entry.from = rename.from; }
            this.#append(entry);
        }
    }

    // Append one entry, coalescing per (backend, key) within the current
    // transaction: the final state of a key is what matters to a reader, and
    // a delete immediately followed by a put (content succession) must not
    // read as "gone" halfway through.
    #append(entry) {
        const clean = { ...entry };
        if (clean.origin == null) delete clean.origin;
        const prior = this.#pending.find(p => p.backend === clean.backend && p.key === clean.key);
        if (prior) {
            const merged = { ...clean, seq: prior.seq, ts: Date.now() };
            // A rename's `from` survives a later put of the same key in the
            // same transaction (rename then content refresh) — it is still
            // where the bytes came from.
            if (prior.op === 'rename' && merged.op === 'put') { merged.op = 'rename'; merged.from = prior.from; }
            this.#changesDb.putSync(prior.seq, merged);
            Object.assign(prior, merged);
            return prior;
        }
        this.#seq += 1;
        const full = { seq: this.#seq, ts: Date.now(), ...clean };
        this.#changesDb.putSync(this.#seq, full);
        this.#metaDb.putSync(CHANGES_SEQ_KEY, this.#seq);
        this.#pending.push(full);
        this.#appendsSinceTrim += 1;
        return full;
    }

    /** Sequence number of the newest entry (0 when nothing was ever logged). */
    head() { return this.#seq; }

    /** Sequence number of the oldest retained entry, or null when the log is empty. */
    oldest() {
        for (const { key } of this.#changesDb.getRange({ limit: 1 })) return key;
        return null;
    }

    /**
     * Entries after `since` (exclusive), oldest first, optionally filtered by
     * backend. `cursor` is the last examined seq — pass it back as `since` to
     * continue; when it equals `head` the reader is caught up. `cursorTooOld`
     * means entries the reader never saw have been trimmed: rebuild from a
     * full listing instead of tailing.
     */
    changes({ backend = null, since = 0, limit = 1000 } = {}) {
        const from = Math.max(0, Math.floor(Number(since) || 0));
        const max = Math.max(1, Math.floor(Number(limit) || 1000));
        const head = this.#seq;
        const oldest = this.oldest();
        const floor = oldest ?? (head + 1);
        const cursorTooOld = from + 1 < floor;

        const changes = [];
        let cursor = from;
        if (!cursorTooOld) {
            for (const { key, value } of this.#changesDb.getRange({ start: from + 1 })) {
                cursor = key;
                if (backend && value.backend !== backend) continue;
                changes.push(value);
                if (changes.length >= max) break;
            }
        }
        return { changes, head, oldest, cursor, cursorTooOld };
    }

    /**
     * Drop entries beyond the retention window: keep at most `keep` entries
     * and nothing older than `maxAgeMs`. Returns the number removed.
     */
    trimChanges(options = {}) {
        const keep = Math.max(0, Number(options.keep ?? this.#trim.keep) || 0);
        const maxAgeMs = Math.max(0, Number(options.maxAgeMs ?? this.#trim.maxAgeMs) || 0);
        const minSeq = this.#seq - keep + 1;
        const cutoff = Date.now() - maxAgeMs;

        const doomed = [];
        for (const { key, value } of this.#changesDb.getRange({})) {
            if (key < minSeq || (value?.ts ?? 0) < cutoff) doomed.push(key);
            else break;
        }
        if (!doomed.length) return 0;
        this.transaction(() => {
            for (const key of doomed) this.#changesDb.removeSync(key);
        });
        debug(`Trimmed ${doomed.length} change-log entries`);
        return doomed.length;
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

    /**
     * Page through one backend's locations in key order via the path index
     * (a range scan, no full-table walk). Returns `{ objects, cursor }`; pass
     * `cursor` back as `after` for the next page, a null cursor means done.
     */
    locationsByBackend(backendName, { prefix = '', after = null, limit = 1000 } = {}) {
        const base = `${backendName}:`;
        const max = Math.max(1, Math.floor(Number(limit) || 1000));
        const start = `${base}${after != null && after >= prefix ? after : prefix}`;
        const end = `${base}${prefix}￿`;
        const objects = [];
        let cursor = null;
        for (const { key: pathKey, value: id } of this.#pathDb.getRange({ start, end })) {
            if (typeof pathKey !== 'string' || !pathKey.startsWith(base)) continue;
            const key = pathKey.slice(base.length);
            if (prefix && !key.startsWith(prefix)) continue;
            if (after != null && key <= after) continue;
            const meta = this.#db.get(id);
            const loc = meta?.locations?.find(l => l.backend === backendName && l.key === key) || null;
            objects.push({
                key,
                id,
                checksums: meta?.checksums || null,
                size: loc?.size ?? meta?.size ?? null,
                mtime: loc?.mtime ?? null,
                mimeType: meta?.mimeType ?? null,
                synced: loc?.synced ?? null,
            });
            cursor = key;
            if (objects.length >= max) break;
        }
        return { objects, cursor: objects.length >= max ? cursor : null };
    }

    *entries() {
        for (const { key, value } of this.#db.getRange()) {
            yield [key, value];
        }
    }

    clear() {
        this.#db.clearSync();
        this.#pathDb.clearSync();
        this.#changesDb.clearSync();
        this.#metaDb.clearSync();
        this.#seq = 0;
    }

    close() {
        this.#db.close();
    }
}
