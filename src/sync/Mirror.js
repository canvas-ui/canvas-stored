import { EventEmitter } from 'events';
import Debug from 'debug';
import JobQueue from './JobQueue.js';
import Ledger from './Ledger.js';
import { normalizeKey, validateKey, isIgnored, matchesPrefixes, conflictKey, MIRROR_IGNORE_DEFAULTS } from './keys.js';

const debug = Debug('stored:sync:mirror');

const DEFAULTS = {
    prefixes: [],
    ignore: [],
    deletes: 'propagate',
    conflictMode: 'prompt',
    debounceMs: 1500,
    fullReconcileEvery: 6 * 60 * 60_000,
    concurrency: 2,
    offlineBackoffMs: [5_000, 10_000, 20_000, 40_000, 60_000],
};

/**
 * The three-way decision per key (protocol doc, "Client algorithm"). Pure.
 * `L` = local sha, `B` = base ledger sha, `R` = hub sha; null = absent.
 */
export function decide(L, B, R) {
    const localChanged = L !== B;
    const remoteChanged = R !== B;
    if (!localChanged && !remoteChanged) return { action: 'nothing' };
    if (localChanged && !remoteChanged) {
        if (L) return { action: 'push', ifMatch: B };
        return B ? { action: 'delete-remote', ifMatch: B } : { action: 'nothing' };
    }
    if (!localChanged && remoteChanged) {
        if (R) return { action: 'pull', sha256: R };
        return L ? { action: 'trash-local' } : { action: 'nothing' };
    }
    // Both changed.
    if (L === R) return { action: 'adopt' };
    if (!L && R) return { action: 'pull', sha256: R };           // edit beats delete
    if (L && !R) return { action: 'push', ifMatch: null };        // edit beats delete: push as new
    return { action: 'conflict', remote: R };
}

/**
 * Device-mirror sync engine: keeps a local folder (a `file` backend) equal
 * to one backend of a workspace on a canvas hub (a `canvas` backend), both
 * registered on the same `Stored`.
 *
 * Stored's index is the view of both sides — local keys via the file
 * watcher/scan, hub keys via the canvas driver's change-feed poller — and
 * the ledger (LMDB sub-db) holds the base: what the two last agreed on.
 * Every `object:*` event on either backend marks its key dirty; a debounced
 * pass runs the three-way table (`decide`) per dirty key and turns the
 * outcome into durable jobs (`JobQueue`): push, pull, delete-remote,
 * trash-local, rename-remote, rename-local, conflict. A worker (concurrency
 * 2, one job per key) executes them through Stored's own copy/move/rename
 * primitives so locations, succession and echo suppression behave exactly
 * as for any other transfer. Base is written only after the byte operation
 * succeeded; the feed cursor only after a whole batch reconciled.
 *
 * Events: `status` (snapshot), `job:start|done|failed` (job), `conflict`,
 * `skip`, `offline`, `online`, `error`.
 */
export default class Mirror extends EventEmitter {
    #stored;
    #opts;
    #ledger;
    #queue;
    #state = 'idle';
    #dirty = new Set();
    #debounceTimer = null;
    #fullTimer = null;
    #offlineTimer = null;
    #offlineAttempts = 0;
    #retryTimer = null;
    #running = new Map();      // seq → promise
    #runningKeys = new Set();
    #drainLoop = null;
    #pendingCursor = null;
    #lastSyncAt = null;
    #lastError = null;
    #conflicts = 0;
    #stopping = false;
    #started = false;
    #unbind = [];
    #ignorePatterns;
    #reconcilePass = null;

    constructor(stored, options = {}) {
        super();
        if (!stored) throw new Error('Mirror requires a Stored instance');
        const opts = { ...DEFAULTS, ...options };
        for (const k of ['id', 'local', 'remote', 'trash', 'conflicts']) {
            if (!opts[k]) throw new Error(`Mirror requires option "${k}"`);
        }
        // `conflicts` names the conflicts backend; the policy is `conflictMode`
        // ('prompt' = inbox upload, 'rename' = Dropbox-style copy on the hub).
        if (!['prompt', 'rename'].includes(opts.conflictMode)) opts.conflictMode = 'prompt';
        if (!['propagate', 'keep'].includes(opts.deletes)) opts.deletes = 'propagate';
        this.#stored = stored;
        this.#opts = opts;
        this.#ignorePatterns = [...MIRROR_IGNORE_DEFAULTS, ...(opts.ignore || [])];
        this.#ledger = new Ledger({ index: stored.index, mirror: opts.id });
        this.#queue = new JobQueue({ index: stored.index });
        const st = this.#ledger.getState();
        this.#conflicts = Number(st.conflicts) || 0;
        this.#lastSyncAt = st.lastSyncAt ?? null;
        debug(`Mirror ${opts.id}: ${opts.local} ⇄ ${opts.remote} (trash ${opts.trash}, conflicts ${opts.conflicts}, ${opts.conflictMode}, deletes ${opts.deletes})`);
    }

    get id() { return this.#opts.id; }
    get state() { return this.#state; }
    get ledger() { return this.#ledger; }
    get queue() { return this.#queue; }
    get options() { return { ...this.#opts }; }
    get deviceId() { return this.#opts.deviceId || this.#remote?.deviceId || null; }
    get deviceName() { return this.#opts.deviceName || this.#remote?.deviceName || this.deviceId || 'device'; }

    get #remote() { return this.#stored.getBackend(this.#opts.remote); }
    get #local() { return this.#stored.getBackend(this.#opts.local); }

    // ─────────────────────────────────────────────────────────────────────────
    // Lifecycle
    // ─────────────────────────────────────────────────────────────────────────

    async start() {
        if (this.#started) return this.status();
        for (const name of [this.#opts.local, this.#opts.remote, this.#opts.trash, this.#opts.conflicts]) {
            if (!this.#stored.getBackend(name)) throw new Error(`Mirror ${this.#opts.id}: backend "${name}" is not registered`);
        }
        if (typeof this.#remote.putStream !== 'function' || typeof this.#remote.poll !== 'function') {
            throw new Error(`Mirror ${this.#opts.id}: remote backend "${this.#opts.remote}" must be a canvas driver`);
        }
        this.#started = true;
        this.#stopping = false;
        this.#setState('starting');
        this.#bind();
        try {
            await this.#stored.scan(this.#opts.local);
            await this.#catchUp();
            await this.reconcileAll();
            await this.#remote.watch();
            this.#setState('online');
        } catch (err) {
            if (this.#isOffline(err)) this.#goOffline(err);
            else { this.#lastError = err.message; this.emit('error', err); this.#setState('online'); }
        }
        if (this.#opts.fullReconcileEvery > 0) {
            this.#fullTimer = setInterval(() => { this.fullReconcile().catch(() => {}); }, this.#opts.fullReconcileEvery);
            this.#fullTimer.unref?.();
        }
        this.#kick();
        return this.status();
    }

    /** Drain gracefully: no new jobs are started, running ones finish. */
    async stop() {
        if (!this.#started) return;
        this.#stopping = true;
        this.#setState('stopping');
        for (const t of [this.#debounceTimer, this.#fullTimer, this.#offlineTimer, this.#retryTimer]) if (t) clearTimeout(t);
        this.#debounceTimer = this.#fullTimer = this.#offlineTimer = this.#retryTimer = null;
        for (const off of this.#unbind.splice(0)) off();
        await this.#remote?.stop?.().catch(() => {});
        if (this.#drainLoop) await this.#drainLoop.catch(() => {});
        await Promise.allSettled([...this.#running.values()]);
        if (this.#reconcilePass) await this.#reconcilePass.catch(() => {});
        this.#ledger.setState({ lastSyncAt: this.#lastSyncAt, conflicts: this.#conflicts });
        this.#started = false;
        this.#setState('stopped');
    }

    /** Poll the hub now, reconcile what changed, run the queue. */
    async nudge() {
        if (!this.#started || this.#stopping) return this.status();
        if (this.#state === 'offline') { await this.#tryOnline(); return this.status(); }
        const r = await this.#remote.poll();
        if (!r.ok && r.reason === 'cursor-too-old') await this.#rebuild();
        else if (!r.ok && r.reason === 'offline') { this.#goOffline(new Error(r.error || 'offline')); return this.status(); }
        await this.#reconcileDirty();
        this.#kick();
        return this.status();
    }

    /** Full pass: rescan the local folder, reconcile every known key. */
    async fullReconcile() {
        if (!this.#started || this.#stopping || this.#state === 'offline') return this.status();
        await this.#stored.scan(this.#opts.local);
        await this.reconcileAll();
        this.#kick();
        return this.status();
    }

    /** Rebuild the hub view from a full listing (as after a 410), then reconcile. */
    async resync() {
        if (!this.#started || this.#stopping) return this.status();
        await this.#stored.scan(this.#opts.local);
        await this.#rebuild();
        this.#kick();
        return this.status();
    }

    /** Wait until the queue is empty and no reconcile pass is running (tests, CLI `sync now`). */
    async idle({ timeout = 30_000 } = {}) {
        const until = Date.now() + timeout;
        for (;;) {
            if (this.#reconcilePass) await this.#reconcilePass.catch(() => {});
            if (this.#drainLoop) await this.#drainLoop.catch(() => {});
            const c = this.#queue.counters(this.#opts.id);
            const due = [...this.#queue.pending(Date.now(), this.#opts.id)].length;
            if (!this.#dirty.size && !this.#running.size && due === 0 && !this.#debounceTimer && !this.#reconcilePass) {
                return { ...c, drained: c.pending === 0 };
            }
            if (Date.now() > until) return { ...c, drained: false, timeout: true };
            if (this.#state === 'offline') return { ...c, drained: false, offline: true };
            await new Promise((r) => setTimeout(r, 25));
        }
    }

    status() {
        const counters = this.#queue.counters(this.#opts.id);
        const skips = [...this.#ledger.skips()];
        return {
            id: this.#opts.id,
            state: this.#state,
            cursor: this.#ledger.cursor,
            head: this.#remote?.head ?? null,
            pending: counters.pending + counters.running,
            running: counters.running,
            failed: counters.failed,
            conflicts: this.#conflicts,
            lastSyncAt: this.#lastSyncAt,
            lastError: this.#lastError,
            skipped: skips.length,
            skips: Object.fromEntries(skips.map(([k, v]) => [k, v.reason])),
            dirty: this.#dirty.size,
        };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Event wiring
    // ─────────────────────────────────────────────────────────────────────────

    #bind() {
        const local = this.#opts.local;
        const remote = this.#opts.remote;
        const on = (event, handler) => {
            this.#stored.on(event, handler);
            this.#unbind.push(() => this.#stored.off(event, handler));
        };
        on('object:add', (e) => {
            if (e.backend === local) {
                if (e.renamedFrom) this.#localRenamed(normalizeKey(e.renamedFrom), normalizeKey(e.key));
                else this.#markDirty(e.key);
            } else if (e.backend === remote) {
                if (e.renamedFrom) this.#remoteRenamed(normalizeKey(e.renamedFrom), normalizeKey(e.key));
                else this.#markDirty(e.key);
            }
        });
        on('object:unlink', (e) => { if (e.backend === local || e.backend === remote) this.#markDirty(e.key); });
        on('object:location:add', (e) => { if (e.backend === local || e.backend === remote) this.#markDirty(e.key); });
        on('object:location:remove', (e) => { if (e.backend === local || e.backend === remote) this.#markDirty(e.key); });
        on('object:move', (e) => {
            for (const end of [e.from, e.to]) {
                if (end && (end.backend === local || end.backend === remote)) this.#markDirty(end.key);
            }
        });
        on('backend:state', (e) => {
            if (e.backend !== remote) return;
            if (e.reason === 'polled') {
                this.#pendingCursor = e.cursor;
                if (this.#state === 'offline') this.#tryOnline().catch(() => {});
                return;
            }
            if (e.reason === 'cursor-too-old') {
                this.#rebuild().then(() => this.#kick()).catch((err) => { if (this.#isOffline(err)) this.#goOffline(err); });
                return;
            }
            if (e.online === false) this.#goOffline(new Error(e.error || e.reason || 'offline'));
        });
    }

    #markDirty(rawKey) {
        if (this.#stopping) return;
        const key = normalizeKey(rawKey);
        if (!key) return;
        this.#dirty.add(key);
        if (this.#debounceTimer) return;
        this.#debounceTimer = setTimeout(() => {
            this.#debounceTimer = null;
            this.#reconcileDirty().then(() => this.#kick()).catch((err) => this.#report(err));
        }, this.#opts.debounceMs);
        this.#debounceTimer.unref?.();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Catch-up / reconcile
    // ─────────────────────────────────────────────────────────────────────────

    async #catchUp() {
        const remote = this.#remote;
        const cursor = this.#ledger.cursor;
        if (cursor == null) return this.#rebuild({ reconcile: false });
        if (remote.cursor == null) remote.cursor = cursor;
        const r = await remote.poll();
        if (r.ok) { this.#pendingCursor = r.cursor; return; }
        if (r.reason === 'cursor-too-old') return this.#rebuild({ reconcile: false });
        if (r.reason === 'offline') throw Object.assign(new Error(r.error || 'hub offline'), { code: 'OFFLINE' });
        throw Object.assign(new Error(r.error || `poll failed: ${r.reason}`), { code: r.reason });
    }

    // Full listing → Stored index for the remote backend (stale remote
    // locations are dropped by scan), cursor := listing head.
    async #rebuild({ reconcile = true } = {}) {
        this.#setState('scanning');
        const remote = this.#remote;
        remote.cursor = null;
        const result = await this.#stored.scan(this.#opts.remote);
        if (!result.ok || result.complete === false) {
            const reason = result.errors?.[this.#opts.remote]?.root || result.errors?.root || 'unreachable';
            const err = Object.assign(new Error(`hub listing failed: ${reason}`), { code: reason === 'OFFLINE' || reason === 'offline' ? 'OFFLINE' : reason });
            throw err;
        }
        this.#pendingCursor = remote.cursor;
        debug(`rebuilt hub view: ${result.count} objects, cursor ${remote.cursor}`);
        if (reconcile) await this.reconcileAll();
        else if (this.#state === 'scanning') this.#setState('syncing');
    }

    /** Reconcile the union of local keys, hub keys and ledger bases. */
    async reconcileAll() {
        const keys = new Set();
        for (const name of [this.#opts.local, this.#opts.remote]) {
            let after = null;
            do {
                const page = this.#stored.listObjects(name, { after, limit: 5000 });
                for (const o of page.objects) keys.add(normalizeKey(o.key));
                after = page.cursor;
            } while (after);
        }
        for (const [key] of this.#ledger.bases()) keys.add(key);
        for (const key of keys) this.#dirty.add(key);
        await this.#reconcileDirty();
        return keys.size;
    }

    async #reconcileDirty() {
        if (this.#reconcilePass) { await this.#reconcilePass; if (!this.#dirty.size) return; }
        this.#reconcilePass = (async () => {
            const wasState = this.#state;
            if (wasState === 'online') this.#setState('syncing');
            try {
                while (this.#dirty.size) {
                    const batch = [...this.#dirty].sort();
                    this.#dirty.clear();
                    for (const key of batch) {
                        try { this.#reconcileKey(key); }
                        catch (err) { this.#report(err); }
                    }
                }
                // The whole batch reconciled (decisions durable in the queue):
                // the feed position may move.
                if (this.#pendingCursor != null && this.#pendingCursor !== this.#ledger.cursor) {
                    this.#ledger.cursor = this.#pendingCursor;
                }
            } finally {
                if (this.#state === 'syncing' || this.#state === 'scanning') this.#setState(this.#stopping ? 'stopping' : 'online');
            }
        })().finally(() => { this.#reconcilePass = null; });
        await this.#reconcilePass;
    }

    #local3(key) {
        const l = this.#stored.index.get(`${this.#opts.local}:${key}`);
        const r = this.#stored.index.get(`${this.#opts.remote}:${key}`);
        const loc = (meta, backend) => meta?.locations?.find((x) => x.backend === backend && x.key === key) || null;
        return {
            L: l?.checksums?.sha256 ?? null,
            Lmeta: l,
            Lloc: loc(l, this.#opts.local),
            B: this.#ledger.getBase(key)?.sha256 ?? null,
            base: this.#ledger.getBase(key),
            R: r?.checksums?.sha256 ?? null,
            Rmeta: r,
            Rloc: loc(r, this.#opts.remote),
        };
    }

    #inScope(key) {
        if (!matchesPrefixes(key, this.#opts.prefixes)) return false;
        if (isIgnored(key, this.#ignorePatterns)) return false;
        const local = this.#local;
        if (typeof local?.isIgnored === 'function' && local.isIgnored(key)) return false;
        return true;
    }

    #skip(key, reason) {
        if (!this.#ledger.isSkipped(key)) {
            this.#ledger.skip(key, reason);
            debug(`skip ${key}: ${reason}`);
            this.emit('skip', { key, reason });
        }
    }

    // Decide for one key and queue the outcome. Synchronous: reads only the
    // index and the ledger. Runtime confirmation (HEAD) happens in the job.
    #reconcileKey(key) {
        if (!key || !this.#inScope(key)) return null;
        const invalid = validateKey(key);
        if (invalid) { this.#skip(key, invalid); return null; }
        if (this.#ledger.isSkipped(key)) this.#ledger.unskip(key);
        const { L, B, R, Lloc, Rloc, base } = this.#local3(key);
        const d = decide(L, B, R);
        debug(`reconcile ${key}: L=${L?.slice(0, 8) ?? '∅'} B=${B?.slice(0, 8) ?? '∅'} R=${R?.slice(0, 8) ?? '∅'} → ${d.action}`);
        switch (d.action) {
            case 'nothing':
                return d;
            case 'push':
                if (!d.ifMatch && base) this.#ledger.removeBase(key);   // hub deleted it: the base is void
                this.#enqueue('push', key, { sha256: L, ifMatch: d.ifMatch ?? null });
                return d;
            case 'delete-remote':
                if (this.#opts.deletes === 'propagate') this.#enqueue('delete-remote', key, { ifMatch: d.ifMatch });
                else this.#ledger.removeBase(key);
                return d;
            case 'pull':
                this.#enqueue('pull', key, { sha256: R, mtime: Rloc?.mtime ?? null, size: Rloc?.size ?? null });
                return d;
            case 'trash-local':
                this.#enqueue('trash-local', key, { sha256: L });
                return d;
            case 'adopt':
                this.#queue.cancelByKey(key, this.#opts.id);
                if (L) this.#ledger.setBase(key, { sha256: L, size: Lloc?.size ?? Rloc?.size ?? null, mtime: Rloc?.mtime ?? Lloc?.mtime ?? null, remoteSeq: this.#remote?.head ?? 0 });
                else this.#ledger.removeBase(key);
                return d;
            case 'conflict':
                this.#enqueue('conflict', key, { local: L, base: B, remote: R });
                return d;
            default:
                return d;
        }
    }

    #enqueue(kind, key, payload = {}, dedupe = null) {
        // A newer decision for the key supersedes the older pending ones.
        for (const job of this.#queue.byKey(key, this.#opts.id)) {
            if (job.state === 'pending' && job.kind !== kind && job.kind !== 'rename-remote' && job.kind !== 'rename-local') this.#queue.cancel(job.seq);
        }
        return this.#queue.append({ kind, mirror: this.#opts.id, key, payload, dedupe });
    }

    // Local `mv`: the base follows the key; the hub gets a rename when it knew
    // the source. A file never pushed is simply pushed under its new name.
    #localRenamed(from, to) {
        const base = this.#ledger.getBase(from);
        if (!base || this.#ledger.getBase(to) || !this.#inScope(from) || !this.#inScope(to)) {
            this.#markDirty(from);
            this.#markDirty(to);
            return;
        }
        this.#ledger.moveBase(from, to);
        for (const job of this.#queue.byKey(from, this.#opts.id)) if (job.state === 'pending' && job.kind === 'push') this.#queue.cancel(job.seq);
        this.#queue.append({ kind: 'rename-remote', mirror: this.#opts.id, key: to, payload: { from, to }, dedupe: `rename-remote|${from}→${to}` });
        this.#markDirty(to);
    }

    // Hub-side rename: re-key locally when our copy is clean (a job, so it is
    // durable and serialized with the rest); a dirty source stays put.
    #remoteRenamed(from, to) {
        const { L, B } = this.#local3(from);
        if (L && L === B && !this.#stored.index.get(`${this.#opts.local}:${to}`) && this.#inScope(from) && this.#inScope(to)) {
            this.#queue.append({ kind: 'rename-local', mirror: this.#opts.id, key: to, payload: { from, to }, dedupe: `rename-local|${from}→${to}` });
            this.#kick();
            return;
        }
        this.#markDirty(from);
        this.#markDirty(to);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Worker
    // ─────────────────────────────────────────────────────────────────────────

    #jobKeys(job) {
        const keys = new Set([job.key]);
        if (job.payload?.from) keys.add(job.payload.from);
        if (job.payload?.to) keys.add(job.payload.to);
        keys.delete(null); keys.delete(undefined);
        return [...keys];
    }

    #kick() {
        if (!this.#started || this.#stopping || this.#state === 'offline' || this.#state === 'stopped') return;
        if (this.#drainLoop) return;
        this.#drainLoop = this.#drain().catch((err) => this.#report(err)).finally(() => {
            this.#drainLoop = null;
            const c = this.#queue.counters(this.#opts.id);
            if (c.pending > 0 && c.nextAt != null && !this.#retryTimer && !this.#stopping) {
                this.#retryTimer = setTimeout(() => { this.#retryTimer = null; this.#kick(); }, Math.max(50, c.nextAt - Date.now()));
                this.#retryTimer.unref?.();
            }
        });
    }

    async #drain() {
        for (;;) {
            if (this.#stopping || this.#state === 'offline') break;
            let started = 0;
            for (const job of [...this.#queue.pending(Date.now(), this.#opts.id)]) {
                if (this.#running.size >= this.#opts.concurrency) break;
                const keys = this.#jobKeys(job);
                if (keys.some((k) => this.#runningKeys.has(k))) continue;
                const taken = this.#queue.take(job.seq);
                if (!taken) continue;
                started += 1;
                for (const k of keys) this.#runningKeys.add(k);
                const p = this.#execute(taken).finally(() => {
                    this.#running.delete(taken.seq);
                    for (const k of keys) this.#runningKeys.delete(k);
                });
                this.#running.set(taken.seq, p);
            }
            if (!this.#running.size) break;
            await Promise.race([...this.#running.values()]);
            if (!started && !this.#running.size) break;
        }
    }

    async #execute(job) {
        this.emit('job:start', job);
        try {
            await this.#runJob(job);
            this.#queue.complete(job.seq);
            this.#lastSyncAt = Date.now();
            this.#lastError = null;
            this.emit('job:done', job);
            this.emit(`job:${job.kind}`, job);
        } catch (err) {
            if (this.#isOffline(err)) {
                this.#queue.retry(job.seq);
                this.#goOffline(err);
                return;
            }
            const permanent = !!err?.permanent || ['REFUSED', 'UNAUTHORIZED'].includes(err?.code);
            const failed = this.#queue.fail(job.seq, err, { permanent });
            this.#lastError = `${job.kind} ${job.key}: ${err.message}`;
            if (permanent && job.key) this.#skip(job.key, err.hubCode || err.code || 'refused');
            debug(`job #${job.seq} ${job.kind} ${job.key} failed: ${err.message}`);
            this.emit('job:failed', { ...failed, error: err });
        }
    }

    async #runJob(job) {
        switch (job.kind) {
            case 'push': return this.#runPush(job);
            case 'pull': return this.#runPull(job);
            case 'delete-remote': return this.#runDeleteRemote(job);
            case 'trash-local': return this.#runTrashLocal(job);
            case 'rename-remote': return this.#runRenameRemote(job);
            case 'rename-local': return this.#runRenameLocal(job);
            case 'conflict': return this.#runConflict(job);
            default: throw Object.assign(new Error(`unknown job kind ${job.kind}`), { code: 'REFUSED' });
        }
    }

    // A driver call that failed with a typed `{ ok:false }` from Stored.
    #failure(result, key) {
        const err = new Error(result.error || `${result.reason}${key ? ` (${key})` : ''}`);
        err.code = result.code || (result.reason === 'target-offline' || result.reason === 'source-offline' ? 'OFFLINE' : null);
        err.reason = result.reason;
        err.current = result.current;
        return err;
    }

    // Make sure the remote view of `key` is current before reading bytes from it.
    async #freshRemote(key, sha256 = null) {
        let meta = this.#stored.index.get(`${this.#opts.remote}:${key}`);
        if (meta && (!sha256 || meta.checksums?.sha256 === sha256)) return meta;
        const r = await this.#remote.poll();
        if (!r.ok && r.reason === 'offline') throw Object.assign(new Error(r.error || 'offline'), { code: 'OFFLINE' });
        if (!r.ok && r.reason === 'cursor-too-old') await this.#rebuild({ reconcile: false });
        meta = this.#stored.index.get(`${this.#opts.remote}:${key}`);
        return meta || null;
    }

    async #runPush(job) {
        const key = job.key;
        const { L, B, Lloc, Lmeta } = this.#local3(key);
        if (!L || !Lmeta) { this.#markDirty(key); return; }
        const stat = await this.#remote.stat(key);           // confirm R
        const R = stat?.checksums?.sha256 ?? null;
        if (R !== B) {
            // The hub moved since the decision — decide again with what it has.
            await this.#redecide(key, R);
            return;
        }
        const res = await this.#stored.copy(`${this.#opts.local}:${key}`, {
            to: this.#opts.remote,
            key,
            from: this.#opts.local,
            mtime: Lloc?.mtime ?? undefined,
            origin: this.deviceId || undefined,
            ...(B ? { ifMatch: B } : { ifNoneMatch: '*' }),
            onConflict: 'overwrite',
        });
        if (res.ok) {
            const seq = res.remote?.seq ?? this.#remote.head ?? 0;
            const mtime = res.remote?.mtime ?? Lloc?.mtime ?? null;
            this.#ledger.setBase(key, { sha256: L, size: Lloc?.size ?? Lmeta.size ?? null, mtime, remoteSeq: seq });
            debug(`pushed ${key} (${Lloc?.size ?? '?'} bytes, seq ${seq})`);
            return;
        }
        if (res.reason === 'precondition-failed') { await this.#redecide(key, res.current?.sha256 ?? null); return; }
        if (res.reason === 'not-found' || res.reason === 'no-source') { this.#markDirty(key); return; }
        throw this.#failure(res, key);
    }

    // Re-run the table with a confirmed R (after a HEAD or a 412). A pull needs
    // the remote index fresh, so the feed is polled first.
    async #redecide(key, R) {
        if (R) await this.#freshRemote(key, R);
        else if (this.#stored.index.get(`${this.#opts.remote}:${key}`)) await this.#freshRemote(key, '∅');
        const { L, B } = this.#local3(key);
        const d = decide(L, B, R);
        debug(`redecide ${key}: L=${L?.slice(0, 8) ?? '∅'} B=${B?.slice(0, 8) ?? '∅'} R=${R?.slice(0, 8) ?? '∅'} → ${d.action}`);
        switch (d.action) {
            case 'nothing': return;
            case 'adopt': this.#reconcileKey(key); return;
            case 'push': this.#reconcileKey(key); return;
            case 'delete-remote': this.#reconcileKey(key); return;
            case 'pull': this.#enqueue('pull', key, { sha256: R }); return;
            case 'trash-local': this.#enqueue('trash-local', key, { sha256: L }); return;
            case 'conflict': this.#enqueue('conflict', key, { local: L, base: B, remote: R }); return;
            default: return;
        }
    }

    async #runPull(job) {
        const key = job.key;
        const want = job.payload?.sha256 ?? null;
        const Rmeta = await this.#freshRemote(key, want);
        const R = Rmeta?.checksums?.sha256 ?? null;
        if (!R) { this.#markDirty(key); return; }
        const { L, B, Lloc } = this.#local3(key);
        if (L === R) { this.#reconcileKey(key); return; }     // adopt
        // Local changed since the decision → the table decides again
        // (conflict). A local delete still pulls: edit beats delete.
        if (L != null && L !== B) { this.#reconcileKey(key); return; }
        const Rloc = Rmeta.locations.find((x) => x.backend === this.#opts.remote && x.key === key) || null;
        const res = await this.#stored.copy(`${this.#opts.remote}:${key}`, {
            to: this.#opts.local,
            key,
            from: this.#opts.remote,
            onConflict: 'overwrite',
            mtime: Rloc?.mtime ?? undefined,
            origin: this.deviceId || undefined,
        });
        if (res.ok) {
            this.#ledger.setBase(key, { sha256: R, size: Rloc?.size ?? Rmeta.size ?? null, mtime: Rloc?.mtime ?? null, remoteSeq: this.#remote.head ?? 0 });
            debug(`pulled ${key} (${Rloc?.size ?? '?'} bytes)${Lloc ? ' over local' : ''}`);
            return;
        }
        if (res.reason === 'not-found' || res.reason === 'no-source' || res.reason === 'source-unreadable') {
            await this.#freshRemote(key, '∅');
            this.#markDirty(key);
            return;
        }
        throw this.#failure(res, key);
    }

    async #runDeleteRemote(job) {
        const key = job.key;
        const { L, B } = this.#local3(key);
        if (!B) return;
        if (L) { this.#markDirty(key); return; }               // it came back
        let res;
        try {
            res = await this.#stored.removeObject(this.#opts.remote, key, { ifMatch: B, origin: this.deviceId || undefined });
        } catch (err) {
            if (err.code === 'PRECONDITION_FAILED') { await this.#redecide(key, err.current?.sha256 ?? null); return; }
            if (err.code === 'NOT_FOUND') { this.#ledger.removeBase(key); return; }
            throw err;
        }
        if (res.ok || res.reason === 'not-found') {
            this.#ledger.removeBase(key);
            debug(`deleted ${key} on the hub`);
            return;
        }
        if (res.reason === 'precondition-failed') { await this.#redecide(key, res.current?.sha256 ?? null); return; }
        throw this.#failure(res, key);
    }

    async #runTrashLocal(job) {
        const key = job.key;
        const { L, B, R } = this.#local3(key);
        if (!L) { this.#ledger.removeBase(key); return; }
        if (L !== B || R) { this.#markDirty(key); return; }
        const res = await this.#stored.move(`${this.#opts.local}:${key}`, {
            to: this.#opts.trash,
            key,
            from: this.#opts.local,
            onConflict: 'rename',
            origin: this.deviceId || undefined,
        });
        if (res.ok) {
            this.#ledger.removeBase(key);
            debug(`${key}: deleted on the hub; local copy moved to ${this.#opts.trash}`);
            return;
        }
        if (res.reason === 'not-found' || res.reason === 'no-source') { this.#ledger.removeBase(key); return; }
        throw this.#failure(res, key);
    }

    async #runRenameRemote(job) {
        const { from, to } = job.payload;
        const base = this.#ledger.getBase(to);
        if (!base) { this.#markDirty(to); return; }
        let res;
        try {
            res = await this.#stored.renameObject(this.#opts.remote, from, to, { ifMatch: base.sha256, origin: this.deviceId || undefined });
        } catch (err) {
            res = { ok: false, reason: err.code === 'PRECONDITION_FAILED' ? 'precondition-failed' : (err.code === 'NOT_FOUND' ? 'not-found' : (err.code === 'TARGET_EXISTS' ? 'target-exists' : 'transfer-failed')), error: err.message, code: err.code, current: err.current };
        }
        if (res.ok) {
            this.#ledger.setBase(to, { ...base, remoteSeq: res.seq ?? base.remoteSeq });
            debug(`renamed ${from} → ${to} on the hub`);
            return;
        }
        if (res.reason === 'not-found') {
            // Source gone on the hub (index stale or really gone): our bytes at
            // `to` are a new file there.
            const meta = await this.#freshRemote(from, base.sha256);
            if (meta) { this.#queue.append({ kind: 'rename-remote', mirror: this.#opts.id, key: to, payload: { from, to }, dedupe: `rename-remote|${from}→${to}` }); return; }
            this.#ledger.removeBase(to);
            this.#markDirty(to);
            return;
        }
        if (res.reason === 'target-exists' || res.reason === 'precondition-failed') {
            await this.#freshRemote(to, '∅');
            this.#ledger.removeBase(to);
            this.#markDirty(from);
            this.#markDirty(to);
            return;
        }
        throw this.#failure(res, to);
    }

    async #runRenameLocal(job) {
        const { from, to } = job.payload;
        const src = this.#local3(from);
        if (!src.L || src.L !== src.B || this.#stored.index.get(`${this.#opts.local}:${to}`)) {
            this.#markDirty(from); this.#markDirty(to); return;
        }
        const res = await this.#stored.move(`${this.#opts.local}:${from}`, {
            to: this.#opts.local,
            key: to,
            from: this.#opts.local,
            onConflict: 'error',
            origin: this.deviceId || undefined,
        });
        if (res.ok) {
            this.#ledger.moveBase(from, to, { remoteSeq: this.#remote.head ?? 0 });
            debug(`renamed ${from} → ${to} locally (hub rename)`);
            return;
        }
        if (['not-found', 'no-source', 'target-exists'].includes(res.reason)) { this.#markDirty(from); this.#markDirty(to); return; }
        throw this.#failure(res, to);
    }

    async #runConflict(job) {
        const key = job.key;
        const { L, B, Lloc, Lmeta } = this.#local3(key);
        if (!L || !Lmeta) { this.#markDirty(key); return; }
        const stat = await this.#remote.stat(key);
        const R = stat?.checksums?.sha256 ?? null;
        if (!R) { this.#ledger.removeBase(key); this.#markDirty(key); return; }   // hub deleted it after all → push as new
        if (R === L) { this.#reconcileKey(key); return; }                          // adopt
        if (B && R === B) { this.#reconcileKey(key); return; }                     // plain push again
        const now = new Date();
        const copyKey = conflictKey(key, this.deviceName, now);
        const mode = this.#opts.conflictMode === 'rename' ? 'rename' : 'inbox';

        // (a) keep the device version next to the folder.
        const kept = await this.#stored.copy(`${this.#opts.local}:${key}`, {
            to: this.#opts.conflicts, key: copyKey, from: this.#opts.local, onConflict: 'rename', mtime: Lloc?.mtime ?? undefined,
        });
        if (!kept.ok && kept.reason !== 'unchanged') throw this.#failure(kept, copyKey);

        // (b) upload it: inbox document, or an ordinary object at the conflict key.
        let upload;
        if (mode === 'inbox') {
            const stream = await this.#stored.getStreamByUrl(`stored://${this.#opts.local}/${key}`);
            if (!stream) { this.#markDirty(key); return; }
            upload = await this.#remote.putStream(key, stream, {
                conflictOf: key, conflictMode: 'inbox', baseSha256: B || undefined, sha256: L,
                mtime: Lloc?.mtime ?? undefined, origin: this.deviceId || undefined, deviceName: this.deviceName,
                mimeType: Lmeta.mimeType || undefined, size: Lloc?.size ?? Lmeta.size ?? null,
            });
        } else {
            const res = await this.#stored.copy(`${this.#opts.local}:${key}`, {
                to: this.#opts.remote, key: copyKey, from: this.#opts.local, mtime: Lloc?.mtime ?? undefined,
                origin: this.deviceId || undefined, ifNoneMatch: '*', onConflict: 'error',
                conflictOf: key, conflictMode: 'rename', baseSha256: B || undefined, deviceName: this.deviceName,
            });
            if (!res.ok && res.reason !== 'target-exists') throw this.#failure(res, copyKey);
            upload = res.conflict || res.remote || { key: copyKey };
            // No base for the copy key: it has no local file yet, so the next
            // pass pulls it like any other hub object (the copy shows up on
            // every device, Dropbox-style).
        }

        // (c) the hub version keeps the name.
        const Rmeta = await this.#freshRemote(key, R);
        if (!Rmeta) { this.#markDirty(key); return; }
        const Rloc = Rmeta.locations.find((x) => x.backend === this.#opts.remote && x.key === key) || null;
        const pulled = await this.#stored.copy(`${this.#opts.remote}:${key}`, {
            to: this.#opts.local, key, from: this.#opts.remote, onConflict: 'overwrite', mtime: Rloc?.mtime ?? undefined, origin: this.deviceId || undefined,
        });
        if (!pulled.ok) throw this.#failure(pulled, key);
        this.#ledger.setBase(key, { sha256: R, size: Rloc?.size ?? null, mtime: Rloc?.mtime ?? null, remoteSeq: this.#remote.head ?? 0 });
        this.#conflicts += 1;
        this.#ledger.setState({ conflicts: this.#conflicts });
        const event = { key, mode, local: L, base: B, remote: R, copy: kept.added?.[0] ?? `stored://${this.#opts.conflicts}/${copyKey}`, conflictKey: mode === 'rename' ? copyKey : null, upload, at: now.getTime() };
        debug(`conflict ${key}: kept ${copyKey}, uploaded (${mode}), hub version in place`);
        this.emit('conflict', event);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Offline / state
    // ─────────────────────────────────────────────────────────────────────────

    #isOffline(err) {
        return err?.code === 'OFFLINE' || err?.reason === 'target-offline' || err?.reason === 'source-offline' || err?.offline === true;
    }

    #goOffline(err) {
        this.#lastError = err?.message || 'offline';
        if (this.#state === 'offline' || this.#stopping) return;
        this.#setState('offline');
        this.emit('offline', { error: this.#lastError });
        this.#scheduleOnline();
    }

    #scheduleOnline() {
        if (this.#offlineTimer || this.#stopping) return;
        const delays = this.#opts.offlineBackoffMs;
        const delay = delays[Math.min(this.#offlineAttempts, delays.length - 1)];
        this.#offlineAttempts += 1;
        this.#offlineTimer = setTimeout(() => { this.#offlineTimer = null; this.#tryOnline().catch(() => {}); }, delay);
        this.#offlineTimer.unref?.();
    }

    async #tryOnline() {
        if (this.#stopping || this.#state !== 'offline') return;
        const live = await this.#remote.verifyRoot();
        if (!live.ok) { this.#scheduleOnline(); return; }
        this.#offlineAttempts = 0;
        this.#setState('online');
        this.emit('online', {});
        try {
            await this.#catchUp();
            await this.#reconcileDirty();
        } catch (err) {
            if (this.#isOffline(err)) { this.#goOffline(err); return; }
            this.#report(err);
        }
        this.#kick();
    }

    #setState(state) {
        if (this.#state === state) return;
        this.#state = state;
        this.emit('status', this.status());
    }

    #report(err) {
        this.#lastError = err?.message || String(err);
        debug(`error: ${this.#lastError}`);
        if (this.listenerCount('error')) this.emit('error', err);
    }
}
