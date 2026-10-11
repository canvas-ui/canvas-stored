import { EventEmitter } from 'node:events';
import { createHash, randomUUID } from 'node:crypto';
import { pipeline } from 'node:stream/promises';
import { Readable, Transform } from 'node:stream';
import { openSync, closeSync, createReadStream } from 'node:fs';
import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { backoffMs } from './JobQueue.js';

const clone = value => structuredClone(value);
const canonical = value => Array.isArray(value) ? value.map(canonical)
    : value && typeof value === 'object' ? Object.fromEntries(Object.keys(value).sort().map(k => [k, canonical(value[k])])) : value;
const digest = value => createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
const address = value => JSON.stringify([value.backend, value.key]);
const terminal = state => state === 'complete' || state === 'cancelled';
const failure = (code, message, retryable = false) => Object.assign(new Error(message), { code, retryable });
const transient = error => error.retryable || ['ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'ENETUNREACH', 'EAI_AGAIN', 'target-offline'].includes(error.code)
    || [429, 502, 503, 504].includes(error.status ?? error.statusCode);
const safeKey = key => typeof key === 'string' && key.length > 0 && !key.includes('\0')
    && !key.includes('\\') && !key.startsWith('/') && !/^[A-Za-z]:/.test(key)
    && key.split('/').every(part => part && part !== '.' && part !== '..');

/**
 * Physical replica execution. No documents, virtual trees, or rule evaluation.
 * A caller supplies the complete ordered destination set for one opaque owner.
 * The journal, owner ledger and receipts share Stored's LMDB environment.
 * Accepted plans are immutable; retries reuse their resolved keys and outcomes.
 */
export default class ReplicaService extends EventEmitter {
    #stored; #ops; #owners; #meta; #running = null; #timer = null; #stopped = false; #requested = false;
    #lease = { pid: process.pid, token: randomUUID() };
    #lockFd = null;
    #ingress;
    #captureAll;
    constructor(stored, { retryIntervalMs = 1000, autoStart = true, captureAllWrites = false } = {}) {
        super();
        this.#stored = stored;
        this.#captureAll = captureAllWrites;
        this.#ops = stored.index.openDB('replica.operations.v1');
        this.#owners = stored.index.openDB('replica.owners.v1');
        this.#meta = stored.index.openDB('replica.meta.v1');
        this.#ingress = stored.index.openDB('replica.ingress.v1');
        if (process.platform === 'linux') {
            // flock is attached to the shared open-file description. The
            // helper acquires it on fd 3; our descriptor keeps it alive after
            // the helper exits. Kernel release on death survives PID reuse
            // and container namespaces without stale-lock deletion races.
            const fd = openSync(path.join(stored.paths.index, '.executor.lock'), 'a', 0o600);
            const result = spawnSync('flock', ['--nonblock', '3'], { stdio: ['ignore', 'ignore', 'pipe', fd] });
            if (result.status !== 0) {
                closeSync(fd);
                throw failure(result.status === 1 ? 'EXECUTOR_ACTIVE' : 'EXECUTOR_LOCK_UNAVAILABLE',
                    result.status === 1 ? 'Stored already has an active executor' : 'Stored requires util-linux flock for its executor lock');
            }
            this.#lockFd = fd;
        }
        stored.index.transaction(() => {
            const held = this.#meta.get('executor');
            if (held && this.#lockFd == null) {
                let alive = true;
                try { process.kill(held.pid, 0); } catch (error) { if (error.code === 'ESRCH') alive = false; }
                if (alive) throw failure('EXECUTOR_ACTIVE', `Stored already has an active executor (PID ${held.pid})`);
            }
            this.#meta.putSync('executor', this.#lease);
        });
        for (const { key, value } of this.#ops.getRange()) {
            if (value.state === 'running') this.#ops.putSync(key, { ...value, state: 'pending' });
        }
        if (autoStart) {
            this.#timer = setInterval(() => { this.drain().catch(error => this.emit('fault', error)); }, retryIntervalMs);
            this.#timer.unref?.();
        }
    }

    capabilities(backendName) {
        const backend = this.#stored.getBackend(backendName);
        const driver = backend?.config?.driver;
        return {
            available: Boolean(backend),
            create: Boolean(backend?.canWrite && (driver === 'cacache' || typeof backend.createFrom === 'function')),
            // Destructive automatic operations need recovery AND conditional
            // execution. Remote drivers must explicitly implement that contract.
            retire: Boolean(driver === 'file' && backend.canDelete && this.#stored.retention),
            directories: Boolean(driver === 'file' && backend?.canWrite),
            overwrite: Boolean(driver === 'file' && backend.canWrite && this.#stored.retention),
        };
    }

    validate(input) {
        if (!input || typeof input !== 'object') throw failure('INVALID_PLAN', 'A replica plan is required');
        for (const key of ['id', 'owner']) {
            if (typeof input[key] !== 'string' || !input[key] || input[key].length > 512) throw failure('INVALID_PLAN', `${key} is required`);
        }
        if (!Array.isArray(input.replicas)) throw failure('INVALID_PLAN', 'replicas must be an array');
        const plan = clone(input);
        const replicas = new Map();
        for (const r of plan.replicas) {
            if (!r.backend || !safeKey(r.key)) throw failure('INVALID_KEY', 'Invalid replica destination');
            const cap = this.capabilities(r.backend);
            if (!cap.available) throw failure('BACKEND_UNAVAILABLE', `Backend ${r.backend} is unavailable`, true);
            if (r.kind === 'directory' ? !cap.directories : !cap.create) throw failure('UNSUPPORTED_BACKEND', `Backend ${r.backend} cannot safely place this replica`);
            if (r.onConflict === 'overwrite' && !cap.overwrite) throw failure('UNSUPPORTED_OVERWRITE', `Backend ${r.backend} cannot safely overwrite replicas`);
            if (r.mirror !== false && r.kind !== 'directory' && !cap.retire) throw failure('UNSUPPORTED_RETIRE', `Backend ${r.backend} supports copies but not recoverable automatic removal`);
            if (r.onConflict && !['error', 'rename', 'overwrite'].includes(r.onConflict)) throw failure('INVALID_PLAN', 'Invalid conflict behavior');
            const prior = replicas.get(address(r));
            if (prior && (prior.kind !== r.kind || prior.onConflict !== r.onConflict || prior.mirror !== r.mirror)) throw failure('CONFLICTING_TARGETS', 'Two targets request incompatible behavior at the same key');
            replicas.set(address(r), r);
        }
        plan.replicas = [...replicas.values()];
        for (const endpoint of plan.preserve || []) if (!endpoint.backend || !safeKey(endpoint.key)) throw failure('INVALID_KEY', 'Invalid preserved endpoint');
        if (plan.source && (!plan.source.backend || !safeKey(plan.source.key))) throw failure('INVALID_KEY', 'Invalid source');
        if (plan.replicas.some(r => r.kind !== 'directory') && !/^sha256:[a-f0-9]{64}$/.test(plan.contentId || '')) throw failure('INVALID_PLAN', 'File replicas require a SHA-256 content identity');
        for (const r of plan.release || []) {
            if (!r.backend || !safeKey(r.key) || this.#stored.getBackend(r.backend)?.config.driver !== 'cacache') throw failure('INVALID_PLAN', 'Only managed blob staging can be released');
            if (r.contentId && !/^sha256:[a-f0-9]{64}$/.test(r.contentId)) throw failure('INVALID_PLAN', 'Invalid staging content identity');
        }
        return plan;
    }

    submit(input) {
        if (this.#stopped) throw failure('STOPPED', 'Storage is stopping');
        // Validate identical retries against the receipt before consulting live
        // backend config: an offline backend cannot erase an accepted result.
        const prior = this.#ops.get(input?.id);
        if (prior) {
            if (prior.requestHash !== digest(input)) throw failure('IDEMPOTENCY_CONFLICT', 'Operation ID was reused with different input');
            return clone(prior);
        }
        const plan = this.validate(input);
        const op = this.#stored.index.transaction(() => {
            const seq = Number(this.#meta.get('head') || 0) + 1;
            const entry = { id: plan.id, owner: plan.owner, seq, requestHash: digest(input), plan,
                state: 'pending', phase: 'prepare', attempts: 0, nextAt: 0,
                createdAt: Date.now(), updatedAt: Date.now(), outcomes: [] };
            this.#ops.putSync(entry.id, entry);
            this.#meta.putSync('head', seq);
            return entry;
        });
        this.emit('changed', clone(op));
        this.#requested = true;
        queueMicrotask(() => this.#stored.detachedStorageWork(() => { if (!this.#stopped) this.drain().catch(error => this.emit('fault', error)); }));
        return clone(op);
    }

    get(id) { const op = this.#ops.get(id); return op ? clone(op) : null; }
    owner(id) { const owner = this.#owners.get(id); return owner ? clone(owner) : null; }
    async captureIngress(backend, key, contentId, file) {
        const owners = this.owners().filter(o => o.replicas.some(r => r.mirror !== false && r.kind !== 'directory' && r.backend === backend && r.key === key));
        if (!owners.length && !this.#captureAll) return null;
        if (owners.length > 1) throw failure('SHARED_REPLICA', 'An edit cannot choose between multiple logical owners of this file');
        const entry = this.#stored.index.transaction(() => {
            const seq = Number(this.#meta.get('ingress-head') || 0) + 1;
            const value = { cacheKey: `replica-ingress:${randomUUID()}`, owner: owners[0]?.owner || null, contentId,
                backend, key, seq, state: 'preparing', createdAt: Date.now() };
            this.#meta.putSync('ingress-head', seq); this.#ingress.putSync(value.cacheKey, value);
            return value;
        });
        try { await pipeline(createReadStream(file), this.#stored.cache.putStream(entry.cacheKey, { owner: entry.owner, contentId })); }
        catch (error) { await this.discardIngress(entry); throw error; }
        return entry;
    }
    commitIngress(entry) { if (entry) this.#ingress.putSync(entry.cacheKey, { ...entry, state: 'committed' }); }
    async discardIngress(entry) {
        if (!entry || [...this.#ops.getRange()].some(({ value }) => !terminal(value.state) && value.plan.sourceCacheKey === entry.cacheKey)) return;
        await this.#stored.cache.delete(entry.cacheKey);
        this.#ingress.removeSync(entry.cacheKey);
    }
    get hasIngress() { return this.#ingress.getKeysCount() > 0; }
    assertIdleEndpoint(backend, key, { directory = false, except = null } = {}) {
        const matches = r => r.backend === backend && (r.key === key || (directory && r.key.startsWith(`${key}/`)));
        for (const { value } of this.#ops.getRange()) {
            if (terminal(value.state) || value.id === except) continue;
            if ([...value.plan.replicas, ...value.outcomes, ...(value.base?.replicas || []), ...(value.plan.source ? [value.plan.source] : [])].some(matches)) {
                throw failure('OPERATION_PENDING', `Finish storage operation ${value.id} before changing this path`);
            }
        }
    }
    moveIngress(backend, from, toBackend, to, { directory = false } = {}) {
        for (const { key, value } of this.#ingress.getRange()) {
            if (value.backend === backend && (value.key === from || (directory && value.key.startsWith(`${from}/`)))) {
                this.#ingress.putSync(key, { ...value, backend: toBackend, key: to + value.key.slice(from.length) });
            }
        }
    }
    async abandonUnownedIngress(backend, key, { directory = false } = {}) {
        for (const { value } of this.#ingress.getRange()) {
            if (value.owner == null && value.backend === backend && (value.key === key || (directory && value.key.startsWith(`${key}/`)))) await this.discardIngress(value);
        }
    }
    sourceFor(owner, contentId, endpoints = []) {
        return [...this.#ingress.getRange()].map(r => r.value).filter(r => r.contentId === contentId
            && (r.owner === owner || (r.owner == null && endpoints.some(e => address(e) === address(r))))).sort((a, b) => b.seq - a.seq)[0] || null;
    }
    async finishUnmirroredIngress(entry) {
        return this.#stored.withStorageMutation(async () => {
            if ((await this.#inspect(entry))?.id !== entry.contentId) throw failure('SOURCE_CHANGED', 'The primary write has not been verified');
            for (const { value } of this.#ingress.getRange()) {
                if (value.owner == null && address(value) === address(entry) && value.seq <= entry.seq) await this.discardIngress(value);
            }
        });
    }
    async supersede(id, input) {
        const plan = this.validate(input);
        return this.#stored.withStorageMutation(async () => {
            const previous = this.get(id);
            if (!previous || previous.owner !== plan.owner || previous.plan.contentId === plan.contentId) throw failure('INVALID_SUPERSESSION', 'Only a newer content version of the same owner may supersede unfinished work');
            if (previous.state === 'complete') return this.submit(plan);
            if (previous.state === 'cancelled') {
                if (previous.supersededBy !== plan.id) throw failure('INVALID_SUPERSESSION', 'Operation already has a successor');
                return this.submit(plan);
            }
            const base = this.owner(plan.owner) || previous.base || { owner: plan.owner, replicas: [] };
            // Preserve exact ownership of any partial effects. Never assume a
            // pending outcome landed, or claim bytes changed by another writer.
            const replicas = new Map(base.replicas.map(r => [address(r), r]));
            for (const outcome of previous.outcomes) {
                const actual = await this.#inspect(outcome);
                if (actual && (outcome.kind === 'directory' || actual.id === outcome.contentId)) replicas.set(address(outcome), { ...outcome, state: 'verified' });
            }
            let accepted;
            this.#stored.index.transaction(() => {
                this.#owners.putSync(plan.owner, { ...base, replicas: [...replicas.values()] });
                this.#save(previous, { state: 'cancelled', supersededBy: plan.id });
                accepted = this.submit(plan);
            });
            return accepted;
        });
    }
    async adopt(ownerId, endpoint) {
        return this.#stored.withStorageMutation(async () => {
            const owner = this.owner(ownerId);
            const replica = owner?.replicas.find(r => address(r) === address(endpoint));
            if (!replica || (await this.#inspect(replica))?.id !== replica.contentId) throw failure('TARGET_CHANGED', 'Cannot adopt an unknown or changed replica');
            replica.managed = true;
            this.#owners.putSync(ownerId, owner);
        });
    }
    async audit(id) {
        const owner = this.owner(id);
        const differences = [];
        for (const replica of owner?.replicas || []) {
            // Copy-only targets no longer in the current plan are historical
            // exports, not a continuing promise to overwrite external edits.
            if (replica.mirror === false) continue;
            const current = await this.#inspect(replica);
            if (!current || (replica.kind !== 'directory' && current.id !== replica.contentId)) {
                differences.push({ backend: replica.backend, key: replica.key, reason: current ? 'changed' : 'missing' });
            }
        }
        return differences;
    }
    owners(prefix = '') { return [...this.#owners.getRange()].map(r => clone(r.value)).filter(r => r.owner.startsWith(prefix)); }
    async recordRelocation(backend, from, to, { directory = false } = {}) {
        return this.#stored.withStorageMutation(async () => {
            const affected = r => r.backend === backend && (r.key === from || (directory && r.key.startsWith(`${from}/`)));
            const updates = [];
            for (const owner of this.owners()) {
                if (!owner.replicas.some(affected)) continue;
                if (this.list({ owner: owner.owner, active: true, limit: 1 }).operations.length) throw failure('OPERATION_PENDING', 'Finish the current replica operation before relocating its ownership');
                for (const replica of owner.replicas.filter(affected)) {
                    const key = to + replica.key.slice(from.length);
                    const current = await this.#inspect({ ...replica, key });
                    if (!current || (replica.kind !== 'directory' && current.id !== replica.contentId)) throw failure('TARGET_CHANGED', 'Relocation cannot be verified against the recorded byte version');
                    replica.key = key; replica.requestedKey = key;
                }
                updates.push(owner);
            }
            this.#stored.index.transaction(() => { for (const owner of updates) this.#owners.putSync(owner.owner, owner); });
            this.moveIngress(backend, from, backend, to, { directory });
        });
    }
    list({ owner, limit = 100, after = 0, active = false } = {}) {
        const all = [...this.#ops.getRange()].map(r => r.value).filter(r => (!owner || r.owner === owner) && r.seq > after && (!active || !terminal(r.state))).sort((a, b) => a.seq - b.seq);
        const operations = all.slice(0, Math.min(1000, Math.max(1, limit))).map(clone);
        return { operations, next: all.length > operations.length ? operations.at(-1).seq : null, head: this.#meta.get('head') || 0 };
    }
    retry(id) {
        const op = this.get(id);
        if (!op) throw failure('NOT_FOUND', 'Operation not found');
        if (!terminal(op.state)) this.#save(op, { state: 'pending', nextAt: 0, error: null });
        this.#requested = true;
        return this.drain().then(() => this.get(id));
    }
    async stop() {
        this.#stopped = true;
        clearInterval(this.#timer);
        await this.#running;
    }
    releaseExecutor() {
        this.#stored.index.transaction(() => {
            if (this.#meta.get('executor')?.token === this.#lease.token) this.#meta.removeSync('executor');
        });
        if (this.#lockFd != null) { closeSync(this.#lockFd); this.#lockFd = null; }
    }
    async drain() {
        if (this.#stopped) return;
        if (this.#running) return this.#running;
        this.#running = (async () => {
            do { this.#requested = false; await this.#drain(); } while (this.#requested && !this.#stopped);
        })();
        try { await this.#running; } finally { this.#running = null; }
    }
    async #drain() {
        if (this.#stopped) return;
        // A crash before a physical write may leave an unacknowledged ingress
        // pin. Retain uncertainty while the backend is unavailable; otherwise
        // reclaim only an old preparation that did not become the live file.
        await this.#stored.withStorageMutation(async () => {
            for (const { value } of this.#ingress.getRange()) {
                if (value.state !== 'preparing' || Date.now() - value.createdAt < 3600_000) continue;
                try {
                    if ((await this.#inspect(value))?.id === value.contentId) this.commitIngress(value);
                    else await this.discardIngress(value);
                } catch { /* An unavailable or changing source is not proof of absence. */ }
            }
        });
        if (!this.#stopped) await this.#stored.resumePendingMoves();
        const blockedOwners = new Set();
        const rows = [...this.#ops.getRange()].map(r => r.value).sort((a, b) => a.seq - b.seq);
        for (const row of rows) {
            if (this.#stopped) break;
            const op = this.get(row.id);
            if (terminal(op.state)) { await this.#cleanup(op); continue; }
            if (blockedOwners.has(op.owner)) continue;
            if (op.state === 'blocked' || op.nextAt > Date.now()) { blockedOwners.add(op.owner); continue; }
            try {
                this.#save(op, { state: 'running', error: null });
                await this.#stored.withStorageMutation(() => this.#apply(op));
            } catch (error) {
                const attempts = op.attempts + 1;
                this.#save(op, { state: transient(error) ? 'pending' : 'blocked', attempts,
                    nextAt: transient(error) ? Date.now() + backoffMs(attempts) : 0,
                    error: { code: error.code || 'IO_ERROR', message: error.message } });
                blockedOwners.add(op.owner);
            }
        }
        if (!this.#stopped) await this.#stored.resumePendingMoves();
    }
    #save(op, patch) {
        Object.assign(op, patch, { updatedAt: Date.now() });
        this.#ops.putSync(op.id, op);
        this.emit('changed', clone(op));
    }
    async #cleanup(op) {
        if (op.state === 'complete' && op.plan.ingressSeq) {
            const source = op.plan.ingressSource;
            for (const { value } of this.#ingress.getRange()) {
                if ((value.owner === op.owner || (value.owner == null && source && address(value) === address(source))) && value.seq <= op.plan.ingressSeq) await this.discardIngress(value);
            }
        }
        if (op.cleaned) return;
        let successor = op;
        const visited = new Set();
        while (successor?.state === 'cancelled' && successor.supersededBy && !visited.has(successor.id)) {
            visited.add(successor.id); successor = this.get(successor.supersededBy);
        }
        if (successor?.state !== 'complete') return;
        if (op.stageKey) await this.#stored.cache.delete(op.stageKey);
        if (op.plan.sourceCacheKey) {
            if ([...this.#ops.getRange()].some(({ value }) => !terminal(value.state) && value.plan.sourceCacheKey === op.plan.sourceCacheKey)) return;
            await this.#stored.cache.delete(op.plan.sourceCacheKey);
        }
        this.#ops.putSync(op.id, { ...op, cleaned: true });
    }
    #backend(name) {
        const backend = this.#stored.getBackend(name);
        if (!backend) throw failure('BACKEND_UNAVAILABLE', `Backend ${name} is unavailable`, true);
        return backend;
    }
    async #inspect(endpoint) {
        const backend = this.#backend(endpoint.backend);
        if (typeof backend.verifyRoot === 'function' && !(await backend.verifyRoot()).ok) throw failure('BACKEND_UNAVAILABLE', `Backend ${endpoint.backend} is offline`, true);
        const stat = endpoint.kind === 'directory' ? await backend.statContainer(endpoint.key) : await backend.stat(endpoint.key);
        if (!stat) return null;
        if (endpoint.kind === 'directory') return { kind: 'directory' };
        const input = await backend.get(endpoint.key, { stream: true });
        if (!input) throw failure('SOURCE_CHANGED', `Cannot read ${endpoint.backend}:${endpoint.key}`, true);
        const hash = createHash('sha256');
        for await (const chunk of Buffer.isBuffer(input) ? Readable.from([input]) : input) hash.update(chunk);
        const after = await backend.stat(endpoint.key);
        if (!after || ['size', 'modified', 'ino', 'dev', 'id'].some(k => stat[k] !== after[k])) throw failure('SOURCE_CHANGED', `File changed while reading ${endpoint.key}`, true);
        return { id: `sha256:${hash.digest('hex')}`, stat: after };
    }
    async #stage(op) {
        const stageKey = `replica-operation:${op.id}`;
        let info = await this.#stored.cache.getInfo(stageKey);
        if (info) {
            const hash = createHash('sha256');
            for await (const chunk of this.#stored.cache.getStream(stageKey)) hash.update(chunk);
            if (`sha256:${hash.digest('hex')}` !== (op.plan.contentId || op.base?.contentId)) throw failure('STAGING_CORRUPT', 'Recovery content failed verification');
            this.#save(op, { stageKey });
            return info;
        }
        const source = op.plan.source || op.base?.replicas?.find(r => r.kind !== 'directory');
        if (op.plan.sourceCacheKey) {
            const hash = createHash('sha256');
            const tap = new Transform({ transform(chunk, encoding, done) { hash.update(chunk); done(null, chunk); } });
            await pipeline(this.#stored.cache.getStream(op.plan.sourceCacheKey), tap, this.#stored.cache.putStream(stageKey, { operation: op.id }));
            if (`sha256:${hash.digest('hex')}` !== op.plan.contentId) {
                await this.#stored.cache.delete(stageKey);
                throw failure('STAGING_CORRUPT', 'Transfer source failed checksum verification');
            }
            this.#save(op, { stageKey, phase: 'place' });
            return this.#stored.cache.getInfo(stageKey);
        }
        if (!source) throw failure('NO_SOURCE', 'No recoverable byte source was supplied');
        const backend = this.#backend(source.backend);
        if (typeof backend.verifyRoot === 'function' && !(await backend.verifyRoot()).ok) throw failure('BACKEND_UNAVAILABLE', `Backend ${source.backend} is offline`, true);
        const before = await backend.stat(source.key);
        if (!before) throw failure('SOURCE_MISSING', `Source ${source.backend}:${source.key} is missing`);
        const input = await backend.get(source.key, { stream: true });
        if (!input) throw failure('SOURCE_MISSING', 'Source is unreadable');
        const hash = createHash('sha256');
        const tap = new Transform({ transform(chunk, encoding, done) { hash.update(chunk); done(null, chunk); } });
        await pipeline(Buffer.isBuffer(input) ? Readable.from([input]) : input, tap, this.#stored.cache.putStream(stageKey, { operation: op.id }));
        const actual = `sha256:${hash.digest('hex')}`;
        const expected = op.plan.contentId || op.base?.contentId;
        const after = await backend.stat(source.key);
        if (actual !== expected || !after || ['size', 'modified', 'ino', 'dev', 'id'].some(k => before[k] !== after[k])) {
            await this.#stored.cache.delete(stageKey);
            throw failure('SOURCE_CHANGED', 'Source no longer contains the accepted content version');
        }
        info = await this.#stored.cache.getInfo(stageKey);
        this.#save(op, { stageKey, phase: 'place' });
        return info;
    }
    #otherClaims(op, endpoint) {
        const wanted = address(endpoint);
        for (const { value } of this.#owners.getRange()) {
            if (value.owner !== op.owner && value.replicas.some(r => address(r) === wanted)) return true;
        }
        for (const { value } of this.#ops.getRange()) {
            if (value.id !== op.id && !terminal(value.state)
                && [...value.plan.replicas, ...(value.plan.source ? [value.plan.source] : [])].some(r => address(r) === wanted)) return true;
        }
        return false;
    }
    async #apply(op) {
        if (!op.base) this.#save(op, { base: this.owner(op.owner) || { owner: op.owner, replicas: [] } });
        const files = op.plan.replicas.some(r => r.kind !== 'directory');
        // An immutable staging pin is retained for the entire operation, even
        // when workspace:data is released or the original is moved externally.
        const info = (files || op.base.replicas.some(r => r.kind !== 'directory')) ? await this.#stage(op) : null;
        for (let i = 0; i < op.plan.replicas.length; i++) {
            const target = op.plan.replicas[i];
            let outcome = op.outcomes[i];
            const backend = this.#backend(target.backend);
            if (!outcome) {
                const previous = op.base.replicas.find(r => r.backend === target.backend && (r.requestedKey || r.key) === target.key);
                const resolved = previous && target.onConflict === 'rename' ? { ...target, key: previous.key } : target;
                const current = await this.#inspect(resolved);
                if (previous?.managed && current?.id && current.id !== previous.contentId && current.id !== op.plan.contentId) {
                    throw failure('TARGET_CHANGED', 'A managed replica changed since its last verified version');
                }
                outcome = { ...resolved, requestedKey: target.key, contentId: op.plan.contentId || null, state: 'pending',
                    managed: Boolean(previous?.managed || !current || target.adopt === true
                        || this.owners().some(owner => owner.replicas.some(r => r.managed && address(r) === address(target)))),
                    expected: current?.id || null,
                    // An owned, verified old version is an update, not a name
                    // collision. Persist its version before any replacement.
                    replaces: previous?.managed && previous.mirror !== false && this.capabilities(target.backend).overwrite
                        ? previous.contentId : null };
                if (current?.id && current.id !== op.plan.contentId && current.id !== outcome.replaces && target.onConflict === 'rename') {
                    let n = 1;
                    const dot = target.key.lastIndexOf('.');
                    const split = dot > target.key.lastIndexOf('/') ? dot : target.key.length;
                    do { outcome.key = `${target.key.slice(0, split)}-${n++}${target.key.slice(split)}`; } while (await backend.stat(outcome.key));
                    outcome.expected = null;
                    outcome.managed = true;
                }
                op.outcomes[i] = outcome;
                this.#save(op, { outcomes: op.outcomes });
            }
            if (target.kind === 'directory') {
                await this.#stored.ensureDirectory(target.backend, outcome.key);
            } else {
                const found = await this.#inspect(outcome);
                if (found?.id !== op.plan.contentId) {
                    if (found && found.id !== outcome.replaces && target.onConflict !== 'overwrite') throw failure('TARGET_CONFLICT', `Destination ${outcome.backend}:${outcome.key} contains different bytes`);
                    if (found && this.#otherClaims(op, outcome) && found.id !== op.plan.contentId) throw failure('SHARED_REPLICA', 'Another owner or pending operation still needs this byte version');
                    if (found && found.id !== outcome.expected) throw failure('TARGET_CHANGED', 'Destination changed after the plan was accepted');
                    if (!found && outcome.expected && outcome.state !== 'pending') throw failure('TARGET_CHANGED', 'A previously completed destination disappeared');
                    const res = backend.config.driver === 'cacache'
                        ? await this.#stored.put(info.path, { backends: [outcome.backend], key: outcome.key, origin: `replica:${op.id}` })
                        : await this.#stored.writeObject(outcome.backend, outcome.key, info.path, {
                        sha256: op.plan.contentId.slice(7),
                        ...(found ? { ifMatch: found.id.slice(7) } : { ifNoneMatch: '*' }),
                        origin: `replica:${op.id}`,
                    });
                    if (!res?.ok) throw failure(res?.reason || 'WRITE_FAILED', `Replica write failed: ${res?.reason || 'unknown'}`);
                    const verified = await this.#inspect(outcome);
                    if (verified?.id !== op.plan.contentId) throw failure('VERIFY_FAILED', 'Destination verification failed');
                }
            }
            if (target.kind !== 'directory') await this.#stored.acknowledgeReplica(outcome.backend, outcome.key, op.plan.contentId, `replica:${op.id}`);
            outcome.state = 'verified';
            this.#save(op, { outcomes: op.outcomes });
        }
        this.#save(op, { phase: 'retire' });
        for (const old of op.base.replicas) {
            if (op.outcomes.some(r => address(r) === address(old)) || (op.plan.preserve || []).some(r => address(r) === address(old)) || old.mirror === false || !old.managed || this.#otherClaims(op, old)) continue;
            if (old.kind === 'directory') {
                // Only empty owned directories may be retired. Unknown children
                // are never swept into a policy's recursive deletion.
                await this.#stored.removeEmptyDirectory(old.backend, old.key);
                continue;
            }
            const found = await this.#inspect(old);
            if (!found) continue;
            if (found.id !== old.contentId) throw failure('TARGET_CHANGED', 'The replica to remove has been edited');
            if (!this.capabilities(old.backend).retire) throw failure('RECOVERY_UNAVAILABLE', 'Recoverable removal is unavailable');
            const res = await this.#stored.removeObject(old.backend, old.key, { ifMatch: old.contentId.slice(7), origin: `replica:${op.id}` });
            if (!res?.ok) throw failure(res?.reason || 'REMOVE_FAILED', 'Replica retirement failed');
        }
        // Verify ALL destinations again before releasing a single staging
        // location. Presence in the index, or a queued upload, is insufficient.
        const fileOutcomes = op.outcomes.filter(r => r.kind !== 'directory');
        for (const target of fileOutcomes) {
            if ((await this.#inspect(target))?.id !== op.plan.contentId) throw failure('VERIFY_FAILED', 'A destination changed before completion');
        }
        if (fileOutcomes.length) for (const loc of op.plan.release || []) {
            if (this.#otherClaims(op, loc)) continue;
            const current = await this.#stored.stat(`${loc.backend}:${loc.key}`);
            if (current && current.id !== (loc.contentId || op.plan.contentId)) throw failure('SOURCE_CHANGED', 'Staging identity changed');
            if (current) {
                const result = await this.#stored.deleteByUrl(`stored://${loc.backend}/${loc.key}`);
                if (!result.ok) throw failure('RELEASE_FAILED', 'Staging release failed', true);
            }
        }
        const preserved = op.base.replicas.filter(r => (r.mirror === false || (op.plan.preserve || []).some(p => address(p) === address(r)))
            && !op.outcomes.some(n => address(n) === address(r)) && !(op.plan.release || []).some(n => address(n) === address(r))).map(r => ({ ...r, mirror: false }));
        const owner = { owner: op.owner, operationId: op.id, contentId: op.plan.contentId || op.base.contentId,
            replicas: [...preserved, ...op.outcomes], updatedAt: Date.now() };
        this.#stored.index.transaction(() => {
            this.#owners.putSync(op.owner, owner);
            Object.assign(op, { state: 'complete', phase: 'complete', updatedAt: Date.now() });
            this.#ops.putSync(op.id, op);
        });
        this.emit('changed', clone(op));
        await this.#cleanup(op);
    }
}
