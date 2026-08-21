import { Worker } from 'worker_threads';
import { EventEmitter } from 'events';
import cacache from 'cacache';
import Debug from 'debug';

const debug = Debug('stored:sync');

/**
 * Background sync queue for remote backend writes.
 *
 * Every job is `{ id, cacheRoot, cacheKey, targets[] }` — the bytes already
 * sit in the cache under `cacheKey`; each target gets a copy. Two lanes:
 *   - `driver: 'file'` targets are streamed by a lazily-spawned worker thread
 *     (disk-to-disk copy is the one thing worth taking off the event loop);
 *   - every other driver is committed IN-PROCESS through its live backend
 *     (`backend.commit(key, cachePath)`): network uploads are async I/O, and
 *     the driver's credentials, token and path cache live on the main thread.
 * One `synced` event per job carries the merged results of both lanes.
 */
export default class SyncQueue extends EventEmitter {
    #worker = null;
    #seq = 0;
    #pending = new Map(); // seq → resolve(results) for in-flight worker jobs
    #resolveBackend;

    /** @param {{ resolveBackend?: (name:string) => object|undefined }} [options] */
    constructor({ resolveBackend = null } = {}) {
        super();
        this.#resolveBackend = typeof resolveBackend === 'function' ? resolveBackend : () => undefined;
    }

    enqueue(job) {
        const targets = Array.isArray(job?.targets) ? job.targets : [];
        const viaWorker = targets.filter((t) => t.driver === 'file');
        const inProcess = targets.filter((t) => t.driver !== 'file');
        const lanes = [];
        if (viaWorker.length) lanes.push(this.#viaWorker({ ...job, targets: viaWorker }));
        if (inProcess.length) lanes.push(this.#inProcess(job, inProcess));
        if (!lanes.length) return;

        Promise.all(lanes)
            .then((parts) => {
                const results = parts.flat();
                debug(`Sync complete: ${String(job.id).slice(0, 19)}... → ${results.map((r) => `${r.backend}:${r.success}`).join(', ')}`);
                this.emit('synced', { id: job.id, results });
            })
            .catch((err) => this.emit('error', err));
    }

    async stop() {
        if (this.#worker) {
            await this.#worker.terminate();
            this.#worker = null;
        }
        // Jobs the worker never answered must not hang their `synced` forever.
        for (const [seq, resolve] of this.#pending) {
            this.#pending.delete(seq);
            resolve([]);
        }
    }

    #viaWorker(job) {
        if (!this.#worker) this.#spawnWorker();
        const seq = ++this.#seq;
        return new Promise((resolve) => {
            this.#pending.set(seq, resolve);
            this.#worker.postMessage({ seq, ...job });
        });
    }

    async #inProcess({ cacheRoot, cacheKey }, targets) {
        const info = await cacache.get.info(cacheRoot, cacheKey).catch(() => null);
        if (!info) {
            return targets.map((t) => ({ backend: t.name, key: t.key, success: false, error: 'Cache read failed: entry not found' }));
        }
        const results = [];
        for (const target of targets) {
            const backend = this.#resolveBackend(target.name);
            if (!backend || typeof backend.commit !== 'function') {
                results.push({ backend: target.name, key: target.key, success: false, error: `Backend not registered: ${target.name}` });
                continue;
            }
            try {
                await backend.commit(target.key, info.path);
                results.push({ backend: target.name, key: target.key, success: true });
            } catch (err) {
                debug(`In-process sync to ${target.name}:${target.key} failed: ${err.message}`);
                results.push({ backend: target.name, key: target.key, success: false, error: err.message });
            }
        }
        return results;
    }

    #spawnWorker() {
        this.#worker = new Worker(new URL('./worker.js', import.meta.url));
        this.#worker.on('message', (msg) => {
            const resolve = this.#pending.get(msg.seq);
            if (resolve) {
                this.#pending.delete(msg.seq);
                resolve(msg.results || []);
                return;
            }
            // No correlation (foreign/legacy message) — surface as-is.
            this.emit('synced', msg);
        });
        this.#worker.on('error', (err) => {
            debug(`Worker error: ${err.message}`);
            this.emit('error', err);
        });
    }
}
