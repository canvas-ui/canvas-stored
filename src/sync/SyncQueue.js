import { EventEmitter } from 'node:events';
import { randomUUID, createHash } from 'node:crypto';

/** Compatibility adapter: ordinary remote transfers use the same durable
 * executor and verification contract as explicit replica plans. */
export default class SyncQueue extends EventEmitter {
    #service; #listener;
    constructor({ service }) {
        super();
        this.#service = service;
        this.#listener = operation => {
            if (!operation.plan.transportJob) return;
            if (!['complete', 'blocked'].includes(operation.state)) return;
            this.emit('synced', { id: operation.plan.contentId, operationId: operation.id,
                results: operation.plan.replicas.map((target, i) => ({
                    backend: target.backend, key: operation.outcomes[i]?.key || target.key,
                    success: operation.state === 'complete',
                    ...(operation.error ? { error: operation.error.message } : {}),
                })) });
        };
        service.on('changed', this.#listener);
    }
    enqueue(job) {
        return this.#service.submit({
            id: `transfer:${randomUUID()}`, owner: `transfer:${createHash('sha256').update(JSON.stringify([job.id, job.targets])).digest('hex')}`,
            contentId: job.id, sourceCacheKey: job.cacheKey, transportJob: true,
            replicas: job.targets.map(t => ({ backend: t.name, key: t.key, mirror: false, onConflict: 'error' })),
        });
    }
    async stop() { this.#service.off('changed', this.#listener); }
}
