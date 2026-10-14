import { AsyncLocalStorage } from 'node:async_hooks';
import { performance } from 'node:perf_hooks';
import { setImmediate as yieldToIO } from 'node:timers/promises';

/** One physical mutation executor. Nested operations join their owner's scope;
 * observers must detach before scheduling independent work. Diagnostics never
 * abandon a write or release its lock early.
 */
export default class MutationQueue {
    #context = new AsyncLocalStorage();
    #tail = Promise.resolve();
    #waiting = 0;
    #active = null;
    #closed = false;
    #timer = null;
    #report;
    #threshold;
    #completed = 0;
    constructor({ onStall = null, stallMs = 5000 } = {}) {
        this.#report = onStall;
        this.#threshold = Math.max(1, stallMs);
    }
    get diagnostics() {
        return { queued: this.#waiting, active: this.#active
            ? { operation: this.#active.operation, elapsedMs: Math.round(performance.now() - this.#active.started) } : null };
    }
    detach(action) { return this.#context.exit(action); }
    close() { this.#closed = true; }
    drain() { return this.#tail.catch(() => {}); }
    run(action, operation = 'mutation') {
        if (this.#context.getStore()?.active) return action();
        if (this.#closed) return Promise.reject(Object.assign(new Error('Stored is stopping'), { code: 'STOPPED' }));
        this.#waiting++;
        if (!this.#timer && this.#report) {
            this.#timer = setInterval(() => {
                const state = this.diagnostics;
                if (state.active?.elapsedMs >= this.#threshold) {
                    try { this.#report(state); } catch { /* diagnostic only */ }
                }
            }, this.#threshold);
            this.#timer.unref?.();
        }
        const next = this.#tail.catch(() => {}).then(async () => {
            // Make progress on HTTP, cancellation and watcher callbacks even
            // when thousands of queued mutations resolve from memory.
            if (++this.#completed % 32 === 0) await yieldToIO();
            this.#waiting--;
            this.#active = { operation, started: performance.now() };
            const scope = { active: true };
            try { return await this.#context.run(scope, action); }
            finally {
                scope.active = false;
                this.#active = null;
                if (!this.#waiting) { clearInterval(this.#timer); this.#timer = null; }
            }
        });
        this.#tail = next;
        return next;
    }
}
