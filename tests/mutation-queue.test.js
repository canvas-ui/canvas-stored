import { test } from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import MutationQueue from '../src/runtime/MutationQueue.js';

test('a pending mutation reports its holder and queue without releasing the lock', async () => {
    const reports = [], calls = [];
    const queue = new MutationQueue({ stallMs: 5, onStall: state => reports.push(state) });
    let release;
    const gate = new Promise(resolve => { release = resolve; });
    const first = queue.run(async () => { calls.push('first'); await gate; return queue.run(() => calls.push('nested')); }, 'copy');
    const second = queue.run(() => calls.push('second'), 'remove');
    try {
        await delay(25);
        assert.deepEqual(calls, ['first']);
        assert.ok(reports.some(r => r.active.operation === 'copy' && r.queued === 1));
    } finally { release(); await Promise.all([first, second]); }
    assert.deepEqual(calls, ['first', 'nested', 'second']);
    assert.deepEqual(queue.diagnostics, { queued: 0, active: null });
    queue.close();
    await assert.rejects(queue.run(() => {}), { code: 'STOPPED' });
});

test('a queued burst gives sockets a turn and a rejection does not poison later mutations', async () => {
    const queue = new MutationQueue();
    const calls = []; let heartbeat = false;
    const marker = new Promise(resolve => setImmediate(() => { heartbeat = true; resolve(); }));
    const tasks = Array.from({ length: 100 }, (_, i) => queue.run(() => {
        calls.push(i);
        if (i === 80) assert.equal(heartbeat, true);
        if (i === 20) throw new Error('expected');
    }));
    const results = await Promise.allSettled(tasks);
    await marker;
    assert.equal(results.filter(r => r.status === 'rejected').length, 1);
    assert.deepEqual(calls, Array.from({ length: 100 }, (_, i) => i));
});

test('detached observers join the queue instead of borrowing a live mutation scope', async () => {
    const queue = new MutationQueue(); const calls = []; let observer;
    await queue.run(async () => {
        calls.push('start');
        observer = queue.detach(() => queue.run(() => calls.push('observer')));
        await Promise.resolve(); calls.push('finish');
    });
    await observer;
    assert.deepEqual(calls, ['start', 'finish', 'observer']);
});
