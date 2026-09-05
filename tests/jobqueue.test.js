import { test, describe, before, after } from 'node:test';
import assert from 'node:assert';
import fs from 'fs-extra';
import os from 'os';
import path from 'path';
import JobQueue, { backoffMs } from '../src/sync/JobQueue.js';
import Index from '../src/index/index.js';

describe('JobQueue', () => {
    const base = path.join(os.tmpdir(), `stored-jobqueue-${process.pid}`);
    let index;
    let queue;

    before(async () => {
        await fs.remove(base);
        await fs.ensureDir(base);
        index = new Index(path.join(base, 'index'));
        queue = new JobQueue({ index });
    });

    after(async () => {
        index.close();
        await fs.remove(base);
    });

    test('append assigns increasing seqs and pending() yields in order', () => {
        const a = queue.append({ kind: 'push', mirror: 'm1', key: 'a.txt', payload: { n: 1 } });
        const b = queue.append({ kind: 'pull', mirror: 'm1', key: 'b.txt' });
        assert.ok(b.seq > a.seq);
        assert.deepStrictEqual([...queue.pending()].map((j) => j.key), ['a.txt', 'b.txt']);
        assert.deepStrictEqual(queue.counters('m1'), { pending: 2, running: 0, failed: 0, total: 2, nextAt: null });
    });

    test('a pending job with the same dedupe key is replaced (payload wins, seq kept)', () => {
        const before = queue.get(1);
        const again = queue.append({ kind: 'push', mirror: 'm1', key: 'a.txt', payload: { n: 2 } });
        assert.strictEqual(again.seq, before.seq);
        assert.deepStrictEqual(queue.get(1).payload, { n: 2 });
        assert.strictEqual(queue.counters('m1').pending, 2);
    });

    test('take → complete removes the job; take on a non-pending job is null', () => {
        const taken = queue.take(2);
        assert.strictEqual(taken.state, 'running');
        assert.strictEqual(queue.take(2), null, 'already running');
        assert.deepStrictEqual([...queue.pending()].map((j) => j.seq), [1]);
        assert.strictEqual(queue.complete(2), true);
        assert.strictEqual(queue.get(2), null);
        assert.strictEqual(queue.counters('m1').total, 1);
    });

    test('a running job is not replaced by append — the new one queues behind it', () => {
        queue.take(1);
        const next = queue.append({ kind: 'push', mirror: 'm1', key: 'a.txt', payload: { n: 3 } });
        assert.notStrictEqual(next.seq, 1);
        assert.strictEqual(queue.get(1).state, 'running');
        queue.complete(1);
        queue.complete(next.seq);
    });

    test('fail() backs off min(60s·2^n, 1h) and returns the job to pending', () => {
        const job = queue.append({ kind: 'push', mirror: 'm1', key: 'c.txt' });
        queue.take(job.seq);
        const t0 = Date.now();
        const failed = queue.fail(job.seq, new Error('boom'));
        assert.strictEqual(failed.state, 'pending');
        assert.strictEqual(failed.attempts, 1);
        assert.ok(failed.nextAt >= t0 + 60_000 - 5 && failed.nextAt <= t0 + 60_000 + 1000);
        assert.strictEqual(failed.lastError.message, 'boom');
        assert.deepStrictEqual([...queue.pending(Date.now())].map((j) => j.seq), [], 'not due yet');
        assert.deepStrictEqual([...queue.pending(Date.now() + 61_000)].map((j) => j.seq), [job.seq], 'due after the backoff');

        assert.strictEqual(backoffMs(1), 60_000);
        assert.strictEqual(backoffMs(2), 120_000);
        assert.strictEqual(backoffMs(6), 1_920_000);
        assert.strictEqual(backoffMs(7), 3_600_000);
        assert.strictEqual(backoffMs(20), 3_600_000);
        queue.cancel(job.seq);
    });

    test('permanent failure parks the job as failed; retry() revives it', () => {
        const job = queue.append({ kind: 'push', mirror: 'm1', key: 'd.txt' });
        queue.take(job.seq);
        const failed = queue.fail(job.seq, Object.assign(new Error('refused'), { code: 'REFUSED' }), { permanent: true });
        assert.strictEqual(failed.state, 'failed');
        assert.strictEqual(queue.counters('m1').failed, 1);
        assert.deepStrictEqual([...queue.pending(Date.now() + 1e9)].map((j) => j.seq), []);
        queue.retry(job.seq);
        assert.strictEqual(queue.get(job.seq).state, 'pending');
        assert.strictEqual(queue.get(job.seq).nextAt, 0);
        queue.cancel(job.seq);
    });

    test('byKey finds jobs by key and by payload.from/to; cancelByKey drops them', () => {
        queue.append({ kind: 'rename-remote', mirror: 'm1', key: 'new.txt', payload: { from: 'old.txt', to: 'new.txt' } });
        queue.append({ kind: 'push', mirror: 'm1', key: 'old.txt' });
        assert.strictEqual(queue.byKey('old.txt', 'm1').length, 2);
        assert.strictEqual(queue.byKey('new.txt', 'm1').length, 1);
        assert.strictEqual(queue.cancelByKey('old.txt', 'm1'), 2);
        assert.strictEqual(queue.byKey('old.txt', 'm1').length, 0);
    });

    test('mirrors are isolated in pending()/counters()', () => {
        queue.append({ kind: 'push', mirror: 'm2', key: 'x' });
        assert.strictEqual(queue.counters('m1').pending, 0);
        assert.strictEqual(queue.counters('m2').pending, 1);
        assert.strictEqual([...queue.pending(Date.now(), 'm1')].length, 0);
        queue.clear('m2');
        assert.strictEqual(queue.counters('m2').total, 0);
    });

    test('recover() on reopen turns running jobs back into pending, seq continues', () => {
        const job = queue.append({ kind: 'pull', mirror: 'm1', key: 'r.txt' });
        queue.take(job.seq);
        assert.strictEqual(queue.get(job.seq).state, 'running');
        const head = queue.head;
        index.close();

        index = new Index(path.join(base, 'index'));
        queue = new JobQueue({ index });
        const recovered = queue.get(job.seq);
        assert.strictEqual(recovered.state, 'pending');
        assert.strictEqual(queue.head, head);
        const next = queue.append({ kind: 'push', mirror: 'm1', key: 's.txt' });
        assert.strictEqual(next.seq, head + 1);
        // Dedupe survived the reopen too.
        const dup = queue.append({ kind: 'pull', mirror: 'm1', key: 'r.txt', payload: { again: true } });
        assert.strictEqual(dup.seq, job.seq);
    });

    test('standalone env via { path }', async () => {
        const q = new JobQueue({ path: path.join(base, 'own') });
        q.append({ kind: 'push', key: 'k' });
        assert.strictEqual(q.counters().pending, 1);
        q.close();
    });
});
