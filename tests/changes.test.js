import { test, describe, before, after } from 'node:test';
import assert from 'node:assert';
import fs from 'fs-extra';
import path from 'path';
import Stored from '../src/index.js';

const ROOT = path.resolve('./.test-changes');
const A = path.join(ROOT, 'a');
const B = path.join(ROOT, 'b');
const STORED_ROOT = path.join(ROOT, '.stored');

// The change log is the feed a remote mirror tails: one entry per location
// mutation, in commit order, with a durable sequence number.
describe('change log', async () => {
    let stored;
    const seen = [];

    before(async () => {
        await fs.remove(ROOT);
        await fs.ensureDir(A);
        await fs.ensureDir(B);
        stored = new Stored({ root: STORED_ROOT });
        stored.addBackend('fs:a', { driver: 'file', root: A });
        stored.addBackend('fs:b', { driver: 'file', root: B });
        stored.on('change', (entry) => seen.push(entry));
    });

    after(async () => {
        await stored.stop();
        await fs.remove(ROOT);
    });

    test('starts empty', () => {
        assert.strictEqual(stored.head(), 0);
        assert.strictEqual(stored.oldest(), null);
        const page = stored.changes();
        assert.deepStrictEqual(page.changes, []);
        assert.strictEqual(page.cursorTooOld, false);
    });

    test('put logs one entry per landed location, with origin', async () => {
        const meta = await stored.put(Buffer.from('one'), { key: 'docs/one.txt', backends: ['fs:a'], origin: 'dev-1' });
        assert.strictEqual(stored.head(), 1);
        const { changes, head, cursor } = stored.changes();
        assert.strictEqual(changes.length, 1);
        assert.deepStrictEqual(
            { op: changes[0].op, backend: changes[0].backend, key: changes[0].key, id: changes[0].id, origin: changes[0].origin, size: changes[0].size },
            { op: 'put', backend: 'fs:a', key: 'docs/one.txt', id: meta.id, origin: 'dev-1', size: 3 },
        );
        assert.ok(changes[0].mtime > 0, 'mtime recorded from the on-disk stat');
        assert.strictEqual(changes[0].seq, 1);
        assert.strictEqual(head, 1);
        assert.strictEqual(cursor, 1);
        // Emitted as an event too, after commit.
        assert.strictEqual(seen.length, 1);
        assert.strictEqual(seen[0].seq, 1);
    });

    test('since/backend paging', async () => {
        await stored.put(Buffer.from('two'), { key: 'two.txt', backends: ['fs:b'] });
        assert.strictEqual(stored.head(), 2);
        const later = stored.changes({ since: 1 });
        assert.strictEqual(later.changes.length, 1);
        assert.strictEqual(later.changes[0].key, 'two.txt');
        assert.strictEqual(later.changes[0].origin, undefined, 'no origin when none was given');
        const onlyA = stored.changes({ backend: 'fs:a' });
        assert.deepStrictEqual(onlyA.changes.map(c => c.key), ['docs/one.txt']);
        assert.strictEqual(onlyA.cursor, 2, 'cursor advances past filtered-out entries');
        const caughtUp = stored.changes({ since: 2 });
        assert.deepStrictEqual(caughtUp.changes, []);
        assert.strictEqual(caughtUp.cursor, 2);
    });

    test('a same-backend move is logged as a single rename', async () => {
        const meta = stored.index.get('fs:a:docs/one.txt');
        const result = await stored.move(meta.id, { to: 'fs:a', key: 'docs/renamed.txt', origin: 'dev-1' });
        assert.strictEqual(result.ok, true);
        const { changes } = stored.changes({ since: 2 });
        assert.strictEqual(changes.length, 1, 'one entry, not delete + put');
        assert.strictEqual(changes[0].op, 'rename');
        assert.strictEqual(changes[0].key, 'docs/renamed.txt');
        assert.strictEqual(changes[0].from, 'docs/one.txt');
        assert.strictEqual(changes[0].origin, 'dev-1');
        assert.strictEqual(stored.index.get('fs:a:docs/one.txt'), null);
    });

    test('a cross-backend copy then delete log put + delete', async () => {
        const meta = stored.index.get('fs:a:docs/renamed.txt');
        await stored.copy(meta.id, { to: 'fs:b' });
        let { changes } = stored.changes({ since: 3 });
        assert.deepStrictEqual(changes.map(c => [c.op, c.backend, c.key]), [['put', 'fs:b', 'docs/renamed.txt']]);
        await stored.delete(meta.id, { backends: ['fs:b'] });
        ({ changes } = stored.changes({ since: 4 }));
        assert.deepStrictEqual(changes.map(c => [c.op, c.backend, c.key]), [['delete', 'fs:b', 'docs/renamed.txt']]);
    });

    test('deleting the last location logs a delete for it', async () => {
        const meta = stored.index.get('fs:a:docs/renamed.txt');
        await stored.delete(meta.id);
        const { changes } = stored.changes({ since: 5 });
        assert.deepStrictEqual(changes.map(c => [c.op, c.backend, c.key, c.id]), [['delete', 'fs:a', 'docs/renamed.txt', meta.id]]);
        assert.strictEqual(stored.index.get(meta.id), null);
    });

    test('trim keeps the newest entries and flags stale cursors', () => {
        const head = stored.head();
        const removed = stored.trimChanges({ keep: 2 });
        assert.strictEqual(removed, head - 2);
        assert.strictEqual(stored.oldest(), head - 1);
        const stale = stored.changes({ since: 0 });
        assert.strictEqual(stale.cursorTooOld, true, 'a reader that never saw the trimmed entries must rebuild');
        assert.deepStrictEqual(stale.changes, []);
        const fresh = stored.changes({ since: head - 2 });
        assert.strictEqual(fresh.cursorTooOld, false);
        assert.strictEqual(fresh.changes.length, 2);
        const exact = stored.changes({ since: head });
        assert.strictEqual(exact.cursorTooOld, false, 'a caught-up reader is never stale');
    });

    test('survives reopen: head and entries persist', async () => {
        const head = stored.head();
        await stored.stop();
        stored = new Stored({ root: STORED_ROOT });
        stored.addBackend('fs:a', { driver: 'file', root: A });
        stored.addBackend('fs:b', { driver: 'file', root: B });
        assert.strictEqual(stored.head(), head);
        assert.strictEqual(stored.changes({ since: head - 2 }).changes.length, 2);
    });

    test('listObjects pages a backend in key order with checksums', async () => {
        await stored.put(Buffer.from('l1'), { key: 'list/a.txt', backends: ['fs:a'] });
        await stored.put(Buffer.from('l2'), { key: 'list/b.txt', backends: ['fs:a'] });
        await stored.put(Buffer.from('l3'), { key: 'list/sub/c.txt', backends: ['fs:a'] });
        await stored.put(Buffer.from('other'), { key: 'zzz.txt', backends: ['fs:a'] });
        await stored.put(Buffer.from('elsewhere'), { key: 'list/a.txt', backends: ['fs:b'] });

        const page1 = stored.listObjects('fs:a', { prefix: 'list/', limit: 2 });
        assert.deepStrictEqual(page1.objects.map(o => o.key), ['list/a.txt', 'list/b.txt']);
        assert.ok(page1.objects[0].checksums.sha256, 'sha256 comes from the index');
        assert.ok(page1.objects[0].mtime > 0);
        assert.strictEqual(page1.cursor, 'list/b.txt');

        const page2 = stored.listObjects('fs:a', { prefix: 'list/', limit: 2, after: page1.cursor });
        assert.deepStrictEqual(page2.objects.map(o => o.key), ['list/sub/c.txt']);
        assert.strictEqual(page2.cursor, null, 'null cursor = done');

        const all = stored.listObjects('fs:a');
        assert.deepStrictEqual(all.objects.map(o => o.key), ['list/a.txt', 'list/b.txt', 'list/sub/c.txt', 'zzz.txt']);
        assert.deepStrictEqual(stored.listObjects('fs:b').objects.map(o => o.key), ['list/a.txt', 'two.txt']);
        assert.strictEqual(stored.listObjects('nope').reason, 'unknown-backend');
    });
});
