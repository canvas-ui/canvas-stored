import { test, describe, before, after } from 'node:test';
import assert from 'node:assert';
import fs from 'fs-extra';
import path from 'path';
import Stored from '../src/index.js';

const ROOT = path.resolve('./.test-write-object');
const A = path.join(ROOT, 'a');
const W = path.join(ROOT, 'watched');
const STORED_ROOT = path.join(ROOT, '.stored');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function collect(emitter, name) {
    const events = [];
    emitter.on(name, (e) => events.push(e));
    return events;
}

// writeObject/removeObject/renameObject are the hub side of a device mirror:
// precondition-checked keyed writes that take the same succession path a
// watcher-observed edit takes, so documents keep their placements.
describe('keyed object writes', async () => {
    let stored;

    before(async () => {
        await fs.remove(ROOT);
        await fs.ensureDir(A);
        await fs.ensureDir(W);
        stored = new Stored({ root: STORED_ROOT });
        stored.addBackend('fs:a', { driver: 'file', root: A });
    });

    after(async () => {
        await stored.stop();
        await fs.remove(ROOT);
    });

    test('writeObject creates a file, indexes it with its inode, emits one object:add', async () => {
        const adds = collect(stored, 'object:add');
        const result = await stored.writeObject('fs:a', 'w/a.txt', Buffer.from('alpha'), { origin: 'laptop', mtime: 1700000000000 });
        assert.strictEqual(result.ok, true, JSON.stringify(result));
        assert.strictEqual(result.sha256.length, 64);
        assert.strictEqual(result.size, 5);
        assert.strictEqual(result.previous, null);
        assert.strictEqual(result.mtime, 1700000000000, 'mtime honoured');
        assert.strictEqual(await fs.readFile(path.join(A, 'w/a.txt'), 'utf8'), 'alpha');

        assert.strictEqual(adds.length, 1);
        assert.strictEqual(adds[0].key, 'w/a.txt');
        assert.strictEqual(adds[0].origin, 'laptop');
        const meta = stored.index.get('fs:a:w/a.txt');
        assert.strictEqual(meta.id, result.id);
        assert.ok(meta.locations[0].ino != null, 'inode recorded');
        assert.strictEqual(meta.locations[0].mtime, 1700000000000);

        const { changes } = stored.changes({ since: result.seq - 1 });
        assert.strictEqual(changes.length, 1);
        assert.deepStrictEqual([changes[0].op, changes[0].key, changes[0].origin], ['put', 'w/a.txt', 'laptop']);
        // The staging dir left nothing behind.
        assert.deepStrictEqual(await fs.readdir(path.join(A, '.stored-tmp')).catch(() => []), []);
    });

    test('rewriting identical bytes is a no-op (no event, no log entry)', async () => {
        const head = stored.head();
        const adds = collect(stored, 'object:add');
        const result = await stored.writeObject('fs:a', 'w/a.txt', 'alpha');
        assert.strictEqual(result.ok, true);
        assert.strictEqual(result.unchanged, true);
        assert.strictEqual(stored.head(), head);
        assert.strictEqual(adds.length, 0);
    });

    test('If-None-Match:* refuses an occupied key with the current state', async () => {
        const result = await stored.writeObject('fs:a', 'w/a.txt', 'beta', { ifNoneMatch: '*' });
        assert.strictEqual(result.ok, false);
        assert.strictEqual(result.reason, 'precondition-failed');
        assert.strictEqual(result.code, 'PRECONDITION_FAILED');
        assert.strictEqual(result.current.sha256, stored.index.get('fs:a:w/a.txt').checksums.sha256);
        assert.strictEqual(result.current.size, 5);
        assert.strictEqual(await fs.readFile(path.join(A, 'w/a.txt'), 'utf8'), 'alpha', 'bytes untouched');
    });

    test('If-Match with a stale sha refuses; with the right one it is a succession', async () => {
        const before = stored.index.get('fs:a:w/a.txt');
        const stale = await stored.writeObject('fs:a', 'w/a.txt', 'beta', { ifMatch: 'deadbeef' });
        assert.strictEqual(stale.ok, false);
        assert.strictEqual(stale.reason, 'precondition-failed');

        const unlinks = collect(stored, 'object:unlink');
        const adds = collect(stored, 'object:add');
        const result = await stored.writeObject('fs:a', 'w/a.txt', 'beta', { ifMatch: `"${before.checksums.sha256}"`, origin: 'laptop' });
        assert.strictEqual(result.ok, true, JSON.stringify(result));
        assert.deepStrictEqual(result.previous, { id: before.id, checksums: before.checksums });
        assert.strictEqual(await fs.readFile(path.join(A, 'w/a.txt'), 'utf8'), 'beta');

        // Succession vocabulary: unlink(old, successor) then add(new, previous).
        assert.strictEqual(unlinks.length, 1);
        assert.strictEqual(unlinks[0].id, before.id);
        assert.strictEqual(unlinks[0].successor.id, result.id);
        assert.strictEqual(adds.length, 1);
        assert.strictEqual(adds[0].previous.id, before.id);
        assert.strictEqual(stored.index.get(before.id), null, 'old entry gone (that was its only location)');
        assert.strictEqual(stored.index.get('fs:a:w/a.txt').id, result.id);

        // One log entry for the key (coalesced), not delete + put.
        const { changes } = stored.changes({ since: result.seq - 1 });
        assert.strictEqual(changes.length, 1);
        assert.strictEqual(changes[0].op, 'put');
        assert.strictEqual(changes[0].id, result.id);
    });

    test('a corrupted upload is refused by sha256', async () => {
        const result = await stored.writeObject('fs:a', 'w/b.txt', 'gamma', { sha256: 'ab'.repeat(32) });
        assert.strictEqual(result.ok, false);
        assert.strictEqual(result.reason, 'checksum-mismatch');
        assert.strictEqual(await fs.pathExists(path.join(A, 'w/b.txt')), false);
    });

    test('unsafe keys are refused, sloppy ones normalized', async () => {
        for (const key of ['../x', 'a/../../x', '', 'a/./b', 'x\0y']) {
            const result = await stored.writeObject('fs:a', key, 'x');
            assert.strictEqual(result.ok, false, `${JSON.stringify(key)} → ${JSON.stringify(result)}`);
            assert.strictEqual(result.reason, 'invalid-key');
        }
        for (const [key, normalized] of [['a//', 'a'], ['/abs', 'abs'], ['dir//x.txt/', 'dir/x.txt']]) {
            const result = await stored.writeObject('fs:a', key, 'x');
            assert.strictEqual(result.ok, true, `${JSON.stringify(key)} → ${JSON.stringify(result)}`);
            assert.ok(stored.index.get(`fs:a:${normalized}`), `${key} normalized to ${normalized}`);
        }
    });

    test('renameObject moves in place: same id, object:move, one rename entry', async () => {
        const before = stored.index.get('fs:a:w/a.txt');
        const moves = collect(stored, 'object:move');
        await stored.writeObject('fs:a', 'w/blocker.txt', 'blocker');
        const taken = await stored.renameObject('fs:a', 'w/a.txt', 'w/blocker.txt', { origin: 'laptop' });
        assert.strictEqual(taken.ok, false);
        assert.strictEqual(taken.reason, 'target-exists');

        const result = await stored.renameObject('fs:a', 'w/a.txt', 'w/moved.txt', { ifMatch: before.checksums.sha256, origin: 'laptop' });
        assert.strictEqual(result.ok, true, JSON.stringify(result));
        assert.strictEqual(result.id, before.id);
        assert.strictEqual(result.state, 'complete');
        assert.strictEqual(await fs.pathExists(path.join(A, 'w/moved.txt')), true);
        assert.strictEqual(await fs.pathExists(path.join(A, 'w/a.txt')), false);
        assert.strictEqual(moves.length, 1);
        assert.strictEqual(moves[0].from.key, 'w/a.txt');
        assert.strictEqual(moves[0].to.key, 'w/moved.txt');
        assert.strictEqual(moves[0].origin, 'laptop');
        const { changes } = stored.changes({ since: result.seq - 1 });
        assert.deepStrictEqual(changes.map(c => [c.op, c.key, c.from, c.origin]), [['rename', 'w/moved.txt', 'w/a.txt', 'laptop']]);
        assert.strictEqual(stored.index.get('fs:a:w/moved.txt').id, before.id);
    });

    test('removeObject honours If-Match and unlinks', async () => {
        const meta = stored.index.get('fs:a:w/moved.txt');
        const stale = await stored.removeObject('fs:a', 'w/moved.txt', { ifMatch: 'nope' });
        assert.strictEqual(stale.reason, 'precondition-failed');
        const unlinks = collect(stored, 'object:unlink');
        const result = await stored.removeObject('fs:a', 'w/moved.txt', { ifMatch: meta.checksums.sha256, origin: 'laptop' });
        assert.strictEqual(result.ok, true);
        assert.strictEqual(result.id, meta.id);
        assert.strictEqual(await fs.pathExists(path.join(A, 'w/moved.txt')), false);
        assert.strictEqual(unlinks.length, 1);
        assert.strictEqual(unlinks[0].key, 'w/moved.txt');
        assert.strictEqual(unlinks[0].origin, 'laptop');
        assert.strictEqual(stored.index.get(meta.id), null);
        const { changes } = stored.changes({ since: result.seq - 1 });
        assert.deepStrictEqual(changes.map(c => [c.op, c.key, c.origin]), [['delete', 'w/moved.txt', 'laptop']]);
        const gone = await stored.removeObject('fs:a', 'w/moved.txt');
        assert.strictEqual(gone.reason, 'not-found');
    });

    test('an overwrite copy displaces the previous owner of the key', async () => {
        const first = await stored.put(Buffer.from('first'), { key: 'ow/target.txt', backends: ['fs:a'] });
        const second = await stored.put(Buffer.from('second'), { key: 'ow/source.txt', backends: ['fs:a'] });

        const unlinks = collect(stored, 'object:unlink');
        const adds = collect(stored, 'object:location:add');
        const result = await stored.copy(second.id, { to: 'fs:a', key: 'ow/target.txt', onConflict: 'overwrite', origin: 'laptop' });
        assert.strictEqual(result.ok, true, JSON.stringify(result));
        assert.strictEqual(await fs.readFile(path.join(A, 'ow/target.txt'), 'utf8'), 'second');

        assert.strictEqual(stored.index.get(first.id), null, 'displaced entry dropped (that was its only location)');
        assert.strictEqual(stored.index.get('fs:a:ow/target.txt').id, second.id);
        assert.strictEqual(unlinks.length, 1);
        assert.strictEqual(unlinks[0].id, first.id);
        assert.strictEqual(unlinks[0].reason, 'overwritten');
        assert.strictEqual(unlinks[0].successor.id, second.id);
        assert.strictEqual(adds.length, 1);
        assert.strictEqual(adds[0].previous.id, first.id, 'the add carries the predecessor for placement migration');

        // Log: the key changed hands once (coalesced), no transient delete.
        const { changes } = stored.changes({ since: stored.head() - 1 });
        assert.deepStrictEqual(changes.map(c => [c.op, c.key, c.id]), [['put', 'ow/target.txt', second.id]]);
    });

    test('put() records mtime + inode on committed locations', async () => {
        const meta = await stored.put(Buffer.from('stat me'), { key: 'stat/me.txt', backends: ['fs:a'], mtime: 1600000000000 });
        const loc = stored.index.get(meta.id).locations[0];
        assert.ok(loc.ino != null, 'inode');
        assert.strictEqual(loc.mtime, 1600000000000);
        assert.strictEqual((await fs.stat(path.join(A, 'stat/me.txt'))).mtimeMs, 1600000000000);
    });

    test('writeObject on a watched backend yields exactly one add (echo suppressed)', async () => {
        const backend = stored.addBackend('fs:w', { driver: 'file', root: W, stabilityThreshold: 100 });
        await backend.watch();
        await sleep(300);
        const adds = collect(stored, 'object:add');
        const result = await stored.writeObject('fs:w', 'echo.txt', 'echo', { origin: 'laptop' });
        assert.strictEqual(result.ok, true);
        await sleep(1500);
        assert.strictEqual(adds.length, 1, 'watcher echo of our own write was suppressed');
        assert.strictEqual(adds[0].origin, 'laptop');

        // An external edit through the filesystem still flows as a succession.
        const unlinks = collect(stored, 'object:unlink');
        await fs.writeFile(path.join(W, 'echo.txt'), 'echo-edited');
        await sleep(1500);
        assert.strictEqual(adds.length, 2);
        assert.strictEqual(adds[1].previous.id, result.id);
        assert.strictEqual(unlinks.length, 1);
        await backend.stop();
    });
});
