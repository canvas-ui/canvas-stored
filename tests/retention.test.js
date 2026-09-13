import { test, describe, before, after } from 'node:test';
import assert from 'node:assert';
import fs from 'fs-extra';
import path from 'path';
import crypto from 'crypto';
import Stored from '../src/index.js';

const ROOT = path.resolve('./.test-retention');
const A = path.join(ROOT, 'a');
const B = path.join(ROOT, 'b');
const sha = (s) => crypto.createHash('sha256').update(s).digest('hex');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Displaced-blob retention (docs/durable-workspaces.md step 3): bytes an
 * overwrite/delete replaces on a local file backend stay recoverable for a
 * window, addressed by digest; placements are atomic (rename over).
 */
describe('retention window + atomic placement', async () => {
    let stored;

    before(async () => {
        await fs.remove(ROOT);
        await fs.ensureDir(A); await fs.ensureDir(B);
        stored = new Stored({ root: path.join(ROOT, '.stored'), retention: { days: 30, sweepEveryMs: 60_000 } });
        stored.on('error', () => {});
        stored.addBackend('fs:a', { driver: 'file', root: A });
        stored.addBackend('fs:b', { driver: 'file', root: B });
    });
    after(async () => { await stored.stop(); await fs.remove(ROOT); });

    test('overwrite via writeObject retains the displaced bytes under their sha256', async () => {
        const v1 = await stored.writeObject('fs:a', 'doc/x.txt', Buffer.from('version one'), {});
        assert.strictEqual(v1.ok, true);
        assert.deepStrictEqual(stored.listRetained(), []);
        const v2 = await stored.writeObject('fs:a', 'doc/x.txt', Buffer.from('version two'), { ifMatch: v1.sha256 });
        assert.strictEqual(v2.ok, true);
        assert.strictEqual(await fs.readFile(path.join(A, 'doc/x.txt'), 'utf8'), 'version two');

        const kept = stored.listRetained();
        assert.strictEqual(kept.length, 1);
        assert.strictEqual(kept[0].sha256, sha('version one'));
        assert.deepStrictEqual(kept[0].keys, ['fs:a:doc/x.txt']);
        assert.strictEqual(kept[0].size, 'version one'.length);
        const onDisk = path.join(A, '.stored-tmp', 'retained', sha('version one'));
        assert.strictEqual(await fs.readFile(onDisk, 'utf8'), 'version one');
        // The retained file is its own inode now: editing the visible file in place must not touch it.
        const [a, b] = await Promise.all([fs.stat(onDisk), fs.stat(path.join(A, 'doc/x.txt'))]);
        assert.notStrictEqual(a.ino, b.ino);
    });

    test('removeObject retains; the same bytes displaced twice are kept once with both keys', async () => {
        await stored.writeObject('fs:a', 'doc/y.txt', Buffer.from('version one'), {});
        const r = await stored.removeObject('fs:a', 'doc/y.txt', {});
        assert.strictEqual(r.ok, true);
        const entry = stored.getRetained(sha('version one'));
        assert.deepStrictEqual(entry.keys, ['fs:a:doc/x.txt', 'fs:a:doc/y.txt']);
        assert.strictEqual(stored.listRetained().length, 1);
        assert.strictEqual(stored.listRetained({ key: 'doc/y.txt' }).length, 1);
        assert.strictEqual(stored.listRetained({ key: 'doc/nope.txt' }).length, 0);
    });

    test('an onConflict:overwrite transfer retains what it replaced on the target', async () => {
        await stored.writeObject('fs:b', 'target.txt', Buffer.from('on b before'), {});
        const src = await stored.writeObject('fs:a', 'src.txt', Buffer.from('from a'), {});
        const res = await stored.copy(`fs:a:src.txt`, { to: 'fs:b', key: 'target.txt', from: 'fs:a', onConflict: 'overwrite' });
        assert.strictEqual(res.ok, true, JSON.stringify(res));
        assert.strictEqual(await fs.readFile(path.join(B, 'target.txt'), 'utf8'), 'from a');
        const entry = stored.getRetained(sha('on b before'));
        assert.ok(entry, 'displaced target bytes retained');
        assert.strictEqual(entry.backend, 'fs:b');
        void src;
    });

    test('restoreRetained puts the bytes back through the keyed write (refuses an occupied key without ifMatch)', async () => {
        const occupied = await stored.restoreRetained(sha('version one'), { backend: 'fs:a', key: 'doc/x.txt', ifNoneMatch: '*' });
        assert.strictEqual(occupied.ok, false);
        assert.strictEqual(occupied.reason, 'precondition-failed');

        const fresh = await stored.restoreRetained(sha('version one'), { backend: 'fs:a', key: 'doc/restored.txt', ifNoneMatch: '*', origin: 'ui' });
        assert.strictEqual(fresh.ok, true, JSON.stringify(fresh));
        assert.strictEqual(fresh.sha256, sha('version one'));
        assert.strictEqual(await fs.readFile(path.join(A, 'doc/restored.txt'), 'utf8'), 'version one');

        // Replace the current version explicitly: the current bytes get retained in turn.
        const back = await stored.restoreRetained(sha('version one'), { backend: 'fs:a', key: 'doc/x.txt', ifMatch: sha('version two') });
        assert.strictEqual(back.ok, true, JSON.stringify(back));
        assert.strictEqual(await fs.readFile(path.join(A, 'doc/x.txt'), 'utf8'), 'version one');
        assert.ok(stored.getRetained(sha('version two')), 'the replaced current version is now retained');
    });

    test('sweepRetained drops entries older than the window and their bytes; forgetRetained drops one', async () => {
        assert.deepStrictEqual(await stored.sweepRetained(), { swept: 0, kept: stored.listRetained().length });
        await sleep(20);
        const r = await stored.sweepRetained({ olderThanMs: 10 });
        assert.ok(r.swept >= 2);
        assert.strictEqual(stored.listRetained().length, 0);
        assert.strictEqual(await fs.pathExists(path.join(A, '.stored-tmp', 'retained', sha('version one'))), false);

        await stored.writeObject('fs:a', 'doc/z.txt', Buffer.from('z1'), {});
        await stored.writeObject('fs:a', 'doc/z.txt', Buffer.from('z2'), { ifMatch: sha('z1') });
        assert.strictEqual(await stored.forgetRetained(sha('z1')), true);
        assert.strictEqual(await stored.forgetRetained(sha('z1')), false);
    });

    test('retention is off by default: nothing is kept', async () => {
        const off = new Stored({ root: path.join(ROOT, '.stored-off') });
        off.on('error', () => {});
        const dir = path.join(ROOT, 'c'); await fs.ensureDir(dir);
        off.addBackend('fs:c', { driver: 'file', root: dir });
        await off.writeObject('fs:c', 'f.txt', Buffer.from('one'), {});
        await off.writeObject('fs:c', 'f.txt', Buffer.from('two'), { ifMatch: sha('one') });
        assert.deepStrictEqual(off.listRetained(), []);
        assert.strictEqual(off.retention, null);
        assert.strictEqual(await fs.pathExists(path.join(dir, '.stored-tmp', 'retained')), false);
        await off.stop();
    });

    test('put() and commit() leave no partial file and no staging leftovers', async () => {
        const backend = stored.getBackend('fs:a');
        await backend.put('atomic/p.txt', Buffer.from('put bytes'));
        assert.strictEqual(await fs.readFile(path.join(A, 'atomic/p.txt'), 'utf8'), 'put bytes');
        const staged = path.join(ROOT, 'staged.bin');
        await fs.writeFile(staged, 'committed bytes');
        await backend.commit('atomic/p.txt', staged);
        assert.strictEqual(await fs.readFile(path.join(A, 'atomic/p.txt'), 'utf8'), 'committed bytes');
        assert.strictEqual(await fs.pathExists(staged), true, 'commit() does not consume its source');
        const leftovers = (await fs.readdir(path.join(A, '.stored-tmp'))).filter((f) => f !== 'retained');
        assert.deepStrictEqual(leftovers, []);
    });
});
