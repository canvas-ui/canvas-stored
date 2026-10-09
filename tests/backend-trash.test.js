import test from 'node:test';
import assert from 'node:assert/strict';
import os from 'node:os';
import path from 'node:path';
import fs from 'fs-extra';
import crypto from 'node:crypto';
import Stored from '../src/index.js';

async function setup(t) {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'stored-trash-'));
    const stored = new Stored({ root: path.join(root, 'db'), retention: { days: 30 } });
    stored.on('error', () => {});
    for (const name of ['a', 'b']) stored.addBackend(name, { driver: 'file', root: path.join(root, name) });
    t.after(async () => { await stored.stop(); await fs.remove(root); });
    return { stored, root, put: (key, body, backend = 'a') => stored.writeObject(backend, key, Buffer.from(body)) };
}

test('overwrites are versions, deleted locations are independent restorable trash items', async t => {
    const { stored, put, root } = await setup(t);
    await put('live.txt', 'old'); await put('live.txt', 'current');
    assert.equal((await stored.listTrash('a')).items.length, 0);
    for (let i = 0; i < 25; i++) { await put(`Photos/${i}.txt`, 'same'); await stored.removeObject('a', `Photos/${i}.txt`); }
    const page = await stored.listTrash('a', { limit: 100 });
    assert.equal(page.items.length, 25);
    assert.equal(stored.getRetained(crypto.createHash('sha256').update('same').digest('hex')).keys.length, 25);
    assert.equal((await stored.restoreTrash('b', page.items[0].id)).reason, 'not-found', 'IDs are backend scoped');
    for (const item of page.items) {
        assert.equal((await stored.restoreTrash('a', item.id)).ok, true);
        assert.equal(await fs.readFile(path.join(root, 'a', item.key), 'utf8'), 'same');
    }
    assert.equal((await stored.listTrash('a')).items.length, 0);
    assert.equal(await fs.readFile(path.join(root, 'a/live.txt'), 'utf8'), 'current');
});

test('repeated deletions retain distinct versions; occupied destinations never get overwritten', async t => {
    const { stored, put, root } = await setup(t);
    await put('a/b.txt', 'one'); await stored.removeObject('a', 'a/b.txt');
    await put('a/b.txt', 'two'); await stored.removeObject('a', 'a/b.txt');
    const { items } = await stored.listTrash('a');
    assert.equal(items.length, 2); assert.notEqual(items[0].id, items[1].id);
    await put('a/b.txt', 'new work');
    for (const item of items) assert.equal((await stored.restoreTrash('a', item.id)).reason, 'precondition-failed');
    assert.equal(await fs.readFile(path.join(root, 'a/a/b.txt'), 'utf8'), 'new work');
    assert.equal((await stored.listTrash('a')).items.length, 2);
});

test('whole-folder trash and restore preserve indexed, hidden, unindexed and empty children', async t => {
    const { stored, put, root } = await setup(t);
    await put('Architecture/Kitchen/plan.txt', 'plan');
    await fs.outputFile(path.join(root, 'a/Architecture/Kitchen/.private'), 'private');
    await fs.outputFile(path.join(root, 'a/Architecture/Kitchen/unindexed'), 'unindexed');
    await fs.ensureDir(path.join(root, 'a/Architecture/Kitchen/Empty'));
    const inode = (await fs.stat(path.join(root, 'a/Architecture/Kitchen/plan.txt'))).ino;
    const removed = await stored.trashDirectory('a', 'Architecture/Kitchen');
    assert.equal(await fs.pathExists(path.join(root, 'a/Architecture/Kitchen')), false);
    assert.equal(await stored.has('a:Architecture/Kitchen/plan.txt'), false);
    const { items } = await stored.listTrash('a');
    assert.equal(items.length, 1); assert.equal(items[0].type, 'directory');
    await fs.remove(path.join(root, 'a/Architecture'));
    assert.equal((await stored.restoreTrash('a', removed.id)).ok, true);
    assert.equal((await fs.stat(path.join(root, 'a/Architecture/Kitchen/plan.txt'))).ino, inode);
    assert.equal(await fs.readFile(path.join(root, 'a/Architecture/Kitchen/.private'), 'utf8'), 'private');
    assert.equal(await fs.readFile(path.join(root, 'a/Architecture/Kitchen/unindexed'), 'utf8'), 'unindexed');
    assert.ok((await fs.stat(path.join(root, 'a/Architecture/Kitchen/Empty'))).isDirectory());
    assert.equal((await stored.listTrash('a')).items.length, 0);
});

test('occupied directory restore keeps both folders, including an empty deleted folder', async t => {
    const { stored, root } = await setup(t);
    await fs.ensureDir(path.join(root, 'a/Empty'));
    const item = await stored.trashDirectory('a', 'Empty');
    await fs.outputFile(path.join(root, 'a/Empty/current.txt'), 'keep');
    assert.equal((await stored.restoreTrash('a', item.id)).reason, 'target-exists');
    assert.equal(await fs.readFile(path.join(root, 'a/Empty/current.txt'), 'utf8'), 'keep');
    assert.equal((await stored.listTrash('a')).items.length, 1);
});

test('legacy retained blobs become recoverable original paths and are not reimported after restore', async t => {
    const { stored, root } = await setup(t);
    const sha = crypto.createHash('sha256').update('legacy').digest('hex');
    await fs.outputFile(path.join(root, 'a/.stored-tmp/retained', sha), 'legacy');
    await fs.outputFile(path.join(root, 'a/live.txt'), 'current');
    stored.index.openDB('retained').putSync(sha, {
        sha256: sha, backend: 'a', keys: ['a:old/one.txt', 'a:old/two.txt', 'a:live.txt'], size: 6,
        firstAt: Date.now(), lastAt: Date.now(),
    });
    const { items } = await stored.listTrash('a');
    assert.deepEqual(items.map(i => i.key).sort(), ['old/one.txt', 'old/two.txt']);
    assert.ok(items.every(i => i.legacy));
    assert.equal((await stored.restoreTrash('a', items[0].id)).ok, true);
    assert.equal((await stored.listTrash('a')).items.length, 1);
});

test('pagination visits every item once and failed deletes do not appear as successful trash', async t => {
    const { stored, put } = await setup(t);
    for (let n = 0; n < 7; n++) { await put(`${n}.txt`, `${n}`); await stored.removeObject('a', `${n}.txt`); }
    const ids = []; let cursor = null;
    do { const page = await stored.listTrash('a', { limit: 2, cursor }); ids.push(...page.items.map(i => i.id)); cursor = page.cursor; } while (cursor);
    assert.equal(ids.length, 7); assert.equal(new Set(ids).size, 7);
    await put('denied.txt', 'keep');
    const backend = stored.getBackend('a');
    const original = backend.delete; backend.delete = async () => { throw new Error('permission denied'); };
    await assert.rejects(stored.removeObject('a', 'denied.txt'), /permission denied/);
    backend.delete = original;
    assert.equal((await stored.listTrash('a')).items.length, 7);
});

test('folder deletion rejects symlink ancestors and restore never writes through a symlink', async t => {
    const { stored, root } = await setup(t);
    await fs.ensureDir(path.join(root, 'a/Parent/Folder'));
    const item = await stored.trashDirectory('a', 'Parent/Folder');
    await fs.remove(path.join(root, 'a/Parent'));
    await fs.ensureDir(path.join(root, 'outside'));
    await fs.symlink(path.join(root, 'outside'), path.join(root, 'a/Parent'));
    await assert.rejects(stored.restoreTrash('a', item.id), /symbolic links/);
    assert.equal(await fs.pathExists(path.join(root, 'outside/Folder')), false);
    assert.equal((await stored.listTrash('a')).items.length, 1);
});

test('file restore refuses a replaced symlink parent and remains recoverable', async t => {
    const { stored, put, root } = await setup(t);
    await put('Parent/file.txt', 'original');
    await stored.removeObject('a', 'Parent/file.txt');
    const [item] = (await stored.listTrash('a')).items;
    await fs.remove(path.join(root, 'a/Parent'));
    await fs.ensureDir(path.join(root, 'outside'));
    await fs.symlink(path.join(root, 'outside'), path.join(root, 'a/Parent'));
    await assert.rejects(stored.restoreTrash('a', item.id), /symbolic links/);
    assert.equal(await fs.pathExists(path.join(root, 'outside/file.txt')), false);
    assert.equal((await stored.listTrash('a')).items.length, 1);
});

test('retention failure refuses a delete or overwrite before changing the original', async t => {
    const { stored, put, root } = await setup(t);
    await put('important.txt', 'original');
    t.mock.method(stored.getBackend('a'), 'retain', async () => { throw new Error('disk full'); });
    await assert.rejects(stored.removeObject('a', 'important.txt'), { code: 'RETAIN_FAILED' });
    await assert.rejects(put('important.txt', 'replacement'), { code: 'RETAIN_FAILED' });
    assert.equal(await fs.readFile(path.join(root, 'a/important.txt'), 'utf8'), 'original');
    assert.equal((await stored.listTrash('a')).items.length, 0);
});

test('equal contents in separate backends restore from the surviving retained copy', async t => {
    const { stored, put, root } = await setup(t);
    for (const backend of ['a', 'b']) {
        await put('same.txt', 'same contents', backend);
        await stored.removeObject(backend, 'same.txt');
    }
    await fs.remove(path.join(root, 'a/.stored-tmp/retained'));
    const [item] = (await stored.listTrash('b')).items;
    assert.equal((await stored.restoreTrash('b', item.id)).ok, true);
    assert.equal(await fs.readFile(path.join(root, 'b/same.txt'), 'utf8'), 'same contents');
});

test('pending folder deletion recovers after interruption and expiry removes its private trash', async t => {
    const { stored, root } = await setup(t);
    await fs.outputFile(path.join(root, 'a/Folder/.hidden'), 'saved');
    const removed = await stored.trashDirectory('a', 'Folder');
    const db = stored.index.openDB('backend-trash-v1');
    const record = db.get(`item:${removed.id}`);
    db.putSync(`item:${removed.id}`, { ...record, state: 'pending' });
    assert.equal((await stored.listTrash('a')).items[0].id, removed.id);
    assert.equal(db.get(`item:${removed.id}`).state, 'active');
    await stored.sweepRetained({ now: Date.now() + 31 * 86400000 });
    assert.equal((await stored.listTrash('a')).items.length, 0);
    assert.equal(await fs.pathExists(path.join(root, 'a/.stored-tmp/trash', removed.id)), false);
});
