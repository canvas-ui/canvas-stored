import { test } from 'node:test';
import assert from 'node:assert/strict';
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Stored from '../src/index.js';

async function setup(t) {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'stored-dir-rename-'));
    const home = path.join(root, 'home');
    await fs.mkdir(home);
    const open = () => {
        const instance = new Stored({ root: path.join(root, 'state') });
        instance.addBackend('home', { driver: 'file', root: home, watch: false });
        return instance;
    };
    let stored = open();
    t.after(async () => { await stored.stop(); await fs.rm(root, { recursive: true, force: true }); });
    return { stored, home, reopen: async () => { await stored.stop(); stored = open(); return stored; } };
}

await test('1000 photos: one native rename, no content streams, stable inodes/identities and retry receipt', async t => {
    const { stored, home } = await setup(t);
    for (let n = 0; n < 1000; n++) await stored.writeObject('home', `Architektúra/Domček/${n}.jpg`, Buffer.from(`photo-${n % 10}`));
    await fs.mkdir(path.join(home, 'Architektúra/Domček/Empty'));
    const before = await fs.stat(path.join(home, 'Architektúra/Domček/0.jpg'));
    const identity = stored.index.get('home:Architektúra/Domček/0.jpg').id;
    const head = stored.head();
    let renames = 0;
    const nativeRename = fs.rename;
    t.mock.method(fs, 'rename', async (...args) => { renames++; return nativeRename(...args); });
    const backend = stored.getBackend('home');
    t.mock.method(backend, 'get', () => { throw new Error('must not read file contents'); });
    t.mock.method(backend, 'renameFrom', () => { throw new Error('must not move individual files'); });
    const args = ['home', 'Architektúra/Domček', 'Architektúra/Fotky', { operationId: 'move-1000', origin: 'dev1' }];
    const result = await stored.renameDirectory(...args);
    assert.equal(result.ok, true, JSON.stringify(result));
    assert.equal(renames, 1);
    assert.equal((await fs.stat(path.join(home, 'Architektúra/Fotky/0.jpg'))).ino, before.ino);
    assert.equal(stored.index.get('home:Architektúra/Fotky/0.jpg').id, identity);
    assert.equal(stored.index.locationsByBackend('home', { prefix: 'Architektúra/Domček/', limit: 2000 }).objects.length, 0);
    assert.equal(stored.index.locationsByBackend('home', { prefix: 'Architektúra/Fotky/', limit: 2000 }).objects.length, 1000);
    assert.ok((await fs.stat(path.join(home, 'Architektúra/Fotky/Empty'))).isDirectory());
    const changes = stored.changes({ since: head, limit: 2000 }).changes;
    assert.equal(changes.length, 1000);
    assert.ok(changes.every(c => c.op === 'rename' && c.origin === 'dev1'));
    const movedHead = stored.head();
    assert.equal((await stored.renameDirectory(...args)).ok, true);
    assert.equal(renames, 1);
    assert.equal(stored.head(), movedHead);
    // A repeated request must not move a newly recreated source directory.
    await fs.mkdir(path.join(home, 'Architektúra/Domček'));
    await fs.writeFile(path.join(home, 'Architektúra/Domček/new.txt'), 'keep');
    assert.equal((await stored.renameDirectory(...args)).ok, true);
    assert.equal(await fs.readFile(path.join(home, 'Architektúra/Domček/new.txt'), 'utf8'), 'keep');
});

await test('destination collisions, nested moves and symlink escapes leave the source untouched', async t => {
    const { stored, home } = await setup(t);
    await stored.writeObject('home', 'old/a.txt', 'source');
    await stored.writeObject('home', 'taken/a.txt', 'destination');
    assert.equal((await stored.renameDirectory('home', 'old', 'taken', { operationId: 'taken' })).reason, 'target-exists');
    assert.equal((await stored.renameDirectory('home', 'old', 'old/subdir', { operationId: 'nested' })).reason, 'invalid-key');
    const outside = await fs.mkdtemp(path.join(os.tmpdir(), 'stored-dir-outside-'));
    t.after(() => fs.rm(outside, { recursive: true, force: true }));
    await fs.symlink(outside, path.join(home, 'escape'));
    assert.equal((await stored.renameDirectory('home', 'old', 'escape/new/subdir', { operationId: 'escape' })).reason, 'invalid-key');
    assert.deepEqual(await fs.readdir(outside), []);
    assert.equal(await fs.readFile(path.join(home, 'old/a.txt'), 'utf8'), 'source');
    assert.equal(await fs.readFile(path.join(home, 'taken/a.txt'), 'utf8'), 'destination');
});

await test('a crash between filesystem rename and index commit is recoverable without another move', async t => {
    const { stored, home, reopen } = await setup(t);
    await stored.writeObject('home', 'old/a.txt', 'source');
    const mock = t.mock.method(stored.index, 'put', () => { throw new Error('interrupted commit'); });
    const args = ['home', 'old', 'new', { operationId: 'recover', origin: 'dev1' }];
    await assert.rejects(stored.renameDirectory(...args), /interrupted commit/);
    assert.equal(await fs.readFile(path.join(home, 'new/a.txt'), 'utf8'), 'source');
    mock.mock.restore();
    const recovered = await reopen();
    assert.equal((await recovered.renameDirectory(...args)).ok, true);
    assert.ok(recovered.index.get('home:new/a.txt'));
    assert.equal(recovered.index.get('home:old/a.txt'), null);
    const restarted = await reopen();
    assert.equal((await restarted.renameDirectory(...args)).ok, true, 'completed receipt survives restart too');
});

await test('an interrupted move cannot rekey a recreated source', async t => {
    const { stored, home } = await setup(t);
    await stored.writeObject('home', 'old/a.txt', 'original');
    const mock = t.mock.method(stored.index, 'put', () => { throw new Error('interrupted'); });
    const args = ['home', 'old', 'new', { operationId: 'source-recreated' }];
    await assert.rejects(stored.renameDirectory(...args), /interrupted/);
    mock.mock.restore();
    await stored.writeObject('home', 'old/a.txt', 'recreated');
    const recreated = stored.index.get('home:old/a.txt').id;
    assert.equal((await stored.renameDirectory(...args)).reason, 'target-exists');
    assert.equal(stored.index.get('home:old/a.txt').id, recreated);
    assert.equal(await fs.readFile(path.join(home, 'new/a.txt'), 'utf8'), 'original');
    assert.equal(await fs.readFile(path.join(home, 'old/a.txt'), 'utf8'), 'recreated');
});

await test('empty directories use the same operation', async t => {
    const { stored, home } = await setup(t);
    await fs.mkdir(path.join(home, 'empty'));
    assert.equal((await stored.renameDirectory('home', 'empty', 'renamed', { operationId: 'empty' })).ok, true);
    assert.ok((await fs.stat(path.join(home, 'renamed'))).isDirectory());
});
