import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import Stored from '../src/index.js';
import { spawn } from 'node:child_process';

async function setup(t) {
    const root = await fs.mkdtemp('/tmp/stored-replicas-');
    const open = () => {
        const s = new Stored({ root: path.join(root, 'state'), retention: { days: 30 }, replication: { autoStart: false } });
        s.on('error', () => {});
        s.addBackend('data', { driver: 'cacache', root: path.join(root, 'data') });
        for (const backend of ['a', 'b']) s.addBackend(backend, { driver: 'file', root: path.join(root, backend), watch: false });
        return s;
    };
    let stored = open();
    t.after(async () => { await stored.stop(); await fs.rm(root, { recursive: true, force: true }); });
    const put = body => stored.put(Buffer.from(body), { backends: ['data'] });
    const plan = (file, replicas, extra = {}) => ({ id: 'op-1', owner: 'document-1', contentId: file.id,
        source: { backend: 'data', key: file.key }, replicas,
        release: [{ backend: 'data', key: file.key }], ...extra });
    const run = async input => { stored.replicas.submit(input); await stored.replicas.drain(); return stored.replicas.get(input.id); };
    return { root, get stored() { return stored; }, put, plan, run,
        reopen: async () => { await stored.stop(); stored = open(); return stored; } };
}

test('all exact targets verify before staging is released, including two folders on one backend', async t => {
    const c = await setup(t);
    const file = await c.put('invoice');
    const op = await c.run(c.plan(file, [{ backend: 'a', key: 'Accounting/a.txt' }, { backend: 'a', key: 'Backup/a.txt' }]));
    assert.equal(op.state, 'complete');
    assert.equal(await c.stored.getBackend('data').stat(file.key), null);
    for (const key of ['Accounting/a.txt', 'Backup/a.txt']) assert.equal(await fs.readFile(path.join(c.root, 'a', key), 'utf8'), 'invoice');
    assert.equal(c.stored.replicas.owner('document-1').replicas.length, 2);
});

test('zero-byte content replicates through staging to every destination', async t => {
    const c = await setup(t);
    const file = await c.put('');
    const op = await c.run(c.plan(file, [{ backend: 'a', key: 'empty.txt' }, { backend: 'b', key: 'empty.txt' }]));
    assert.equal(op.state, 'complete', JSON.stringify(op.error));
    for (const backend of ['a', 'b']) {
        assert.deepEqual(await fs.readFile(path.join(c.root, backend, 'empty.txt')), Buffer.alloc(0));
    }
    assert.equal(await c.stored.getBackend('data').stat(file.key), null);
});

test('partial failure keeps staging and resumes without duplicating a completed target after restart', async t => {
    const c = await setup(t);
    const file = await c.put('invoice');
    await fs.writeFile(path.join(c.root, 'b', 'taken.txt'), 'other data');
    const input = c.plan(file, [{ backend: 'a', key: 'a.txt' }, { backend: 'b', key: 'taken.txt' }]);
    const failed = await c.run(input);
    assert.equal(failed.state, 'blocked');
    assert.equal(failed.outcomes[0].state, 'verified');
    assert.ok(await c.stored.getBackend('data').stat(file.key));
    const inode = (await fs.stat(path.join(c.root, 'a', 'a.txt'))).ino;
    await c.reopen();
    assert.equal(c.stored.replicas.submit(input).state, 'blocked');
    await fs.rename(path.join(c.root, 'b', 'taken.txt'), path.join(c.root, 'b', 'other.txt'));
    assert.equal((await c.stored.replicas.retry(input.id)).state, 'complete');
    assert.equal((await fs.stat(path.join(c.root, 'a', 'a.txt'))).ino, inode);
    assert.equal(await fs.readFile(path.join(c.root, 'b', 'other.txt'), 'utf8'), 'other data');
});

test('a newer edit supersedes partial work without losing replica ownership or staging versions', async t => {
    const c = await setup(t); const old = await c.put('draft');
    await fs.writeFile(path.join(c.root, 'b', 'invoice'), 'independent');
    const targets = [{ backend: 'a', key: 'invoice' }, { backend: 'b', key: 'invoice' }];
    const prior = await c.run(c.plan(old, targets));
    assert.equal(prior.state, 'blocked');
    const next = await c.put('final');
    await fs.rename(path.join(c.root, 'b', 'invoice'), path.join(c.root, 'b', 'independent'));
    const input = c.plan(next, targets, { id: 'next', release: [
        { backend: 'data', key: old.key, contentId: old.id }, { backend: 'data', key: next.key, contentId: next.id },
    ] });
    await c.stored.replicas.supersede(prior.id, input); await c.stored.replicas.drain();
    assert.equal(c.stored.replicas.get('next').state, 'complete', JSON.stringify(c.stored.replicas.get('next').error));
    assert.equal(c.stored.replicas.get(prior.id).state, 'cancelled');
    for (const backend of ['a', 'b']) assert.equal((await c.stored.getBackend(backend).get('invoice')).toString(), 'final');
    for (const file of [old, next]) assert.equal(await c.stored.getBackend('data').stat(file.key), null);
    await c.stored.replicas.drain();
    assert.equal(await c.stored.cache.getInfo(`replica-operation:${prior.id}`), null);
});

test('API edits pin immutable local ingress until the complete replica generation succeeds', async t => {
    const c = await setup(t); const old = await c.put('old');
    const replicas = [{ backend: 'a', key: 'file' }, { backend: 'b', key: 'file' }];
    await c.run(c.plan(old, replicas));
    const edited = await c.stored.writeObject('a', 'file', Buffer.from('accepted edit'));
    const ingress = c.stored.replicas.sourceFor('document-1', edited.id);
    assert.ok(ingress);
    await fs.writeFile(path.join(c.root, 'a', 'file'), 'newer independent edit');
    assert.equal((await c.stored.cache.get(ingress.cacheKey)).data.toString(), 'accepted edit');
    const result = await c.run({ id: 'pinned-edit', owner: 'document-1', contentId: edited.id,
        sourceCacheKey: ingress.cacheKey, ingressSeq: ingress.seq, replicas, release: [] });
    assert.equal(result.state, 'blocked');
    assert.equal(result.error.code, 'TARGET_CHANGED');
    assert.ok(await c.stored.cache.getInfo(ingress.cacheKey));
    assert.equal(await fs.readFile(path.join(c.root, 'a', 'file'), 'utf8'), 'newer independent edit');
});

test('receipt rejects a reused operation id with different destinations', async t => {
    const c = await setup(t); const file = await c.put('x');
    const input = c.plan(file, [{ backend: 'a', key: 'x' }]);
    await c.run(input);
    assert.throws(() => c.stored.replicas.submit({ ...input, replicas: [] }), { code: 'IDEMPOTENCY_CONFLICT' });
});

test('a second Stored executor cannot concurrently mutate the same root', async t => {
    const c = await setup(t);
    assert.throws(() => new Stored({ root: path.join(c.root, 'state') }), { code: 'EXECUTOR_ACTIVE' });
    const file = await c.put('first executor remains usable');
    assert.equal((await c.run(c.plan(file, [{ backend: 'a', key: 'x' }]))).state, 'complete');
});

test('Linux executor recovery does not confuse a reused PID with a live lock', { skip: process.platform !== 'linux' }, async t => {
    const c = await setup(t);
    c.stored.index.openDB('replica.meta.v1').putSync('executor', { pid: process.pid, token: 'old-process-incarnation' });
    await c.reopen();
    const file = await c.put('recovered');
    assert.equal((await c.run(c.plan(file, [{ backend: 'a', key: 'x' }]))).state, 'complete');
});

for (const phase of ['place', 'release', 'retire']) test(`journal recovers abrupt process exit after ${phase}`, async t => {
    const c = await setup(t); const file = await c.put('durable');
    let input = c.plan(file, [{ backend: 'a', key: 'invoice' }, { backend: 'b', key: 'invoice' }]);
    if (phase === 'retire') {
        await c.run(input);
        input = c.plan(file, [{ backend: 'data', key: file.key, mirror: false }], {
            id: 'op-remove', source: { backend: 'a', key: 'invoice' }, release: [],
        });
    }
    await c.stored.stop();
    const child = spawn(process.execPath, [new URL('./fixtures/replica-crash-worker.mjs', import.meta.url).pathname, c.root, phase, JSON.stringify(input)], { stdio: ['ignore', 'pipe', 'pipe'] });
    let output = '';
    child.stdout.on('data', data => { output += data; }); child.stderr.on('data', data => { output += data; });
    assert.equal(await new Promise((resolve, reject) => { child.on('error', reject); child.on('exit', resolve); }), 42, output);
    await c.reopen();
    await c.stored.replicas.drain();
    const result = c.stored.replicas.get(input.id);
    assert.equal(result.state, 'complete', JSON.stringify(result.error));
    if (phase === 'retire') {
        assert.equal((await c.stored.getBackend('data').get(file.key)).toString(), 'durable');
        for (const backend of ['a', 'b']) assert.equal(await c.stored.getBackend(backend).stat('invoice'), null);
    } else {
        for (const backend of ['a', 'b']) assert.equal((await c.stored.getBackend(backend).get('invoice')).toString(), 'durable');
        assert.equal(await c.stored.getBackend('data').stat(file.key), null);
    }
    assert.equal(await c.stored.cache.getInfo(`replica-operation:${input.id}`), null);
});

test('leaving a mirror stages into the blob store before retiring owned files', async t => {
    const c = await setup(t); const file = await c.put('keep');
    await c.run(c.plan(file, [{ backend: 'a', key: 'x' }, { backend: 'a', key: 'twin' }]));
    const result = await c.run(c.plan(file, [{ backend: 'data', key: file.key, mirror: false }], {
        id: 'op-2', source: { backend: 'a', key: 'x' }, release: [],
    }));
    assert.equal(result.state, 'complete', JSON.stringify(result.error));
    assert.equal((await c.stored.getBackend('data').get(file.key)).toString(), 'keep');
    assert.equal((await c.stored.listTrash('a')).items.length, 2);
});

test('a shared replica survives until its last owner releases it', async t => {
    const c = await setup(t); const file = await c.put('shared');
    const replicas = [{ backend: 'a', key: 'shared.txt' }];
    await c.run(c.plan(file, replicas, { release: [] }));
    await c.run(c.plan(file, replicas, { id: 'op-2', owner: 'document-2', release: [] }));
    assert.equal((await c.run(c.plan(file, [], { id: 'op-3', release: [] }))).state, 'complete');
    assert.equal(await fs.readFile(path.join(c.root, 'a', 'shared.txt'), 'utf8'), 'shared');
    assert.equal((await c.run(c.plan(file, [], { id: 'op-4', owner: 'document-2', release: [] }))).state, 'complete');
    await assert.rejects(fs.stat(path.join(c.root, 'a', 'shared.txt')), { code: 'ENOENT' });
});

test('mutable replicas never share an inode with staging or other destinations', async t => {
    const c = await setup(t); const file = await c.put('original');
    await c.run(c.plan(file, [{ backend: 'a', key: 'one' }, { backend: 'a', key: 'two' }], { release: [] }));
    await fs.writeFile(path.join(c.root, 'a', 'one'), 'external edit');
    assert.equal(await fs.readFile(path.join(c.root, 'a', 'two'), 'utf8'), 'original');
    assert.equal((await c.stored.getBackend('data').get(file.key)).toString(), 'original');
});

test('an edit updates owned old versions but refuses an independently changed replica', async t => {
    const c = await setup(t); const old = await c.put('old');
    const replicas = [{ backend: 'a', key: 'one' }, { backend: 'b', key: 'two' }];
    await c.run(c.plan(old, replicas));
    const next = await c.put('next');
    await fs.writeFile(path.join(c.root, 'b', 'two'), 'concurrent external edit');
    const result = await c.run(c.plan(next, replicas, { id: 'op-edit' }));
    assert.equal(result.state, 'blocked');
    assert.equal(await fs.readFile(path.join(c.root, 'a', 'one'), 'utf8'), 'next');
    assert.equal(await fs.readFile(path.join(c.root, 'b', 'two'), 'utf8'), 'concurrent external edit');
    assert.ok(await c.stored.getBackend('data').stat(next.key));
});

test('an overwrite captures the original inode so a concurrent open-handle edit is recoverable', async t => {
    const c = await setup(t);
    await c.stored.writeObject('a', 'file', Buffer.from('old'));
    const handle = await fs.open(path.join(c.root, 'a', 'file'), 'r+');
    try {
        assert.ok((await c.stored.writeObject('a', 'file', Buffer.from('new'))).ok);
        await handle.write(Buffer.from('late writer'), 0, 11, 0);
        assert.equal(await fs.readFile(path.join(c.root, 'a', 'file'), 'utf8'), 'new');
        const captured = [...c.stored.index.openDB('backend-trash-v1').getRange()].map(r => r.value).find(r => r.reason === 'overwrite');
        assert.ok(captured?.quarantined);
        assert.equal(await fs.readFile(path.join(c.root, 'a/.stored-tmp/trash', captured.id), 'utf8'), 'late writer');
    } finally { await handle.close(); }
});

test('keyed writes validate actual bytes when indexing is stale or absent', async t => {
    const c = await setup(t);
    const old = await c.stored.writeObject('a', 'file', Buffer.from('old'));
    await fs.writeFile(path.join(c.root, 'a', 'file'), 'external edit');
    const refused = await c.stored.writeObject('a', 'file', Buffer.from('old'), { ifMatch: old.sha256 });
    assert.equal(refused.ok, false);
    assert.equal(refused.reason, 'precondition-failed');
    assert.equal(await fs.readFile(path.join(c.root, 'a', 'file'), 'utf8'), 'external edit');
    await fs.writeFile(path.join(c.root, 'a', 'unknown'), 'unindexed original');
    assert.ok((await c.stored.writeObject('a', 'unknown', Buffer.from('replacement'))).ok);
    assert.ok(c.stored.listRetained({ backend: 'a' }).some(r => r.keys.includes('a:unknown')));
});

test('preexisting equal content is not implicitly adopted for deletion', async t => {
    const c = await setup(t); const file = await c.put('existing');
    await c.stored.writeObject('a', 'existing', Buffer.from('existing'));
    await c.run(c.plan(file, [{ backend: 'a', key: 'existing' }], { release: [] }));
    assert.equal(c.stored.replicas.owner('document-1').replicas[0].managed, false);
    await c.run(c.plan(file, [], { id: 'op-2', release: [] }));
    assert.equal(await fs.readFile(path.join(c.root, 'a', 'existing'), 'utf8'), 'existing');
});

test('edited replica blocks retirement and preserves both the new bytes and recovery source', async t => {
    const c = await setup(t); const file = await c.put('old');
    await c.run(c.plan(file, [{ backend: 'a', key: 'x' }], { release: [] }));
    await fs.writeFile(path.join(c.root, 'a', 'x'), 'new external edit');
    const result = await c.run(c.plan(file, [], { id: 'op-2', release: [] }));
    assert.equal(result.state, 'blocked');
    assert.equal(result.error.code, 'TARGET_CHANGED');
    assert.equal(await fs.readFile(path.join(c.root, 'a', 'x'), 'utf8'), 'new external edit');
    assert.ok(await c.stored.getBackend('data').stat(file.key));
});

test('an edited source cannot be replicated under an old checksum', async t => {
    const c = await setup(t); const file = await c.put('old');
    await c.stored.writeObject('a', 'source', Buffer.from('new'));
    const result = await c.run(c.plan(file, [{ backend: 'b', key: 'target' }], { source: { backend: 'a', key: 'source' } }));
    assert.equal(result.state, 'blocked');
    assert.equal(result.error.code, 'SOURCE_CHANGED');
    assert.equal(await c.stored.getBackend('b').stat('target'), null);
});

test('folder retirement never removes unindexed children', async t => {
    const c = await setup(t);
    await c.run({ id: 'folder-1', owner: 'folder', replicas: [{ backend: 'a', key: 'Folder', kind: 'directory' }] });
    await fs.writeFile(path.join(c.root, 'a', 'Folder', '.unindexed'), 'keep');
    const result = await c.run({ id: 'folder-2', owner: 'folder', replicas: [] });
    assert.equal(result.state, 'complete', JSON.stringify(result.error));
    assert.equal(await fs.readFile(path.join(c.root, 'a', 'Folder', '.unindexed'), 'utf8'), 'keep');
});

test('unsupported automatic removal is rejected before any placement', async t => {
    const c = await setup(t); const file = await c.put('x');
    assert.throws(() => c.stored.replicas.submit(c.plan(file, [{ backend: 'data', key: 'another' }])), { code: 'UNSUPPORTED_RETIRE' });
    assert.equal(c.stored.replicas.list().operations.length, 0);
});

test('retained and quarantined versions survive writes through an already open inode', async t => {
    const c = await setup(t);
    await c.stored.writeObject('a', 'live', Buffer.from('original'));
    const handle = await fs.open(path.join(c.root, 'a/live'), 'r+');
    t.after(() => handle.close());
    await c.stored.writeObject('a', 'live', Buffer.from('replacement'));
    await handle.write('modified', 0, 'utf8');
    const versions = c.stored.listRetained({ backend: 'a' });
    const stream = await c.stored.retainedStream(versions[0].sha256, 'a');
    const parts = []; for await (const chunk of stream) parts.push(chunk);
    assert.equal(Buffer.concat(parts).toString(), 'original');
    const live = await fs.open(path.join(c.root, 'a/live'), 'r+');
    await c.stored.removeObject('a', 'live');
    await live.write('new version', 0, 'utf8'); await live.close();
    const item = (await c.stored.listTrash('a')).items[0];
    assert.ok((await c.stored.restoreTrash('a', item.id)).ok);
    assert.equal(await fs.readFile(path.join(c.root, 'a/live'), 'utf8'), 'new version');
});

test('garbage collection preserves other keys sharing bytes and removes content after the final release', async t => {
    const c = await setup(t), backend = c.stored.getBackend('data');
    await backend.put('first', Buffer.from('same'));
    await backend.put('second', Buffer.from('same'));
    await backend.delete('first'); await backend.collectGarbage();
    assert.equal((await backend.get('second')).toString(), 'same');
    await backend.delete('second');
    const result = await backend.collectGarbage();
    assert.ok(result.reclaimedCount > 0);
    assert.equal(await backend.stat('second'), null);
});

test('workspace removal holds pause queued replica work and retired owner IDs can be reused safely', async t => {
    const c = await setup(t); const file = await c.put('review me');
    c.stored.replicas.setOwnerGuard(() => false);
    const input = c.plan(file, [{ backend: 'a', key: 'held.txt' }]);
    const paused = await c.run(input);
    assert.equal(paused.error.code, 'OWNER_HELD');
    await assert.rejects(fs.stat(path.join(c.root, 'a/held.txt')), { code: 'ENOENT' });
    assert.ok(await c.stored.getBackend('data').stat(file.key));
    await assert.rejects(c.stored.replicas.retireOwner(input.owner), { code: 'OWNER_BUSY' });
    c.stored.replicas.setOwnerGuard(() => true);
    assert.equal((await c.stored.replicas.retry(input.id)).state, 'complete');
    await c.stored.replicas.retireOwner(input.owner);
    assert.equal(c.stored.replicas.owner(input.owner), null);
    assert.equal(c.stored.replicas.list({ owner: input.owner }).operations.length, 0);
    assert.equal(await fs.readFile(path.join(c.root, 'a/held.txt'), 'utf8'), 'review me', 'retiring metadata never removes backend bytes');
});

test('replica endpoint and latest receipt lookups do not rescan history per file', async t => {
    const c = await setup(t); const file = await c.put('lookup');
    await c.run(c.plan(file, [{ backend: 'a', key: 'lookup.txt' }]));
    const opsDb = c.stored.index.openDB('replica.operations.v1');
    const range = opsDb.getRange;
    let ownerReads = 0, opReads = 0;
    // openDB returns a separate handle; instrument the shared environment's
    // prototype so reads through ReplicaService's private handles count too.
    t.mock.method(Object.getPrototypeOf(opsDb), 'getRange', function (...args) {
        if (this.name === 'replica.owners.v1') ownerReads++;
        if (this.name === 'replica.operations.v1') opReads++;
        return range.apply(this, args);
    });
    for (let i = 0; i < 100; i++) {
        const [owner] = c.stored.replicas.ownersAt('a', 'lookup.txt');
        assert.equal(owner.owner, 'document-1');
        owner.replicas.length = 0; // returned records must not mutate the ledger/cache
        assert.equal(c.stored.replicas.latest('document-1').id, 'op-1');
    }
    assert.ok(ownerReads <= 1); assert.equal(opReads, 1);
    await fs.rename(path.join(c.root, 'a/lookup.txt'), path.join(c.root, 'a/moved.txt'));
    await c.stored.replicas.recordRelocation('a', 'lookup.txt', 'moved.txt');
    assert.deepEqual(c.stored.replicas.ownersAt('a', 'lookup.txt'), []);
    assert.equal(c.stored.replicas.ownersAt('a', 'moved.txt')[0].replicas[0].key, 'moved.txt');
    await c.reopen();
    assert.equal(c.stored.replicas.ownersAt('a', 'moved.txt')[0].owner, 'document-1');
    assert.equal(c.stored.replicas.latest('document-1').state, 'complete');
    await c.stored.replicas.retireOwner('document-1');
    assert.deepEqual(c.stored.replicas.ownersAt('a', 'moved.txt'), []);
    assert.equal(c.stored.replicas.latest('document-1'), null);
});

test('rolled-back owner changes cannot poison replica lookups', async t => {
    const c = await setup(t); const file = await c.put('rollback');
    await c.run(c.plan(file, [{ backend: 'a', key: 'safe.txt' }]));
    assert.equal(c.stored.replicas.ownersAt('a', 'safe.txt').length, 1);
    assert.equal(c.stored.replicas.latest('document-1').id, 'op-1');
    const transaction = c.stored.index.transaction.bind(c.stored.index);
    const mock = t.mock.method(c.stored.index, 'transaction', action => transaction(() => {
        action();
        c.stored.replicas.ownersAt('a', 'safe.txt'); c.stored.replicas.latest('document-1');
        throw new Error('rollback');
    }));
    await assert.rejects(c.stored.replicas.retireOwner('document-1'), /rollback/);
    mock.mock.restore();
    assert.equal(c.stored.replicas.ownersAt('a', 'safe.txt').length, 1);
    assert.equal(c.stored.replicas.latest('document-1').id, 'op-1');
    assert.equal(await fs.readFile(path.join(c.root, 'a/safe.txt'), 'utf8'), 'rollback');
});

test('latest receipt stays indexed across submission and completion during ingestion', async t => {
    const c = await setup(t); const file = await c.put('concurrent reconciliation');
    await c.run(c.plan(file, [{ backend: 'a', key: 'version-1.txt' }], { release: [] }));
    const ops = c.stored.index.openDB('replica.operations.v1');
    const range = ops.getRange;
    let scans = 0;
    t.mock.method(Object.getPrototypeOf(ops), 'getRange', function (...args) {
        if (this.name === 'replica.operations.v1') scans++;
        return range.apply(this, args);
    });
    assert.equal(c.stored.replicas.latest('document-1').id, 'op-1');
    assert.equal(scans, 1);
    for (let i = 2; i <= 4; i++) {
        c.stored.replicas.submit(c.plan(file, [{ backend: 'a', key: `version-${i}.txt` }], { id: `op-${i}`, release: [] }));
        const afterSubmit = scans;
        assert.equal(c.stored.replicas.latest('document-1').id, `op-${i}`);
        assert.equal(scans, afterSubmit, 'submission updates the lookup without decoding old receipts again');
        await c.stored.replicas.drain();
        const afterExecution = scans;
        assert.equal(c.stored.replicas.latest('document-1').state, 'complete');
        assert.equal(scans, afterExecution, 'ownership commit must not invalidate receipt identity lookup');
    }
});

test('a failed submission discards lookups warmed inside its rolled-back transaction', async t => {
    const c = await setup(t); const file = await c.put('not accepted');
    const transaction = c.stored.index.transaction.bind(c.stored.index);
    const mock = t.mock.method(c.stored.index, 'transaction', action => transaction(() => {
        action();
        assert.equal(c.stored.replicas.latest('document-1').id, 'op-1');
        throw new Error('submission rollback');
    }));
    assert.throws(() => c.stored.replicas.submit(c.plan(file, [{ backend: 'a', key: 'rejected.txt' }])), /submission rollback/);
    mock.mock.restore();
    assert.equal(c.stored.replicas.latest('document-1'), null);
    assert.equal(c.stored.replicas.get('op-1'), null);
    assert.equal((await fs.readdir(path.join(c.root, 'a'))).length, 0);
});
