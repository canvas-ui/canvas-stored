import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import Stored from '../src/index.js';
import { prepareIndex } from '../src/storage/recover-index.js';
import { verifyDatabase } from '../src/storage/lmdb.js';

async function temporary(t) {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'stored-recovery-'));
    t.after(() => fs.rm(root, { recursive: true, force: true }));
    return root;
}

test('damaged index is preserved, startup succeeds, and Rescan rebuilds byte locations', async t => {
    const root = await temporary(t);
    const backend = path.join(root, 'files');
    await fs.mkdir(backend);
    await fs.writeFile(path.join(backend, 'photo.jpg'), 'original-backend-bytes');
    const config = { root: path.join(root, 'stored'), checksums: ['sha256'] };
    let stored = await Stored.open(config);
    stored.addBackend('home', { driver: 'file', root: backend, watch: false });
    const initial = await stored.scan('home');
    const checksum = initial.files[0].checksums.sha256;
    await stored.stop();

    const index = path.join(config.root, 'index');
    const database = path.join(index, 'data.mdb');
    const handle = await fs.open(database, 'r+');
    try { await handle.write(Buffer.alloc(8192, 0xff), 0, 8192, 0); } finally { await handle.close(); }
    const damaged = await fs.readFile(database);
    await fs.writeFile(path.join(index, 'preserve-me'), 'operational-record-marker');
    const stages = [];
    stored = await Stored.open({ ...config, onStartupStage: stage => stages.push(stage) });
    try {
        const recovery = stages.find(stage => stage.stage === 'stored.index.quarantined');
        assert.ok(recovery, JSON.stringify(stages));
        assert.deepEqual(await fs.readFile(path.join(recovery.preservedIndexPath, 'data.mdb')), damaged);
        assert.equal(await fs.readFile(path.join(recovery.preservedIndexPath, 'preserve-me'), 'utf8'), 'operational-record-marker');
        const report = JSON.parse(await fs.readFile(path.join(recovery.quarantinePath, 'recovery.json'), 'utf8'));
        assert.equal(report.databasePath, index);
        assert.ok(report.reason);
        assert.equal(stored.index.size, 0);
        stored.addBackend('home', { driver: 'file', root: backend, watch: false });
        const rebuilt = await stored.scan('home');
        assert.equal(rebuilt.files[0].checksums.sha256, checksum);
        assert.ok(stored.index.get('home:photo.jpg'));
        assert.equal(await fs.readFile(path.join(backend, 'photo.jpg'), 'utf8'), 'original-backend-bytes');
    } finally { await stored.stop(); }
    assert.ok(await verifyDatabase(index));
    const reopenedStages = [];
    stored = await Stored.open({ ...config, onStartupStage: stage => reopenedStages.push(stage) });
    try {
        assert.ok(stored.index.get('home:photo.jpg'));
        assert.equal(reopenedStages.some(stage => stage.stage === 'stored.index.quarantined'), false);
    } finally { await stored.stop(); }
});

test('fresh indexes are not quarantined; repeated damaged indexes get distinct preserved directories', async t => {
    const root = await temporary(t);
    const index = path.join(root, 'index');
    assert.equal(await prepareIndex(index), null);
    const recoveries = [];
    for (let n = 0; n < 2; n++) {
        await fs.mkdir(index);
        await fs.writeFile(path.join(index, 'data.mdb'), '');
        recoveries.push(await prepareIndex(index));
    }
    assert.notEqual(recoveries[0].quarantinePath, recoveries[1].quarantinePath);
    for (const recovery of recoveries) assert.equal((await fs.stat(path.join(recovery.preservedIndexPath, 'data.mdb'))).size, 0);
});

test('permission, timeout and verifier infrastructure failures leave the original index untouched', async t => {
    const root = await temporary(t);
    const index = path.join(root, 'index');
    await fs.mkdir(index);
    await fs.writeFile(path.join(index, 'data.mdb'), 'preserve');
    for (const code of ['EACCES', 'ENOSPC', 'DATABASE_INTEGRITY_FAILED']) {
        const failure = Object.assign(new Error('verification unavailable'), { code });
        await assert.rejects(prepareIndex(index, { verify: async () => { throw failure; } }), error => error === failure);
        assert.equal(await fs.readFile(path.join(index, 'data.mdb'), 'utf8'), 'preserve');
        assert.deepEqual(await fs.readdir(root), ['index']);
    }
});

test('non-regular database paths are not quarantined', async t => {
    const root = await temporary(t);
    const index = path.join(root, 'index');
    await fs.mkdir(path.join(index, 'data.mdb'), { recursive: true });
    await assert.rejects(prepareIndex(index), error => error.canQuarantine === false);
    assert.deepEqual(await fs.readdir(root), ['index']);
});

test('quarantine failure propagates without replacing the original index', async t => {
    const root = await temporary(t);
    const index = path.join(root, 'index');
    await fs.mkdir(index);
    await fs.writeFile(path.join(index, 'data.mdb'), 'original');
    const verificationError = Object.assign(new Error('unreadable'), { code: 'DATABASE_INTEGRITY_FAILED', canQuarantine: true });
    const renameError = Object.assign(new Error('rename denied'), { code: 'EACCES' });
    t.mock.method(fs, 'rename', async () => { throw renameError; });
    await assert.rejects(prepareIndex(index, { verify: async () => { throw verificationError; } }), error => error === renameError);
    assert.equal(await fs.readFile(path.join(index, 'data.mdb'), 'utf8'), 'original');
});
