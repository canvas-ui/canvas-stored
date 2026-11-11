import { test, describe } from 'node:test';
import assert from 'node:assert';
import fs from 'fs-extra';
import os from 'os';
import path from 'path';
import Stored, { Mirror } from '../src/index.js';
import { createFakeHub, sha256, waitFor } from './helpers/fake-hub.js';

const KEYS = Array.from({ length: 8 }, (_, i) => `Docs/f${i}.txt`);

async function setup(name) {
    const base = path.join(os.tmpdir(), `stored-mirror-moves-${name}-${process.pid}`);
    await fs.remove(base);
    const hub = await createFakeHub({ root: path.join(base, 'hub'), token: 'tok' });
    const dir = path.join(base, 'a'); const folder = path.join(dir, 'folder');
    for (const d of ['folder', 'trash', 'conflicts']) await fs.ensureDir(path.join(dir, d));
    let stored; let m;
    const up = async () => {
        stored = new Stored({ root: path.join(dir, '.stored'), checksums: ['sha256'] });
        stored.on('error', () => {});
        stored.addBackend('local', { driver: 'file', root: folder, watch: true, stabilityThreshold: 200, followSymlinks: false, tempDir: '.workspace/tmp' });
        stored.addBackend('trash', { driver: 'file', root: path.join(dir, 'trash') });
        stored.addBackend('conflicts', { driver: 'file', root: path.join(dir, 'conflicts') });
        stored.addBackend('remote', { driver: 'canvas', url: hub.url, workspaceId: 'ws1', backend: 'workspace:home', token: 'tok', deviceId: 'dev-a', deviceName: 'a', pollInterval: 500, retryDelays: [5, 10] });
        m = new Mirror(stored, { id: 'm', local: 'local', remote: 'remote', trash: 'trash', conflicts: 'conflicts', deviceId: 'dev-a', deviceName: 'a', debounceMs: 100, deleteGuard: { min: 5 } });
        m.on('error', () => {});
        await m.start();
    };
    const down = async () => { await m.idle(); await m.stop(); await stored.stop(); };
    for (const k of KEYS) await fs.outputFile(path.join(folder, k), `data ${k}`);
    await up();
    await waitFor(() => KEYS.every((k) => hub.sha(k) === sha256(`data ${k}`) && m.ledger.getBase(k)), { label: 'pushed', timeout: 20_000 });
    await down();
    const close = async () => { await m.stop(); await stored.stop(); await hub.close(); await fs.remove(base); };
    return { hub, folder, up, close, mirror: () => m, trash: path.join(dir, 'trash') };
}

const renames = (hub) => hub.calls.filter((c) => c.method === 'POST' && c.path.endsWith('/objects/rename'));
const deletes = (hub) => hub.calls.filter((c) => c.method === 'DELETE');

describe('Mirror: moves made while the mirror was down', { timeout: 120_000 }, () => {
    test('a folder moved on the device is renamed on the hub: no upload, no delete, no hold', async () => {
        const { hub, folder, up, close, mirror } = await setup('local');
        try {
            const puts = hub.puts().length;
            await fs.move(path.join(folder, 'Docs'), path.join(folder, 'Archive/Docs'));
            await up();
            await waitFor(() => KEYS.every((k) => hub.sha(`Archive/${k}`) === sha256(`data ${k}`) && hub.sha(k) === null), { label: 'renamed on the hub', timeout: 20_000 });
            await mirror().idle();
            assert.strictEqual(hub.puts().length, puts, 'no bytes were uploaded');
            assert.strictEqual(mirror().status().heldDeletes, 0);
            assert.strictEqual(renames(hub).length, 1, 'the folder went to the hub as one request');
            const ops = hub.stored.changes({ backend: 'workspace:home', since: 0 }).changes.filter((c) => c.key.startsWith('Archive/')).map((c) => c.op);
            assert.deepStrictEqual([...new Set(ops)], ['rename']);
            for (const k of KEYS) assert.strictEqual(await fs.readFile(path.join(folder, 'Archive', k), 'utf8'), `data ${k}`);
        } finally { await close(); }
    });

    test('a hub move is replayed as a local move, and an offline edit follows it', async () => {
        const { hub, folder, up, close, mirror, trash } = await setup('remote');
        try {
            const ino = (await fs.stat(path.join(folder, KEYS[0]))).ino;
            await hub.rename(KEYS[0], 'Filed/f0.txt');
            await hub.rename(KEYS[1], 'Filed/f1.txt');
            await fs.writeFile(path.join(folder, KEYS[1]), 'edited offline');
            await up();
            await waitFor(() => hub.sha('Filed/f1.txt') === sha256('edited offline') && mirror().ledger.getBase('Filed/f0.txt'), { label: 'converged', timeout: 20_000 });
            await mirror().idle();
            assert.strictEqual((await fs.stat(path.join(folder, 'Filed/f0.txt'))).ino, ino, 'moved, not downloaded');
            assert.strictEqual(await fs.readFile(path.join(folder, 'Filed/f1.txt'), 'utf8'), 'edited offline');
            assert.strictEqual(await fs.pathExists(path.join(folder, KEYS[0])), false);
            assert.strictEqual(await fs.pathExists(path.join(folder, KEYS[1])), false);
            assert.strictEqual(hub.sha(KEYS[1]), null, 'the old name does not come back');
            assert.deepStrictEqual(await fs.readdir(trash), [], 'nothing went through the trash');
        } finally { await close(); }
    });

    test('part of a folder moved on the device is renamed file by file', async () => {
        const { hub, folder, up, close, mirror } = await setup('partial');
        try {
            const puts = hub.puts().length;
            for (const k of KEYS.slice(0, 3)) await fs.move(path.join(folder, k), path.join(folder, 'Archive', k));
            await up();
            await waitFor(() => KEYS.slice(0, 3).every((k) => hub.sha(`Archive/${k}`) === sha256(`data ${k}`) && hub.sha(k) === null), { label: 'renamed on the hub', timeout: 20_000 });
            await mirror().idle();
            assert.strictEqual(renames(hub).length, 3);
            assert.strictEqual(hub.puts().length, puts);
            for (const k of KEYS.slice(3)) assert.strictEqual(hub.sha(k), sha256(`data ${k}`), 'the rest stays');
        } finally { await close(); }
    });
});

describe('Mirror: a large folder moved while the mirror is running', { timeout: 180_000 }, () => {
    test('is one directory rename on the hub: no upload, no delete', async () => {
        const base = path.join(os.tmpdir(), `stored-mirror-moves-live-${process.pid}`);
        await fs.remove(base);
        const hub = await createFakeHub({ root: path.join(base, 'hub'), token: 'tok' });
        const dir = path.join(base, 'a'); const folder = path.join(dir, 'folder');
        for (const d of ['folder', 'trash', 'conflicts']) await fs.ensureDir(path.join(dir, d));
        const keys = Array.from({ length: 1500 }, (_, i) => `Photos/${2000 + (i % 25)}/img${i}.jpg`);
        for (const k of keys) await fs.outputFile(path.join(folder, k), `pixels ${k}`);
        await fs.outputFile(path.join(folder, 'Other/keep.txt'), 'keep');
        const stored = new Stored({ root: path.join(dir, '.stored'), checksums: ['sha256'] });
        stored.on('error', () => {});
        stored.addBackend('local', { driver: 'file', root: folder, watch: true, stabilityThreshold: 200, followSymlinks: false, tempDir: '.workspace/tmp' });
        stored.addBackend('trash', { driver: 'file', root: path.join(dir, 'trash') });
        stored.addBackend('conflicts', { driver: 'file', root: path.join(dir, 'conflicts') });
        stored.addBackend('remote', { driver: 'canvas', url: hub.url, workspaceId: 'ws1', backend: 'workspace:home', token: 'tok', deviceId: 'dev-a', deviceName: 'a', pollInterval: 500, retryDelays: [5, 10] });
        const m = new Mirror(stored, { id: 'm', local: 'local', remote: 'remote', trash: 'trash', conflicts: 'conflicts', deviceId: 'dev-a', deviceName: 'a', debounceMs: 200, moveSettleMs: 500, concurrency: 8 });
        m.on('error', () => {});
        try {
            await m.start();
            await waitFor(() => keys.every((k) => hub.sha(k)) && [...m.ledger.bases('Photos/')].length === keys.length, { label: 'pushed', timeout: 120_000 });
            await m.idle();
            const puts = hub.puts().length;
            await fs.move(path.join(folder, 'Photos'), path.join(folder, 'Family/Pictures'));
            const moved = (k) => `Family/Pictures/${k.slice('Photos/'.length)}`;
            await waitFor(() => keys.every((k) => hub.sha(moved(k)) && hub.sha(k) === null) && [...m.ledger.bases('Family/')].length === keys.length, { label: 'moved on the hub', timeout: 120_000 });
            await new Promise((r) => setTimeout(r, 7000));
            await m.idle();
            assert.strictEqual(renames(hub).length, 1, 'one request for the whole folder');
            assert.strictEqual(deletes(hub).length, 0, 'nothing was deleted on the hub');
            assert.strictEqual(hub.puts().length, puts, 'no bytes were uploaded');
            assert.strictEqual(m.status().heldDeletes, 0);
            assert.strictEqual(m.status().pending, 0);
            assert.strictEqual([...m.ledger.bases('Photos/')].length, 0);
            assert.strictEqual(hub.sha('Other/keep.txt'), sha256('keep'));
            for (const k of keys.slice(0, 20)) assert.strictEqual(await fs.readFile(path.join(folder, moved(k)), 'utf8'), `pixels ${k}`);
        } finally { await m.stop(); await stored.stop(); await hub.close(); await fs.remove(base); }
    });
});
