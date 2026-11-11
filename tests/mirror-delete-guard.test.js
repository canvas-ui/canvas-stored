import { test, describe } from 'node:test';
import assert from 'node:assert';
import fs from 'fs-extra';
import os from 'os';
import path from 'path';
import Stored, { Mirror } from '../src/index.js';
import { createFakeHub, sha256, waitFor } from './helpers/fake-hub.js';

const MARKER = '.workspace/binding';

async function setup(name, { marker = false } = {}) {
    const base = path.join(os.tmpdir(), `stored-mirror-guard-${name}-${process.pid}`);
    await fs.remove(base);
    const hub = await createFakeHub({ root: path.join(base, 'hub'), token: 'tok' });
    const dir = path.join(base, 'a'); const folder = path.join(dir, 'folder');
    for (const d of ['folder', 'trash', 'conflicts']) await fs.ensureDir(path.join(dir, d));
    if (marker) await fs.outputFile(path.join(folder, MARKER), 'bound-1\n');
    const stored = new Stored({ root: path.join(dir, '.stored'), checksums: ['sha256'] });
    stored.on('error', () => {});
    stored.addBackend('local', {
        driver: 'file', root: folder, watch: true, stabilityThreshold: 200, followSymlinks: false, tempDir: '.workspace/tmp',
        ...(marker ? { createRoot: false, marker: { file: MARKER, id: 'bound-1' } } : {}),
    });
    stored.addBackend('trash', { driver: 'file', root: path.join(dir, 'trash') });
    stored.addBackend('conflicts', { driver: 'file', root: path.join(dir, 'conflicts') });
    stored.addBackend('remote', { driver: 'canvas', url: hub.url, workspaceId: 'ws1', backend: 'workspace:home', token: 'tok', deviceId: 'dev-a', deviceName: 'a', pollInterval: 500, retryDelays: [5, 10] });
    const m = new Mirror(stored, { id: 'm', local: 'local', remote: 'remote', trash: 'trash', conflicts: 'conflicts', deviceId: 'dev-a', deviceName: 'a', debounceMs: 100, deleteGuard: { min: 5 } });
    m.on('error', () => {});
    const keys = Array.from({ length: 8 }, (_, i) => `Docs/f${i}.txt`);
    for (const k of keys) await fs.outputFile(path.join(folder, k), `data ${k}`);
    await m.start();
    await waitFor(() => keys.every((k) => hub.sha(k) === sha256(`data ${k}`) && m.ledger.getBase(k)), { label: 'pushed', timeout: 20_000 });
    // chokidar's awaitWriteFinish swallows an unlink that follows a write too closely.
    await new Promise((r) => setTimeout(r, 800));
    const close = async () => { await m.stop(); await stored.stop(); await hub.close(); await fs.remove(base); };
    return { hub, folder, m, keys, close };
}

describe('Mirror: delete guard', { timeout: 120_000 }, () => {
    test('a burst of local deletions is held; approve sends it', async () => {
        const { hub, folder, m, keys, close } = await setup('approve');
        try {
            await fs.remove(path.join(folder, 'Docs'));
            await waitFor(() => m.status().heldDeletes === keys.length, { label: 'held', timeout: 20_000 });
            await m.idle();
            assert.ok(keys.every((k) => hub.sha(k) !== null), 'nothing was deleted on the hub');
            assert.match(m.status().lastError, /held/);
            assert.strictEqual(m.approveDeletes(), keys.length);
            await waitFor(() => keys.every((k) => hub.sha(k) === null), { label: 'hub deletes', timeout: 20_000 });
            assert.strictEqual(m.status().heldDeletes, 0);
        } finally { await close(); }
    });

    test('restore pulls the hub copies back', async () => {
        const { hub, folder, m, keys, close } = await setup('restore');
        try {
            await fs.remove(path.join(folder, 'Docs'));
            await waitFor(() => m.status().heldDeletes === keys.length, { label: 'held', timeout: 20_000 });
            assert.strictEqual(m.restoreDeletes(), keys.length);
            await waitFor(async () => (await Promise.all(keys.map((k) => fs.pathExists(path.join(folder, k))))).every(Boolean), { label: 'pulled back', timeout: 20_000 });
            assert.ok(keys.every((k) => hub.sha(k) === sha256(`data ${k}`)));
        } finally { await close(); }
    });

    test('a few deletions below the threshold go straight through', async () => {
        const { hub, folder, m, keys, close } = await setup('small');
        try {
            await fs.remove(path.join(folder, keys[0]));
            await waitFor(() => hub.sha(keys[0]) === null, { label: 'hub delete', timeout: 20_000 });
            assert.strictEqual(m.status().heldDeletes, 0);
        } finally { await close(); }
    });

    test('a folder that lost its marker never deletes on the hub, even when approved', async () => {
        const { hub, folder, m, keys, close } = await setup('marker', { marker: true });
        try {
            // What an unmounted drive looks like: the directory is there, empty.
            await fs.emptyDir(folder);
            await waitFor(() => m.status().heldDeletes > 0, { label: 'held', timeout: 20_000 });
            m.approveDeletes();
            await new Promise((r) => setTimeout(r, 1500));
            assert.ok(keys.every((k) => hub.sha(k) === sha256(`data ${k}`)), 'hub untouched');
        } finally { await close(); }
    });
});
