import { test, describe } from 'node:test';
import assert from 'node:assert';
import fs from 'fs-extra';
import os from 'os';
import path from 'path';
import Stored, { Mirror } from '../src/index.js';
import { createFakeHub, sha256, waitFor } from './helpers/fake-hub.js';

/** A folder removed on the device disappears from the hub — files AND the folder shell; and vice versa. */
describe('Mirror: folders', { timeout: 120_000 }, () => {
    test('rm -rf locally → files deleted on the hub and the emptied folders pruned; hub delete → local folder pruned', async () => {
        const base = path.join(os.tmpdir(), `stored-mirror-rmdir-${process.pid}`);
        await fs.remove(base);
        const hub = await createFakeHub({ root: path.join(base, 'hub'), token: 'tok' });
        const dir = path.join(base, 'a'); const folder = path.join(dir, 'folder');
        for (const d of ['folder', 'trash', 'conflicts']) await fs.ensureDir(path.join(dir, d));
        const keys = ['Proj/a.txt', 'Proj/sub/b.txt', 'Proj/sub/deep/c.txt'];
        const stored = new Stored({ root: path.join(dir, '.stored'), checksums: ['sha256'] });
        stored.on('error', () => {});
        stored.addBackend('local', { driver: 'file', root: folder, watch: true, stabilityThreshold: 200, followSymlinks: false, tempDir: '.workspace/tmp' });
        stored.addBackend('trash', { driver: 'file', root: path.join(dir, 'trash') });
        stored.addBackend('conflicts', { driver: 'file', root: path.join(dir, 'conflicts') });
        stored.addBackend('remote', { driver: 'canvas', url: hub.url, workspaceId: 'ws1', backend: 'workspace:home', token: 'tok', deviceId: 'dev-a', deviceName: 'a', pollInterval: 500, retryDelays: [5, 10] });
        const m = new Mirror(stored, { id: 'm', local: 'local', remote: 'remote', trash: 'trash', conflicts: 'conflicts', deviceId: 'dev-a', deviceName: 'a', debounceMs: 100 });
        m.on('error', () => {});
        await m.start();
        try {
            // A tree created live (cp -r style) is fully uploaded.
            for (const k of keys) { await fs.ensureDir(path.dirname(path.join(folder, k))); await fs.writeFile(path.join(folder, k), `data ${k}`); }
            await waitFor(() => keys.every((k) => hub.sha(k) === sha256(`data ${k}`) && m.ledger.getBase(k)?.sha256 === sha256(`data ${k}`)), { label: 'tree pushed', timeout: 20_000 });

            await fs.remove(path.join(folder, 'Proj'));
            await waitFor(() => keys.every((k) => hub.sha(k) === null), { label: 'hub deletes', timeout: 20_000 });
            await waitFor(async () => !(await fs.pathExists(path.join(hub.home, 'Proj'))), { label: 'hub folder pruned' });
            assert.strictEqual(await fs.pathExists(path.join(hub.home, '.stored-tmp')), true, 'hub staging dir untouched');

            // The other direction: the hub drops the last file of a folder → local folder shell goes too (file lands in trash).
            await hub.put('Other/only.txt', 'only');
            await waitFor(() => m.ledger.getBase('Other/only.txt')?.sha256 === sha256('only'), { label: 'pulled', timeout: 20_000 });
            await hub.remove('Other/only.txt');
            m.nudge();
            await waitFor(async () => !(await fs.pathExists(path.join(folder, 'Other'))), { label: 'local folder pruned', timeout: 20_000 });
            assert.strictEqual(await fs.readFile(path.join(dir, 'trash', 'Other/only.txt'), 'utf8'), 'only');
        } finally {
            await m.stop(); await stored.stop(); await hub.close(); await fs.remove(base);
        }
    });
});
