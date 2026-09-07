import { test, describe } from 'node:test';
import assert from 'node:assert';
import fs from 'fs-extra';
import os from 'os';
import path from 'path';
import crypto from 'crypto';
import Stored from '../src/index.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function waitFor(fn, { timeout = 8000, label = 'condition' } = {}) {
    const until = Date.now() + timeout;
    while (Date.now() < until) { if (await fn()) return; await sleep(50); }
    throw new Error(`Timed out waiting for ${label}`);
}

describe('file driver: directories', { timeout: 60_000 }, () => {
    test('delete() prunes emptied parent folders, keeps root, non-empty dirs and the staging dir', async () => {
        const base = path.join(os.tmpdir(), `stored-dirs-prune-${process.pid}`);
        await fs.remove(base);
        const root = path.join(base, 'root');
        const stored = new Stored({ root: path.join(base, '.stored'), checksums: ['sha256'] });
        stored.on('error', () => {});
        stored.addBackend('local', { driver: 'file', root, tempDir: '.workspace/tmp' });
        const backend = stored.getBackend('local');
        for (const k of ['Proj/a.txt', 'Proj/sub/deep/c.txt', 'Keep/x.txt', 'Keep/gone/y.txt', '.workspace/tmp/scratch.bin']) {
            await fs.outputFile(path.join(root, k), k);
        }
        await backend.delete('Proj/sub/deep/c.txt');
        assert.strictEqual(await fs.pathExists(path.join(root, 'Proj/sub')), false, 'empty chain pruned');
        assert.strictEqual(await fs.pathExists(path.join(root, 'Proj/a.txt')), true, 'stops at a non-empty dir');
        await backend.delete('Proj/a.txt');
        assert.strictEqual(await fs.pathExists(path.join(root, 'Proj')), false, 'last file takes the folder with it');
        assert.strictEqual(await fs.pathExists(root), true, 'root survives');
        await backend.delete('Keep/gone/y.txt');
        assert.strictEqual(await fs.pathExists(path.join(root, 'Keep/gone')), false);
        assert.strictEqual(await fs.pathExists(path.join(root, 'Keep/x.txt')), true);
        await backend.delete('.workspace/tmp/scratch.bin');
        assert.strictEqual(await fs.pathExists(path.join(root, '.workspace/tmp')), true, 'staging dir is never pruned');
        await stored.stop();
        await fs.remove(base);
    });

    test('watcher adopts files and folders chokidar missed in a fast nested create (followSymlinks:false)', async () => {
        const base = path.join(os.tmpdir(), `stored-dirs-heal-${process.pid}`);
        await fs.remove(base);
        const root = path.join(base, 'root');
        await fs.ensureDir(root);
        const stored = new Stored({ root: path.join(base, '.stored'), checksums: ['sha256'] });
        stored.on('error', () => {});
        const events = [];
        stored.on('file:add', (e) => events.push(`add ${e.key}`));
        stored.on('file:change', (e) => events.push(`change ${e.key}`));
        stored.on('file:unlink', (e) => events.push(`unlink ${e.key}`));
        stored.addBackend('local', { driver: 'file', root, watch: true, stabilityThreshold: 200, followSymlinks: false, tempDir: '.workspace/tmp' });
        await sleep(500);
        const keys = ['Proj/a.txt', 'Proj/sub/b.txt', 'Proj/sub/deep/c.txt'];
        for (const k of keys) { await fs.ensureDir(path.dirname(path.join(root, k))); await fs.writeFile(path.join(root, k), `data ${k}`); }
        await waitFor(() => keys.every((k) => stored.index.get(`local:${k}`)), { label: 'all three indexed' });
        for (const k of keys) assert.strictEqual(events.filter((e) => e === `add ${k}`).length, 1, `exactly one add for ${k}`);
        // The adopted folders are watched from now on (give chokidar a moment to finish
        // its initial scan of them): edits and deletes inside them surface.
        await sleep(1000);
        await fs.writeFile(path.join(root, 'Proj/sub/deep/c.txt'), 'edited');
        await fs.remove(path.join(root, 'Proj/sub/b.txt'));
        // (chokidar may report the in-place rewrite as unlink+add rather than change; the index is what matters.)
        const edited = crypto.createHash('sha256').update('edited').digest('hex');
        await waitFor(() => stored.index.get('local:Proj/sub/deep/c.txt')?.checksums?.sha256 === edited && !stored.index.get('local:Proj/sub/b.txt'), { label: 'edits in healed dirs reach the index' });
        assert.ok(events.includes('unlink Proj/sub/b.txt'));
        await stored.stop();
        await fs.remove(base);
    });
});
