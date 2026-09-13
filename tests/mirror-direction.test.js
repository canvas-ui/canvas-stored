import { test, describe, before, after } from 'node:test';
import assert from 'node:assert';
import fs from 'fs-extra';
import os from 'os';
import path from 'path';
import Stored, { Mirror } from '../src/index.js';
import { createFakeHub, sha256, waitFor, sleep } from './helpers/fake-hub.js';

/**
 * Mirror `direction` (pull = backup target, push = one-shot import) and the
 * (docId, version) protection evidence the ledger carries.
 */
describe('Mirror direction + applied versions', { timeout: 120_000 }, () => {
    const base = path.join(os.tmpdir(), `stored-mirror-dir-${process.pid}`);
    let hub;
    const devices = [];

    async function setupDevice(name, mirrorOpts = {}) {
        const dir = path.join(base, name);
        const folder = path.join(dir, 'folder');
        const trash = path.join(dir, 'trash');
        const conflicts = path.join(dir, 'conflicts');
        await fs.ensureDir(folder); await fs.ensureDir(trash); await fs.ensureDir(conflicts);
        const stored = new Stored({ root: path.join(dir, '.stored'), checksums: ['sha256'] });
        stored.on('error', () => {});
        stored.addBackend('local', { driver: 'file', root: folder, watch: true, stabilityThreshold: 100 });
        stored.addBackend('trash', { driver: 'file', root: trash });
        stored.addBackend('conflicts', { driver: 'file', root: conflicts });
        stored.addBackend('remote', {
            driver: 'canvas', url: hub.url, workspaceId: 'ws1', backend: 'workspace:home', token: 'tok',
            deviceId: `dev-${name}`, deviceName: name, pollInterval: 1000, retryDelays: [5, 10],
        });
        const mirror = new Mirror(stored, {
            id: 'm', local: 'local', remote: 'remote', trash: 'trash', conflicts: 'conflicts',
            deviceId: `dev-${name}`, deviceName: name, debounceMs: 100, offlineBackoffMs: [150, 150], ...mirrorOpts,
        });
        mirror.on('error', () => {});
        mirror.events = { reverts: [], skips: [] };
        mirror.on('revert', (e) => mirror.events.reverts.push(e));
        mirror.on('skip', (e) => mirror.events.skips.push(e));
        const dev = {
            name, folder, conflicts, stored, mirror,
            file: (key) => path.join(folder, key),
            async write(key, data) { await fs.ensureDir(path.dirname(this.file(key))); await fs.writeFile(this.file(key), data); },
            async read(key) { return fs.readFile(this.file(key), 'utf8').catch(() => null); },
            localSha(key) { return stored.index.get(`local:${key}`)?.checksums?.sha256 ?? null; },
            base(key) { return mirror.ledger.getBase(key); },
            async close() { await mirror.stop().catch(() => {}); await stored.stop(); },
        };
        devices.push(dev);
        await mirror.start();
        return dev;
    }
    const agreed = (dev, key, sha) => hub.sha(key) === sha && dev.localSha(key) === sha && dev.base(key)?.sha256 === sha;

    before(async () => {
        await fs.remove(base);
        hub = await createFakeHub({ root: path.join(base, 'hub'), token: 'tok' });
        await hub.put('Docs/seed.txt', 'seed');
    });
    after(async () => {
        for (const d of devices.splice(0)) await d.close();
        await hub.close();
        await fs.remove(base);
    });

    describe('pull (backup target)', () => {
        let p;
        test('pulls what the hub has; the base carries the hub docId/version; takeApplied hands the pair over once', async () => {
            p = await setupDevice('pull', { direction: 'pull' });
            await waitFor(() => agreed(p, 'Docs/seed.txt', sha256('seed')), { label: 'initial pull' });
            const b = p.base('Docs/seed.txt');
            assert.strictEqual(b.docId, hub.docIdOf('Docs/seed.txt'));
            assert.strictEqual(b.version, hub.versionOf('Docs/seed.txt'));
            assert.strictEqual(p.mirror.status().direction, 'pull');
            const applied = p.mirror.takeApplied();
            assert.deepStrictEqual(applied, [[b.docId, b.version]]);
            assert.deepStrictEqual(p.mirror.takeApplied(), []);
            assert.deepStrictEqual(p.mirror.appliedSnapshot(), [[b.docId, b.version]]);
        });

        test('a local-only file is never pushed: skipped as local-only, left in place', async () => {
            await p.write('Docs/mine.txt', 'only here');
            await waitFor(() => p.mirror.status().skips['Docs/mine.txt'] === 'local-only', { label: 'local-only skip' });
            await p.mirror.idle();
            assert.strictEqual(hub.sha('Docs/mine.txt'), null);
            assert.strictEqual(await p.read('Docs/mine.txt'), 'only here');
            assert.strictEqual(hub.puts('Docs/mine.txt').length, 0);
        });

        test('a local edit of a tracked file is reverted: edit kept in conflicts/, hub bytes back, nothing upstream', async () => {
            await p.write('Docs/seed.txt', 'edited on the backup');
            await waitFor(() => p.mirror.events.reverts.length === 1, { label: 'revert' });
            await waitFor(() => p.localSha('Docs/seed.txt') === sha256('seed'), { label: 'hub bytes back' });
            assert.strictEqual(hub.sha('Docs/seed.txt'), sha256('seed'));
            assert.strictEqual(hub.puts('Docs/seed.txt').length, 0);
            const kept = (await fs.readdir(path.join(p.conflicts, 'Docs'))).find((f) => f.startsWith('seed (conflict from pull'));
            assert.ok(kept, 'edit preserved under a conflict-copy name');
            assert.strictEqual(await fs.readFile(path.join(p.conflicts, 'Docs', kept), 'utf8'), 'edited on the backup');
            assert.strictEqual(p.mirror.status().reverted, 1);
        });

        test('a local delete of a tracked file pulls the hub copy back', async () => {
            await sleep(500);   // let the watcher settle after the revert's own write (awaitWriteFinish)
            await fs.remove(p.file('Docs/seed.txt'));
            await waitFor(async () => agreed(p, 'Docs/seed.txt', sha256('seed')) && !!(await p.read('Docs/seed.txt')), { label: 'pulled back' });
            assert.strictEqual(hub.calls.filter((c) => c.method === 'DELETE').length, 0);
        });

        test('hub edits and deletes still apply; the version moves with the hub', async () => {
            await hub.put('Docs/seed.txt', 'seed v2');
            await p.mirror.nudge();
            await waitFor(() => agreed(p, 'Docs/seed.txt', sha256('seed v2')), { label: 'pull edit' });
            assert.strictEqual(p.base('Docs/seed.txt').version, hub.versionOf('Docs/seed.txt'));
            await hub.remove('Docs/seed.txt');
            await p.mirror.nudge();
            await waitFor(() => p.localSha('Docs/seed.txt') === null, { label: 'trash-local' });
        });
    });

    describe('push (one-shot import)', () => {
        let q;
        test('pushes local files, base carries docId/version from the PUT; hub-only files are skipped as remote-only', async () => {
            await hub.put('Docs/hubonly.txt', 'hub only');
            q = await setupDevice('push', { direction: 'push' });
            await q.write('Import/a.txt', 'imported');
            await waitFor(() => agreed(q, 'Import/a.txt', sha256('imported')), { label: 'push' });
            const b = q.base('Import/a.txt');
            assert.strictEqual(b.docId, hub.docIdOf('Import/a.txt'));
            assert.strictEqual(b.version, hub.versionOf('Import/a.txt'));
            await waitFor(() => q.mirror.status().skips['Docs/hubonly.txt'] === 'remote-only', { label: 'remote-only skip' });
            assert.strictEqual(await q.read('Docs/hubonly.txt'), null);
        });

        test('a hub edit of a pushed file is not pulled: skipped as remote-changed, local bytes untouched', async () => {
            await hub.put('Import/a.txt', 'hub changed it');
            await q.mirror.nudge();
            await waitFor(() => q.mirror.status().skips['Import/a.txt'] === 'remote-changed', { label: 'remote-changed skip' });
            assert.strictEqual(await q.read('Import/a.txt'), 'imported');
        });
    });

    describe('bi (default)', () => {
        test('rename on the hub keeps the pair current on the device', async () => {
            const r = await setupDevice('bi');
            await hub.put('Docs/r1.txt', 'rename me');
            await r.mirror.nudge();
            await waitFor(() => agreed(r, 'Docs/r1.txt', sha256('rename me')), { label: 'pull' });
            await hub.rename('Docs/r1.txt', 'Docs/r2.txt');
            await r.mirror.nudge();
            await waitFor(() => agreed(r, 'Docs/r2.txt', sha256('rename me')) && r.localSha('Docs/r1.txt') === null, { label: 'rename-local' });
            const b = r.base('Docs/r2.txt');
            assert.strictEqual(b.docId, hub.docIdOf('Docs/r2.txt'));
            assert.strictEqual(b.version, hub.versionOf('Docs/r2.txt'));
        });
    });
});
