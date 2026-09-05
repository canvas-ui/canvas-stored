import { test, describe, before, after } from 'node:test';
import assert from 'node:assert';
import fs from 'fs-extra';
import os from 'os';
import path from 'path';
import crypto from 'crypto';
import Stored, { Mirror } from '../src/index.js';
import { createFakeHub, sha256, waitFor, sleep } from './helpers/fake-hub.js';

/**
 * A device folder mirrored against the fake hub through the real engine:
 * file backend (chokidar) + trash + conflicts + canvas driver, Mirror on top.
 */
describe('Mirror ⇄ fake hub', { timeout: 120_000 }, () => {
    const base = path.join(os.tmpdir(), `stored-mirror-${process.pid}`);
    let hub;
    const devices = [];

    async function setupDevice(name, { mirrorOpts = {}, backendOpts = {}, start = true } = {}) {
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
            deviceId: `dev-${name}`, deviceName: name, pollInterval: 1000, retryDelays: [5, 10], ...backendOpts,
        });
        const mkMirror = (opts = {}) => {
            const m = new Mirror(stored, {
                id: 'm', local: 'local', remote: 'remote', trash: 'trash', conflicts: 'conflicts',
                deviceId: `dev-${name}`, deviceName: name, debounceMs: 100, offlineBackoffMs: [150, 150], ...mirrorOpts, ...opts,
            });
            m.on('error', () => {});
            m.events = { conflicts: [], skips: [], jobs: [], states: [] };
            m.on('conflict', (e) => m.events.conflicts.push(e));
            m.on('skip', (e) => m.events.skips.push(e));
            m.on('job:done', (j) => m.events.jobs.push(j));
            m.on('status', (s) => m.events.states.push(s.state));
            return m;
        };
        const dev = {
            name, dir, folder, trash, conflicts, stored, mirror: mkMirror(),
            file: (key) => path.join(folder, key),
            async write(key, data) { await fs.ensureDir(path.dirname(this.file(key))); await fs.writeFile(this.file(key), data); },
            async read(key) { return fs.readFile(this.file(key), 'utf8').catch(() => null); },
            localSha(key) { return stored.index.get(`local:${key}`)?.checksums?.sha256 ?? null; },
            base(key) { return this.mirror.ledger.getBase(key)?.sha256 ?? null; },
            async restart(opts = {}) { await this.mirror.stop(); this.mirror = mkMirror(opts); await this.mirror.start(); return this.mirror; },
            async close() { await this.mirror.stop().catch(() => {}); await stored.stop(); },
        };
        devices.push(dev);
        if (start) await dev.mirror.start();
        return dev;
    }

    // Converged = hub and device agree on the key and the ledger says so.
    const agreed = (dev, key, sha) => hub.sha(key) === sha && dev.localSha(key) === sha && dev.base(key) === sha;

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

    let a;

    test('start(): scans local, lists the hub, pulls what the hub has; status snapshot', async () => {
        a = await setupDevice('a');
        await waitFor(() => agreed(a, 'Docs/seed.txt', sha256('seed')), { label: 'initial pull' });
        assert.strictEqual(await a.read('Docs/seed.txt'), 'seed');
        const st = a.mirror.status();
        assert.strictEqual(st.state, 'online');
        assert.strictEqual(st.cursor, hub.stored.head());
        assert.strictEqual(st.head, hub.stored.head());
        assert.deepStrictEqual([st.pending, st.failed, st.conflicts, st.skipped], [0, 0, 0, 0]);
        assert.ok(st.lastSyncAt > 0);
        assert.strictEqual(st.lastError, null);
        const mtime = hub.stored.index.get('workspace:home:Docs/seed.txt').locations[0].mtime;
        assert.ok(Math.abs((await fs.stat(a.file('Docs/seed.txt'))).mtimeMs - mtime) < 2, 'hub mtime applied locally');
    });

    test('local add → hub (streamed PUT with If-None-Match:*, origin stamped)', async () => {
        await a.write('Docs/local.txt', 'from device a');
        await waitFor(() => agreed(a, 'Docs/local.txt', sha256('from device a')), { label: 'push' });
        const put = hub.puts('Docs/local.txt').at(-1);
        assert.strictEqual(put.headers['if-none-match'], '*');
        assert.strictEqual(put.headers['x-canvas-origin'], 'dev-a');
        assert.strictEqual(put.headers['x-canvas-sha256'], sha256('from device a'));
        const entry = hub.stored.changes({ backend: 'workspace:home', since: 0 }).changes.findLast((c) => c.key === 'Docs/local.txt');
        assert.strictEqual(entry.origin, 'dev-a');
        // Our own echo did not produce a second job.
        await a.mirror.nudge();
        await a.mirror.idle();
        assert.strictEqual(a.mirror.events.jobs.filter((j) => j.key === 'Docs/local.txt').length, 1);
    });

    test('local edit → hub replaced with If-Match: base', async () => {
        await a.write('Docs/local.txt', 'edited on a');
        await waitFor(() => agreed(a, 'Docs/local.txt', sha256('edited on a')), { label: 'push edit' });
        const put = hub.puts('Docs/local.txt').at(-1);
        assert.strictEqual(put.headers['if-match'], `"${sha256('from device a')}"`);
    });

    test('hub add / edit → device (writeObject and a raw file drop)', async () => {
        await hub.put('Docs/hub.txt', 'from hub');
        await a.mirror.nudge();
        await waitFor(() => agreed(a, 'Docs/hub.txt', sha256('from hub')), { label: 'pull add' });
        await hub.put('Docs/hub.txt', 'hub edit');
        await waitFor(() => agreed(a, 'Docs/hub.txt', sha256('hub edit')), { label: 'pull edit (poll timer)' });
        assert.strictEqual(await a.read('Docs/hub.txt'), 'hub edit');
        // A file dropped into the hub's folder reaches the feed through its watcher.
        await fs.writeFile(path.join(hub.home, 'dropped.txt'), 'dropped');
        await waitFor(() => agreed(a, 'dropped.txt', sha256('dropped')), { label: 'pull watcher put' });
    });

    test('local delete → hub DELETE with If-Match; base cleared', async () => {
        await fs.remove(a.file('Docs/hub.txt'));
        await waitFor(() => hub.sha('Docs/hub.txt') === null && a.base('Docs/hub.txt') === null, { label: 'delete propagated' });
        const del = hub.calls.findLast((c) => c.method === 'DELETE' && c.key === 'Docs/hub.txt');
        assert.strictEqual(del.headers['if-match'], `"${sha256('hub edit')}"`);
    });

    test('hub delete → local copy to trash', async () => {
        await hub.remove('dropped.txt');
        await a.mirror.nudge();
        await waitFor(() => a.localSha('dropped.txt') === null && a.base('dropped.txt') === null, { label: 'trash-local' });
        assert.strictEqual(await fs.pathExists(a.file('dropped.txt')), false);
        assert.strictEqual(await fs.readFile(path.join(a.trash, 'dropped.txt'), 'utf8'), 'dropped');
    });

    test('local rename → hub rename, no bytes re-transferred', async () => {
        const puts = hub.puts().length;
        await fs.rename(a.file('Docs/local.txt'), a.file('Docs/renamed.txt'));
        await waitFor(() => hub.sha('Docs/renamed.txt') === sha256('edited on a') && hub.sha('Docs/local.txt') === null, { label: 'rename-remote' });
        await a.mirror.idle();
        assert.strictEqual(hub.puts().length, puts, 'no PUT for a rename');
        assert.strictEqual(a.base('Docs/renamed.txt'), sha256('edited on a'));
        assert.strictEqual(a.base('Docs/local.txt'), null);
        const entry = hub.stored.changes({ backend: 'workspace:home', since: 0 }).changes.findLast((c) => c.key === 'Docs/renamed.txt');
        assert.deepStrictEqual([entry.op, entry.from, entry.origin], ['rename', 'Docs/local.txt', 'dev-a']);
    });

    test('hub rename → local rename in place (rename(2), no download)', async () => {
        const gets = () => hub.calls.filter((c) => c.method === 'GET' && c.key).length;
        const before = gets();
        const ino = (await fs.stat(a.file('Docs/renamed.txt'))).ino;
        await hub.rename('Docs/renamed.txt', 'Docs/moved.txt');
        await a.mirror.nudge();
        await waitFor(() => agreed(a, 'Docs/moved.txt', sha256('edited on a')) && a.localSha('Docs/renamed.txt') === null, { label: 'rename-local' });
        assert.strictEqual(await fs.pathExists(a.file('Docs/renamed.txt')), false);
        assert.strictEqual((await fs.stat(a.file('Docs/moved.txt'))).ino, ino, 'same inode');
        assert.strictEqual(gets(), before, 'no object GET');
        assert.strictEqual(a.base('Docs/renamed.txt'), null);
    });

    test('offline: both sides edit → inbox upload + conflicts/ copy + hub version in place', async () => {
        hub.offline = true;
        await a.write('Docs/moved.txt', 'offline edit on a');
        await waitFor(() => a.mirror.state === 'offline' && a.mirror.queue.byKey('Docs/moved.txt', 'm').some((j) => j.kind === 'push'), { label: 'offline + push queued' });
        assert.ok(a.mirror.status().pending >= 1, 'push queued while offline');
        assert.strictEqual(a.mirror.events.conflicts.length, 0);
        await hub.put('Docs/moved.txt', 'meanwhile on hub');
        hub.offline = false;

        await waitFor(() => a.mirror.events.conflicts.length === 1, { label: 'conflict', timeout: 20_000 });
        const c = a.mirror.events.conflicts[0];
        assert.strictEqual(c.key, 'Docs/moved.txt');
        assert.strictEqual(c.mode, 'inbox');
        assert.strictEqual(c.local, sha256('offline edit on a'));
        assert.strictEqual(c.base, sha256('edited on a'));
        assert.strictEqual(c.remote, sha256('meanwhile on hub'));

        await waitFor(() => agreed(a, 'Docs/moved.txt', sha256('meanwhile on hub')), { label: 'hub version in place' });
        assert.strictEqual(await a.read('Docs/moved.txt'), 'meanwhile on hub');

        const inbox = hub.inbox.find((e) => e.conflictOf === 'Docs/moved.txt');
        assert.ok(inbox, 'inbox entry');
        assert.strictEqual(inbox.mode, 'inbox');
        assert.strictEqual(inbox.sha256, sha256('offline edit on a'));
        assert.strictEqual(inbox.baseSha256, sha256('edited on a'));
        assert.strictEqual(inbox.device, 'dev-a');
        assert.strictEqual(inbox.deviceName, 'a');
        assert.strictEqual(inbox.hubSha256, sha256('meanwhile on hub'));
        assert.strictEqual(hub.sha('Docs/moved.txt'), sha256('meanwhile on hub'), 'hub version kept the name');

        const copies = await fs.readdir(path.join(a.conflicts, 'Docs'));
        assert.strictEqual(copies.length, 1);
        assert.match(copies[0], /^moved \(conflict from a \d{4}-\d{2}-\d{2} \d{4}\)\.txt$/);
        assert.strictEqual(await fs.readFile(path.join(a.conflicts, 'Docs', copies[0]), 'utf8'), 'offline edit on a');
        assert.strictEqual(a.mirror.status().conflicts, 1);
        assert.strictEqual(a.mirror.state, 'online');
        assert.ok(a.mirror.events.states.includes('offline'));
    });

    test('conflictMode rename: device version lands on the hub under the conflict-copy name', async () => {
        await a.restart({ conflictMode: 'rename' });
        hub.offline = true;
        await a.write('Docs/moved.txt', 'second offline edit');
        await waitFor(() => a.mirror.state === 'offline' && a.mirror.queue.byKey('Docs/moved.txt', 'm').some((j) => j.kind === 'push'), { label: 'offline + push queued' });
        await hub.put('Docs/moved.txt', 'hub wins again');
        hub.offline = false;
        await waitFor(() => a.mirror.events.conflicts.length === 1, { label: 'conflict (rename)', timeout: 20_000 });
        const c = a.mirror.events.conflicts[0];
        assert.strictEqual(c.mode, 'rename');
        assert.match(c.conflictKey, /^Docs\/moved \(conflict from a \d{4}-\d{2}-\d{2} \d{4}\)\.txt$/);
        await waitFor(() => agreed(a, 'Docs/moved.txt', sha256('hub wins again')), { label: 'hub version in place' });
        assert.strictEqual(hub.sha(c.conflictKey), sha256('second offline edit'), 'conflict copy is an ordinary hub object');
        const entry = hub.inbox.find((e) => e.key === c.conflictKey);
        assert.strictEqual(entry.mode, 'rename');
        assert.strictEqual(entry.conflictOf, 'Docs/moved.txt');
        const put = hub.puts(c.conflictKey).at(-1);
        assert.strictEqual(put.headers['x-canvas-conflict-mode'], 'rename');
        assert.strictEqual(put.headers['x-canvas-device-name'], 'a');
        await a.restart({});
    });

    test('restart mid-transfer: a queued push survives stop() and runs from the queue', async () => {
        hub.offline = true;
        await a.write('Docs/queued.txt', 'written while offline');
        await waitFor(() => a.mirror.state === 'offline' && a.mirror.queue.byKey('Docs/queued.txt', 'm').some((j) => j.kind === 'push'), { label: 'push queued' });
        await a.mirror.stop();
        assert.strictEqual(a.mirror.state, 'stopped');
        assert.strictEqual(hub.sha('Docs/queued.txt'), null);
        const job = a.mirror.queue.byKey('Docs/queued.txt', 'm').find((j) => j.kind === 'push');
        assert.ok(job && job.state === 'pending', 'durable in the queue');

        hub.offline = false;
        await a.restart();
        await waitFor(() => agreed(a, 'Docs/queued.txt', sha256('written while offline')), { label: 'push after restart' });
        assert.strictEqual(a.mirror.queue.byKey('Docs/queued.txt', 'm').length, 0);
    });

    test('ignore patterns and prefixes apply on both sides; invalid keys are skipped visibly', async () => {
        const p = await setupDevice('p', { mirrorOpts: { prefixes: ['Docs'], ignore: ['**/*.log'] }, backendOpts: { prefixes: ['Docs'] } });
        await waitFor(() => agreed(p, 'Docs/seed.txt', sha256('seed')), { label: 'scoped initial pull' });
        assert.strictEqual(p.localSha('Docs/queued.txt'), sha256('written while offline'));
        assert.strictEqual(p.localSha('Docs/moved.txt'), sha256('hub wins again'));

        await p.write('Other/outside.txt', 'not in scope');
        await p.write('Docs/.DS_Store', 'junk');
        await p.write('Docs/tmp.tmp', 'junk');
        await p.write('Docs/debug.log', 'junk');
        await p.write('Docs/in-scope.txt', 'yes');
        await p.write('Docs/CON', 'reserved');
        await waitFor(() => agreed(p, 'Docs/in-scope.txt', sha256('yes')), { label: 'scoped push' });
        await p.mirror.idle();
        await sleep(300);
        for (const k of ['Other/outside.txt', 'Docs/.DS_Store', 'Docs/tmp.tmp', 'Docs/debug.log', 'Docs/CON']) {
            assert.strictEqual(hub.sha(k), null, `${k} never uploaded`);
        }
        assert.deepStrictEqual(p.mirror.events.skips, [{ key: 'Docs/CON', reason: 'reserved-name' }]);
        assert.strictEqual(p.mirror.status().skipped, 1);
        assert.deepStrictEqual(p.mirror.status().skips, { 'Docs/CON': 'reserved-name' });

        await hub.put('Other/hub-outside.txt', 'hub outside');
        await hub.put('Docs/hub-inside.txt', 'hub inside');
        await p.mirror.nudge();
        await waitFor(() => agreed(p, 'Docs/hub-inside.txt', sha256('hub inside')), { label: 'scoped pull' });
        await p.mirror.idle();
        assert.strictEqual(await p.read('Other/hub-outside.txt'), null, 'out-of-prefix hub key not pulled');
        // Device a (unscoped) sees everything, including p's push.
        await waitFor(() => agreed(a, 'Other/hub-outside.txt', sha256('hub outside')) && agreed(a, 'Docs/in-scope.txt', sha256('yes')), { label: 'a converges' });
        await p.close();
        devices.splice(devices.indexOf(p), 1);
    });

    test('cursor too old → rebuild from the listing, converge, cursor := head', async () => {
        await hub.put('Docs/after-trim.txt', 'late');
        await hub.remove('Docs/in-scope.txt');
        hub.stored.trimChanges({ keep: 0, maxAgeMs: 0 });
        assert.ok(hub.stored.changes({ since: a.mirror.ledger.cursor }).cursorTooOld, 'device cursor now predates the log');
        await a.mirror.nudge();
        await waitFor(() => agreed(a, 'Docs/after-trim.txt', sha256('late')) && a.localSha('Docs/in-scope.txt') === null, { label: 'rebuild converges' });
        await a.mirror.idle();
        assert.strictEqual(a.mirror.ledger.cursor, hub.stored.head());
        assert.strictEqual(a.mirror.state, 'online');
        assert.strictEqual(await fs.readFile(path.join(a.trash, 'Docs/in-scope.txt'), 'utf8'), 'yes', 'hub delete seen through the rebuild → trash');
    });

    test('50 MB streams both ways without buffering', async () => {
        const big = Buffer.alloc(50 * 1024 * 1024);
        for (let i = 0; i < big.length; i += 4096) crypto.randomFillSync(big, i, Math.min(4096, big.length - i));
        const shaUp = sha256(big);
        const before = process.memoryUsage().rss;
        await a.write('big/up.bin', big);
        await waitFor(() => agreed(a, 'big/up.bin', shaUp), { label: '50 MB push', timeout: 60_000 });
        assert.strictEqual(sha256(await hub.read('big/up.bin')), shaUp);

        const big2 = Buffer.concat([big.subarray(1024), big.subarray(0, 1024)]);
        const shaDown = sha256(big2);
        await hub.put('big/down.bin', big2);
        await a.mirror.nudge();
        await waitFor(() => agreed(a, 'big/down.bin', shaDown), { label: '50 MB pull', timeout: 60_000 });
        assert.strictEqual(sha256(await fs.readFile(a.file('big/down.bin'))), shaDown);
        const grown = process.memoryUsage().rss - before;
        assert.ok(grown < 400 * 1024 * 1024, `rss grew ${Math.round(grown / 1e6)} MB`);
    });

    test('stop() drains and leaves the ledger consistent', async () => {
        await a.mirror.stop();
        assert.strictEqual(a.mirror.state, 'stopped');
        for (const [key, b] of a.mirror.ledger.bases()) {
            assert.strictEqual(hub.sha(key), b.sha256, `${key} base matches the hub`);
            assert.strictEqual(a.localSha(key), b.sha256, `${key} base matches the device`);
        }
    });
});
