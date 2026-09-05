import { test, describe, before, after } from 'node:test';
import assert from 'node:assert';
import fs from 'fs-extra';
import os from 'os';
import path from 'path';
import { Readable } from 'stream';
import Stored from '../src/index.js';
import BackendManager from '../src/backends/BackendManager.js';
import CanvasBackend, { CanvasHubError } from '../src/backends/canvas/index.js';
import { createFakeHub, sha256, waitFor } from './helpers/fake-hub.js';

const collect = (emitter, name) => { const out = []; emitter.on(name, (e) => out.push(e)); return out; };

describe('canvas driver ↔ fake hub', () => {
    const base = path.join(os.tmpdir(), `stored-canvas-driver-${process.pid}`);
    let hub;
    let drv;

    before(async () => {
        await fs.remove(base);
        hub = await createFakeHub({ root: path.join(base, 'hub'), token: 'tok' });
        drv = new CanvasBackend('remote', {
            driver: 'canvas', url: hub.url, workspaceId: 'ws1', backend: 'workspace:home', token: 'tok',
            deviceId: 'dev-a', deviceName: 'laptop', instanceId: 'hub-instance-1', pollInterval: 60_000, retryDelays: [10, 20, 30],
        });
    });

    after(async () => {
        await drv.stop();
        await hub.close();
        await fs.remove(base);
    });

    test('registered as DRIVERS.canvas; type remote', () => {
        assert.ok(BackendManager.drivers().includes('canvas'));
        assert.strictEqual(drv.type, 'remote');
        assert.strictEqual(drv.remote, true);
        assert.strictEqual(drv.transport, 'canvas');
        assert.strictEqual(drv.capabilities.canEnumerate, true);
    });

    test('verifyRoot pings (cached 30 s) and records the instance id', async () => {
        const live = await drv.verifyRoot();
        assert.deepStrictEqual(live, { ok: true, fsid: null, instanceId: 'hub-instance-1' });
        const pings = () => hub.calls.filter((c) => c.path === '/rest/v2/ping').length;
        const n = pings();
        await drv.verifyRoot();
        assert.strictEqual(pings(), n, 'second verifyRoot inside the cache window does not ping');
        assert.strictEqual(drv.dev, 'canvas:hub-instance-1');
    });

    test('instance mismatch is refused', async () => {
        const other = new CanvasBackend('r2', { url: hub.url, workspaceId: 'ws1', token: 'tok', instanceId: 'someone-else' });
        const live = await other.verifyRoot();
        assert.strictEqual(live.ok, false);
        assert.strictEqual(live.reason, 'instance-mismatch');
    });

    test('putStream (Buffer, If-None-Match:*) → created; stat/get/getRange read it back', async () => {
        const res = await drv.putStream('Docs/a.txt', Buffer.from('hello hub'), { ifNoneMatch: '*', sha256: sha256('hello hub'), mtime: 1700000000000, origin: 'dev-a', mimeType: 'text/plain' });
        assert.strictEqual(res.created, true);
        assert.strictEqual(res.sha256, sha256('hello hub'));
        assert.strictEqual(res.size, 9);
        assert.strictEqual(res.mtime, 1700000000000);
        assert.ok(res.seq > 0);
        assert.strictEqual(hub.sha('Docs/a.txt'), sha256('hello hub'));

        const st = await drv.stat('Docs/a.txt');
        assert.strictEqual(st.checksums.sha256, sha256('hello hub'));
        assert.strictEqual(st.size, 9);
        assert.strictEqual(st.modified, 1700000000000);
        assert.strictEqual(st.ino, sha256('hello hub'));
        assert.strictEqual(st.dev, 'canvas:hub-instance-1');
        assert.strictEqual(await drv.stat('Docs/missing.txt'), null);

        assert.strictEqual((await drv.get('Docs/a.txt')).toString(), 'hello hub');
        const stream = await drv.get('Docs/a.txt', { stream: true });
        let text = ''; for await (const c of stream) text += c;
        assert.strictEqual(text, 'hello hub');
        const ranged = await drv.getRange('Docs/a.txt', { start: 6, end: 8 });
        let part = ''; for await (const c of ranged) part += c;
        assert.strictEqual(part, 'hub');
        assert.strictEqual(await drv.get('nope'), null);
    });

    test('wrong If-Match → PRECONDITION_FAILED with current; If-None-Match on an occupied key too', async () => {
        await assert.rejects(
            drv.putStream('Docs/a.txt', Buffer.from('v2'), { ifMatch: 'deadbeef' }),
            (err) => err instanceof CanvasHubError && err.code === 'PRECONDITION_FAILED' && err.status === 412 && err.current.sha256 === sha256('hello hub') && err.current.size === 9,
        );
        await assert.rejects(drv.putStream('Docs/a.txt', Buffer.from('v2'), { ifNoneMatch: '*' }), (err) => err.code === 'PRECONDITION_FAILED');
        // Right If-Match: replaced, previous reported.
        const res = await drv.putStream('Docs/a.txt', Readable.from([Buffer.from('v2')]), { ifMatch: sha256('hello hub'), sha256: sha256('v2') });
        assert.strictEqual(res.created, false);
        assert.strictEqual(res.previous.sha256, sha256('hello hub'));
        assert.strictEqual(hub.sha('Docs/a.txt'), sha256('v2'));
        // Same bytes again: unchanged.
        const same = await drv.putStream('Docs/a.txt', Buffer.from('v2'), { ifMatch: sha256('v2') });
        assert.strictEqual(same.unchanged, true);
    });

    test('checksum mismatch is refused (422 → REFUSED) and nothing is committed', async () => {
        await assert.rejects(drv.putStream('Docs/bad.txt', Buffer.from('xyz'), { sha256: sha256('other') }), (err) => err.code === 'REFUSED' && err.status === 422 && err.hubCode === 'CHECKSUM_MISMATCH');
        assert.strictEqual(hub.sha('Docs/bad.txt'), null);
    });

    test('list()/scan() page the listing with sha256 rows; scan sets the cursor to the head', async () => {
        await drv.putStream('Docs/b.txt', 'bee', { ifNoneMatch: '*' });
        await drv.putStream('Other/c.txt', 'sea', { ifNoneMatch: '*' });
        const rows = []; for await (const r of drv.list()) rows.push(r);
        assert.deepStrictEqual(rows.map((r) => r.key), ['Docs/a.txt', 'Docs/b.txt', 'Other/c.txt']);
        assert.strictEqual(rows[1].checksums.sha256, sha256('bee'));
        const scoped = []; for await (const r of drv.list({ prefix: 'Docs' })) scoped.push(r.key);
        assert.deepStrictEqual(scoped, ['Docs/a.txt', 'Docs/b.txt']);

        const scan = await drv.scan();
        assert.strictEqual(scan.complete, true);
        assert.strictEqual(scan.files.length, 3);
        assert.strictEqual(scan.files[0].backend, 'remote');
        assert.strictEqual(drv.cursor, hub.stored.head());
        assert.strictEqual(drv.head, hub.stored.head());

        const scoped2 = new CanvasBackend('r3', { url: hub.url, workspaceId: 'ws1', token: 'tok', prefixes: ['Docs'] });
        const s2 = await scoped2.scan();
        assert.deepStrictEqual(s2.files.map((f) => f.key), ['Docs/a.txt', 'Docs/b.txt']);
    });

    test('rename() is one request, no bytes; delete() honours If-Match', async () => {
        const puts = hub.puts().length;
        const r = await drv.rename('Docs/b.txt', 'Docs/b2.txt', { ifMatch: sha256('bee') });
        assert.strictEqual(r.sha256, sha256('bee'));
        assert.ok(r.seq > 0);
        assert.strictEqual(hub.puts().length, puts, 'no PUT for a rename');
        assert.strictEqual(hub.sha('Docs/b.txt'), null);
        assert.strictEqual(hub.sha('Docs/b2.txt'), sha256('bee'));
        await assert.rejects(drv.rename('Docs/b2.txt', 'Docs/a.txt'), (err) => err.code === 'TARGET_EXISTS');
        await assert.rejects(drv.rename('Docs/b2.txt', 'Docs/b3.txt', { ifMatch: 'ffff' }), (err) => err.code === 'PRECONDITION_FAILED');

        await assert.rejects(drv.delete('Docs/b2.txt', { ifMatch: 'ffff' }), (err) => err.code === 'PRECONDITION_FAILED');
        assert.strictEqual(await drv.delete('Docs/b2.txt', { ifMatch: sha256('bee') }), true);
        assert.strictEqual(await drv.delete('Docs/b2.txt'), false, 'already gone');
        assert.strictEqual(hub.sha('Docs/b2.txt'), null);
    });

    test('changes() tails the feed; poll() synthesizes watcher events, own-origin entries are silent', async () => {
        await drv.poll();                       // drain what earlier tests logged
        const since = drv.cursor;
        const adds = collect(drv, 'file:add');
        const changes = collect(drv, 'file:change');
        const unlinks = collect(drv, 'file:unlink');
        const states = collect(drv, 'backend:state');

        // Another device / the hub itself:
        await hub.put('Docs/new.txt', 'from hub');
        await hub.put('Docs/a.txt', 'a3');
        await hub.rename('Other/c.txt', 'Other/c-renamed.txt');
        await hub.remove('Docs/new.txt');
        // Us:
        await drv.putStream('Docs/mine.txt', 'mine', { ifNoneMatch: '*', origin: 'dev-a' });

        const feed = await drv.changes(since, 100);
        assert.ok(feed.changes.length >= 5);
        assert.deepStrictEqual(feed.changes.map((c) => c.op), ['put', 'put', 'rename', 'delete', 'put']);
        assert.strictEqual(feed.changes[2].from, 'Other/c.txt');
        assert.strictEqual(feed.changes[4].origin, 'dev-a');

        const r = await drv.poll();
        assert.strictEqual(r.ok, true);
        assert.strictEqual(r.applied, 5);
        assert.strictEqual(drv.cursor, feed.head);
        assert.strictEqual(states.at(-1).reason, 'polled');
        assert.strictEqual(states.at(-1).cursor, feed.head);

        assert.deepStrictEqual(adds.map((e) => e.key), ['Docs/new.txt', 'Other/c-renamed.txt'], 'add for the hub put + the rename target; nothing for our own put');
        assert.deepStrictEqual(changes.map((e) => e.key), ['Docs/a.txt']);
        assert.strictEqual(changes[0].checksums.sha256, sha256('a3'));
        assert.deepStrictEqual(unlinks.map((e) => e.key), ['Other/c.txt', 'Docs/new.txt']);
        const renamedAdd = adds.find((e) => e.key === 'Other/c-renamed.txt');
        const renamedUnlink = unlinks.find((e) => e.key === 'Other/c.txt');
        assert.strictEqual(renamedAdd.ino, renamedUnlink.ino, 'rename pair shares the ino (sha256)');
        assert.strictEqual(renamedAdd.ino, sha256('sea'));
        assert.strictEqual((await drv.poll()).applied, 0, 'caught up');
    });

    test('410 → cursor cleared + backend:state cursor-too-old', async () => {
        const states = collect(drv, 'backend:state');
        await hub.put('Docs/trim.txt', 'x');
        hub.stored.trimChanges({ keep: 0, maxAgeMs: 0 });
        const r = await drv.poll();
        assert.strictEqual(r.ok, false);
        assert.strictEqual(r.reason, 'cursor-too-old');
        assert.strictEqual(drv.cursor, null);
        assert.strictEqual(states.at(-1).reason, 'cursor-too-old');
        assert.strictEqual((await drv.poll()).reason, 'no-cursor');
        await assert.rejects(drv.changes(1, 10), (err) => err.code === 'CURSOR_TOO_OLD' && err.head === hub.stored.head());
        // A scan re-establishes the cursor at the head.
        await drv.scan();
        assert.strictEqual(drv.cursor, hub.stored.head());
    });

    test('watch() polls on an interval and stop() ends it', async () => {
        const fast = new CanvasBackend('r4', { url: hub.url, workspaceId: 'ws1', token: 'tok', deviceId: 'dev-b', pollInterval: 1000, retryDelays: [5] });
        const adds = collect(fast, 'file:add');
        assert.strictEqual(await fast.watch(), true);
        assert.strictEqual(fast.cursor, hub.stored.head(), 'no cursor → start at the head');
        await hub.put('Docs/watched.txt', 'w');
        await waitFor(() => adds.length === 1, { label: 'watch add' });
        assert.strictEqual(adds[0].key, 'Docs/watched.txt');
        await fast.stop();
        assert.strictEqual(fast.watching, false);
    });

    test('429/5xx are retried; transport failure is OFFLINE; 401 is UNAUTHORIZED', async () => {
        hub.failNext.push(503, 429);
        const st = await drv.stat('Docs/a.txt');
        assert.strictEqual(st.checksums.sha256, sha256('a3'), 'succeeded after two retries');

        hub.failNext.push(500, 500, 500, 500);
        await assert.rejects(drv.stat('Docs/a.txt'), (err) => err.code === 'RETRYABLE' && err.status === 500);
        hub.failNext.length = 0;

        hub.offline = true;
        await assert.rejects(drv.stat('Docs/a.txt'), (err) => err.code === 'OFFLINE' && err.offline === true);
        const live = await drv.verifyRoot();
        assert.strictEqual(live.ok, false);
        assert.strictEqual(live.reason, 'offline');
        hub.offline = false;

        const bad = new CanvasBackend('r5', { url: hub.url, workspaceId: 'ws1', token: 'wrong', retryDelays: [5] });
        await assert.rejects(bad.stat('Docs/a.txt'), (err) => err.code === 'UNAUTHORIZED' && err.permanent === true);
    });

    test('Stored.copy() streams straight to the hub (no cache, no SyncQueue) and surfaces 412 as precondition-failed', async () => {
        const local = path.join(base, 'local');
        await fs.ensureDir(local);
        const stored = new Stored({ root: path.join(base, 'stored'), checksums: ['sha256'] });
        stored.on('error', () => {});
        stored.addBackend('local', { driver: 'file', root: local });
        stored.addBackend('hub', { driver: 'canvas', url: hub.url, workspaceId: 'ws1', backend: 'workspace:home', token: 'tok', deviceId: 'dev-a', retryDelays: [5] });
        const synced = collect(stored, 'synced');
        try {
            await fs.writeFile(path.join(local, 'push.txt'), 'pushed via copy');
            await stored.scan('local');
            const res = await stored.copy('local:push.txt', { to: 'hub', key: 'Docs/push.txt', from: 'local', ifNoneMatch: '*', origin: 'dev-a', mtime: 1700000001000 });
            assert.strictEqual(res.ok, true, JSON.stringify(res));
            assert.deepStrictEqual(res.added, ['stored://hub/Docs/push.txt']);
            assert.strictEqual(res.remote.created, true);
            assert.strictEqual(hub.sha('Docs/push.txt'), sha256('pushed via copy'));
            const loc = (await stored.locations('local:push.txt')).find((l) => l.backend === 'hub');
            assert.strictEqual(loc.synced, true, 'direct write: no pending sync');
            assert.strictEqual(loc.remote, true);
            assert.strictEqual(synced.length, 0, 'no SyncQueue involvement');
            assert.strictEqual(await stored.cache.getInfo(res.id).catch(() => null), null, 'nothing staged in the cache');
            const meta = stored.index.get('hub:Docs/push.txt');
            assert.strictEqual(meta.locations.find((l) => l.backend === 'hub').ino, sha256('pushed via copy'));
            assert.strictEqual(meta.locations.find((l) => l.backend === 'hub').mtime, 1700000001000);

            // Hub moved on; our If-Match is stale.
            await hub.put('Docs/push.txt', 'hub edit');
            await fs.writeFile(path.join(local, 'push.txt'), 'local edit');
            await stored.scan('local');
            const stale = await stored.copy('local:push.txt', { to: 'hub', key: 'Docs/push.txt', from: 'local', ifMatch: sha256('pushed via copy'), onConflict: 'overwrite' });
            assert.strictEqual(stale.ok, false);
            assert.strictEqual(stale.reason, 'precondition-failed');
            assert.strictEqual(stale.code, 'PRECONDITION_FAILED');
            assert.strictEqual(stale.current.sha256, sha256('hub edit'));
            assert.strictEqual(hub.sha('Docs/push.txt'), sha256('hub edit'), 'nothing overwritten');

            // Pull the hub version over the local one (overwrite = succession).
            await stored.scan('hub');
            const pulled = await stored.copy('hub:Docs/push.txt', { to: 'local', key: 'push.txt', from: 'hub', onConflict: 'overwrite' });
            assert.strictEqual(pulled.ok, true, JSON.stringify(pulled));
            assert.strictEqual(await fs.readFile(path.join(local, 'push.txt'), 'utf8'), 'hub edit');

            // Same-backend move on the hub = server-side rename, no bytes.
            const puts = hub.puts().length;
            const moved = await stored.renameObject('hub', 'Docs/push.txt', 'Docs/pushed.txt', { ifMatch: sha256('hub edit'), origin: 'dev-a' });
            assert.strictEqual(moved.ok, true, JSON.stringify(moved));
            assert.strictEqual(hub.puts().length, puts);
            assert.strictEqual(hub.sha('Docs/pushed.txt'), sha256('hub edit'));
            assert.ok(stored.index.get('hub:Docs/pushed.txt'));
            assert.strictEqual(stored.index.get('hub:Docs/push.txt'), null);

            // Hub delete through removeObject forwards If-Match.
            await assert.rejects(stored.removeObject('hub', 'Docs/pushed.txt', { ifMatch: 'ffff' }).then((r) => { if (!r.ok) throw Object.assign(new Error(r.reason), { code: r.code }); }), (err) => err.code === 'PRECONDITION_FAILED');
            const removed = await stored.removeObject('hub', 'Docs/pushed.txt', { ifMatch: sha256('hub edit'), origin: 'dev-a' });
            assert.strictEqual(removed.ok, true);
            assert.strictEqual(hub.sha('Docs/pushed.txt'), null);
        } finally {
            await stored.stop();
        }
    });
});
