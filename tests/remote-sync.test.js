import { test, describe, before, after } from 'node:test';
import assert from 'node:assert';
import fs from 'fs-extra';
import os from 'os';
import path from 'path';
import { once } from 'events';
import Stored from '../src/index.js';
import { FakeDrive, CREDS } from './helpers/fake-drive.js';

/**
 * End-to-end through Stored: a `type:'remote'` gdrive backend receives writes
 * via the cache + SyncQueue in-process lane, reads stream back from the API,
 * copy/move between a local file backend and Drive keep content identity.
 */
describe('Stored ↔ remote (gdrive) backend', () => {
    const base = path.join(os.tmpdir(), `stored-remote-${process.pid}`);
    const LOCAL = path.join(base, 'local');
    let stored;
    let drive;

    before(async () => {
        await fs.ensureDir(LOCAL);
        drive = new FakeDrive();
        stored = new Stored({ root: path.join(base, 'stored'), checksums: ['sha256'] });
        stored.addBackend('local', { driver: 'file', root: LOCAL });
        stored.addBackend('gdrive:acc', { driver: 'gdrive', ...CREDS, fetch: drive.fetch });
    });

    after(async () => {
        await stored.stop();
        await fs.remove(base);
    });

    const synced = () => once(stored, 'synced').then(([e]) => e);

    test('put() to a remote backend lands via the SyncQueue and marks the location synced', async () => {
        const waiting = synced();
        const res = await stored.put(Buffer.from('remote bytes'), { key: 'Inbox/note.txt', backends: ['gdrive:acc'] });
        assert.strictEqual(res.ok, true);
        assert.strictEqual(res.locations[0].synced, false, 'remote write is pending until the queue confirms');

        const event = await waiting;
        assert.strictEqual(event.id, res.id);
        assert.deepStrictEqual(event.results.map((r) => [r.backend, r.success]), [['gdrive:acc', true]]);

        const locations = await stored.locations(res.id);
        assert.strictEqual(locations[0].synced, true);
        assert.strictEqual(locations[0].url, 'stored://gdrive:acc/Inbox/note.txt');
        assert.match(locations[0].nativeUrl, /drive\.google\.com/);
        assert.strictEqual(locations[0].remote, true);

        const remote = [...drive.files.values()].find((f) => f.name === 'note.txt');
        assert.strictEqual(remote.data.toString(), 'remote bytes');
    });

    test('getByUrl() reads remote bytes; a cache miss falls back to the API', async () => {
        const buf = await stored.getByUrl('stored://gdrive:acc/Inbox/note.txt');
        assert.strictEqual(buf.toString(), 'remote bytes');
        const ranged = await stored.getRangeStreamByUrl('stored://gdrive:acc/Inbox/note.txt', { start: 7, end: 11 });
        assert.strictEqual(ranged.ranged, true);
        const chunks = [];
        for await (const c of ranged.stream) chunks.push(c);
        assert.strictEqual(Buffer.concat(chunks).toString(), 'bytes');
    });

    test('scan() of the remote backend indexes API-served checksums without downloads', async () => {
        drive.addFile('external.txt', 'added outside canvas');
        const before = drive.calls.length;
        const result = await stored.scan('gdrive:acc');
        assert.strictEqual(result.ok, true);
        assert.ok(result.files.some((f) => f.key === 'external.txt'));
        assert.ok(!drive.calls.slice(before).some((c) => c.path.includes('alt=media')));
        const meta = await stored.stat('gdrive:acc:external.txt');
        assert.ok(meta?.id.startsWith('sha256:'));
    });

    test('copy() local → remote adds a location under the same content id', async () => {
        const local = await stored.put(Buffer.from('shared content'), { key: 'doc.md', backends: ['local'] });
        const waiting = synced();
        const copied = await stored.copy(local.id, { to: 'gdrive:acc', key: 'Mirror/doc.md' });
        assert.strictEqual(copied.ok, true);
        await waiting;
        const locs = await stored.locations(local.id);
        assert.deepStrictEqual(locs.map((l) => [l.backend, l.synced]).sort(), [['gdrive:acc', true], ['local', true]]);
    });

    test('move() local → remote keeps the source until Drive confirms, then drops it', async () => {
        const local = await stored.put(Buffer.from('moving'), { key: 'move-me.txt', backends: ['local'] });
        const moved = once(stored, 'object:move').then(([e]) => e);
        const res = await stored.move(local.id, { to: 'gdrive:acc', key: 'Moved/move-me.txt' });
        assert.strictEqual(res.state, 'pending');
        assert.ok(await fs.pathExists(path.join(LOCAL, 'move-me.txt')), 'source survives until the sync confirms');
        const event = await moved;
        assert.strictEqual(event.id, local.id);
        assert.strictEqual(await fs.pathExists(path.join(LOCAL, 'move-me.txt')), false);
        const locs = await stored.locations(local.id);
        assert.deepStrictEqual(locs.map((l) => l.backend), ['gdrive:acc']);
    });

    test('deleteByUrl() trashes the Drive file and drops the location', async () => {
        const res = await stored.deleteByUrl('stored://gdrive:acc/Moved/move-me.txt');
        assert.deepStrictEqual(res, { ok: true });
        const f = [...drive.files.values()].find((x) => x.name === 'move-me.txt');
        assert.strictEqual(f.trashed, true);
        assert.strictEqual(await stored.getByUrl('stored://gdrive:acc/Moved/move-me.txt'), null);
    });

    test('a failed remote commit reports success:false and leaves the location unsynced', async () => {
        const bad = new FakeDrive();
        const s2 = new Stored({ root: path.join(base, 'stored2'), checksums: ['sha256'] });
        s2.addBackend('gdrive:bad', { driver: 'gdrive', ...CREDS, fetch: async (url, init) => {
            if (String(url).includes('/upload/')) return new Response(JSON.stringify({ error: { message: 'quota exceeded' } }), { status: 403 });
            return bad.fetch(url, init);
        } });
        try {
            const waiting = once(s2, 'synced').then(([e]) => e);
            const res = await s2.put(Buffer.from('x'), { key: 'x.txt', backends: ['gdrive:bad'] });
            const event = await waiting;
            assert.strictEqual(event.results[0].success, false);
            assert.match(event.results[0].error, /quota exceeded/);
            const locs = await s2.locations(res.id);
            assert.strictEqual(locs[0].synced, false);
        } finally {
            await s2.stop();
        }
    });
});
