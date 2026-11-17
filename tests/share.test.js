import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { setTimeout as sleep } from 'node:timers/promises';
import Stored, { ShareSource } from '../src/index.js';
import { READ_WINDOW } from '../src/backends/share/index.js';

// What the edge share unit does over HTTP, in process: poll, answer, repeat.
function pump(source, backend, { wait = 200 } = {}) {
    let running = true;
    const loop = (async () => {
        while (running) {
            const described = source.describe();
            if (!described) { await sleep(20); continue; }
            const { requests } = await backend.poll(described, { wait }).catch(() => ({ requests: [] }));
            for (const request of requests) {
                source.handle(request)
                    .then((answer) => backend.respond(request.id, answer))
                    .catch((error) => backend.respond(request.id, { error: error.message, code: error.code }));
            }
        }
    })();
    return async () => { running = false; await loop; };
}

async function until(check, ms = 5000) {
    const deadline = Date.now() + ms;
    for (;;) {
        const value = await check();
        if (value) return value;
        if (Date.now() > deadline) throw new Error('condition not met in time');
        await sleep(25);
    }
}

async function setup(t, { watch = false } = {}) {
    // The backend's timers never hold the process open; the test has to.
    const keepAlive = setInterval(() => {}, 1000);
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'stored-share-'));
    const folder = path.join(dir, 'disk');
    await fs.mkdir(path.join(folder, 'Music'), { recursive: true });
    await fs.writeFile(path.join(folder, 'Music', 'a.mp3'), 'aaaaaaaaaa');
    await fs.writeFile(path.join(folder, 'b.txt'), 'bbb');
    await fs.writeFile(path.join(folder, '.hidden'), 'secret');

    const source = new ShareSource({ root: folder, stateDir: path.join(dir, 'state'), watch, ignored: ['**/.*'] });
    source.on('error', () => {});
    await source.start();
    await source.rescan();

    const stored = new Stored({ root: path.join(dir, 'hub'), replication: { autoStart: false } });
    const backend = stored.addBackend('wd1t.home', { driver: 'share', share: 'wd1t.home', watch });
    const stop = pump(source, backend);
    t.after(async () => { await stop(); await stored.stop(); await source.stop(); await fs.rm(dir, { recursive: true, force: true }); clearInterval(keepAlive); });
    await until(() => backend.online);
    return { dir, folder, source, stored, backend, stop };
}

test('a share lists, stats and reads what its source indexed, and nothing it excluded', async (t) => {
    const { stored, backend } = await setup(t);
    const result = await stored.scan('wd1t.home');
    assert.equal(result.complete, true);
    assert.deepEqual(result.files.map(f => f.key).sort(), ['Music/a.mp3', 'b.txt']);
    assert.match(result.files[0].checksums.sha256, /^[0-9a-f]{64}$/);

    assert.equal((await stored.stat('wd1t.home:b.txt')).locations[0].backend, 'wd1t.home');
    assert.equal((await backend.get('b.txt')).toString(), 'bbb');
    assert.equal((await backend.stat('Music/a.mp3')).size, 10);
    assert.equal(await backend.get('.hidden'), null, 'an excluded file is not served');
    assert.deepEqual(await backend.shape(), { dirs: ['Music'], files: 2 });
    assert.equal(backend.nativeUrl('Music/a b.mp3'), 'file://wd1t.home/Music/a%20b.mp3');
    assert.equal(backend.capabilities.write, false);

    const ranged = await stored.getRangeStreamByUrl('stored://wd1t.home/Music/a.mp3', { start: 2, end: 5 });
    assert.equal(ranged.ranged, true);
    let bytes = '';
    for await (const chunk of ranged.stream) bytes += chunk;
    assert.equal(bytes, 'aaaa');
});

test('a file larger than one read window arrives whole', async (t) => {
    const { folder, source, stored, backend } = await setup(t);
    const big = Buffer.alloc(READ_WINDOW * 2 + 12345);
    for (let i = 0; i < big.length; i += 4096) big.writeUInt32LE(i, i);
    await fs.writeFile(path.join(folder, 'big.bin'), big);
    await source.rescan();
    await stored.scan('wd1t.home');
    assert.ok((await backend.get('big.bin')).equals(big));
    const tail = await backend.getRange('big.bin', { start: READ_WINDOW - 3 });
    const chunks = [];
    for await (const chunk of tail) chunks.push(chunk);
    assert.ok(Buffer.concat(chunks).equals(big.subarray(READ_WINDOW - 3)));
});

test('a watched share delivers adds, moves and deletes while the source is connected', async (t) => {
    const { folder, stored, backend } = await setup(t, { watch: true });
    await stored.scan('wd1t.home');
    assert.equal(backend.watching, true);

    await fs.writeFile(path.join(folder, 'new.txt'), 'new file');
    await until(() => stored.stat('wd1t.home:new.txt'), 15000);

    const id = (await stored.stat('wd1t.home:new.txt')).id;
    await fs.rename(path.join(folder, 'new.txt'), path.join(folder, 'moved.txt'));
    await until(async () => (await stored.stat('wd1t.home:moved.txt'))?.id === id && !await stored.stat('wd1t.home:new.txt'), 15000);

    await fs.rm(path.join(folder, 'b.txt'));
    await until(async () => !await stored.stat('wd1t.home:b.txt'), 15000);
});

test('an offline share keeps its catalog and fails reads', async (t) => {
    const { stored, backend, stop } = await setup(t);
    await stored.scan('wd1t.home');
    await stop();
    const states = [];
    backend.on('backend:state', (state) => states.push(state.reason));
    backend.leave();
    assert.equal(backend.online, false);
    assert.deepEqual(states, ['offline']);

    const result = await stored.scan('wd1t.home');
    assert.equal(result.complete, false);
    assert.equal(result.errors['wd1t.home'].root, 'offline');
    assert.ok(await stored.stat('wd1t.home:b.txt'), 'nothing is removed while the share is offline');
    await assert.rejects(backend.get('b.txt'), { code: 'OFFLINE' });
    assert.deepEqual(await backend.verifyRoot(), { ok: false, reason: 'offline', fsid: null });
});

test('a folder that vanished is not live and its files are not reported deleted', async (t) => {
    const { dir, folder, source, stored } = await setup(t, { watch: true });
    await stored.scan('wd1t.home');
    await fs.rename(folder, path.join(dir, 'unplugged'));
    await until(() => source.liveness({ fresh: true }).ok === false);
    assert.equal(source.describe(), null);
    await assert.rejects(source.handle({ op: 'list' }), { code: 'OFFLINE' });
    assert.deepEqual(await source.rescan(), { ok: false, reason: 'root-missing' });
    await sleep(300);
    assert.ok(await stored.stat('wd1t.home:b.txt'));

    await fs.rename(path.join(dir, 'unplugged'), folder);
    assert.equal((await source.rescan()).ok, true);
    assert.ok(source.describe());
});

test('an empty folder where an indexed one was is an unplugged drive, not a mass deletion', async (t) => {
    const { dir, folder, source, stored } = await setup(t, { watch: true });
    await stored.scan('wd1t.home');
    // What unmounting leaves: the files gone at once, an empty mountpoint in their place.
    await fs.rename(folder, path.join(dir, 'unplugged'));
    await fs.mkdir(folder);
    await sleep(2500);
    assert.deepEqual(source.liveness({ fresh: true }), { at: source.liveness().at, ok: false, reason: 'root-empty' });
    assert.deepEqual(await source.rescan(), { ok: false, reason: 'root-empty' });
    assert.equal(source.describe(), null);
    assert.ok(await stored.stat('wd1t.home:b.txt'), 'the catalog keeps the files');
    assert.ok(await stored.stat('wd1t.home:Music/a.mp3'));

    await fs.rmdir(folder);
    await fs.rename(path.join(dir, 'unplugged'), folder);
    assert.equal((await source.rescan()).count, 2);
    await stored.scan('wd1t.home');
    assert.ok(await stored.stat('wd1t.home:b.txt'));

    // The watcher is live again on the folder that came back.
    await fs.writeFile(path.join(folder, 'after-replug.txt'), 'written after the drive returned');
    await until(() => stored.stat('wd1t.home:after-replug.txt'), 15000);
});

test('a share bound to one filesystem refuses a source on another', async (t) => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'stored-share-'));
    const stored = new Stored({ root: dir, replication: { autoStart: false } });
    t.after(async () => { await stored.stop(); await fs.rm(dir, { recursive: true, force: true }); });
    const backend = stored.addBackend('red-usb.home', { driver: 'share', fsid: { id: 'uuid:aaaa' } });
    await assert.rejects(backend.poll({ epoch: 'e1', fsid: 'uuid:bbbb', head: 0 }, { wait: 0 }), { code: 'SHARE_REPLACED' });
    assert.equal(backend.online, false);
    assert.deepEqual(await backend.poll({ epoch: 'e1', fsid: 'uuid:aaaa', head: 0 }, { wait: 0 }), { requests: [] });
    assert.deepEqual(await backend.verifyRoot(), { ok: true, fsid: { id: 'uuid:aaaa' } });
});

test('a rescan asked of the backend makes the source walk its folder, and the listing follows', async (t) => {
    const { folder, source, stored, backend } = await setup(t);
    await stored.scan('wd1t.home');
    const states = [];
    backend.on('backend:state', (state) => states.push(state));
    // Not watched: only a pass over the folder finds this file.
    await fs.writeFile(path.join(folder, 'found-by-rescan.txt'), 'new');
    const walks = source.describe().walks;
    await backend.rescanSource();
    await until(() => states.some((state) => state.rescan === true));
    assert.equal(source.describe().walks, walks + 1);
    const result = await stored.scan('wd1t.home');
    assert.ok(result.files.some((file) => file.key === 'found-by-rescan.txt'));
});
