import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import nativeFs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { Readable } from 'node:stream';
import { createHash } from 'node:crypto';
import cacache from 'cacache';
import Stored from '../src/index.js';
import Cache from '../src/cache/index.js';
import { FakeDrive, CREDS } from './helpers/fake-drive.js';

async function setup(t, options = {}) {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'stored-cacache-ingest-'));
    const stored = new Stored({ root: path.join(root, 'stored'), ...options });
    const backend = stored.addBackend('data', { driver: 'cacache', root: path.join(root, 'blobs') });
    t.after(async () => { await stored.stop(); await fs.rm(root, { recursive: true, force: true }); });
    return { root, stored, backend };
}

test('deletion, elapsed timers and shutdown never trigger a full cache verification', async t => {
    t.mock.timers.enable({ apis: ['setInterval'] });
    t.mock.method(cacache, 'verify', async () => { throw new Error('Unexpected full-store verification'); });
    const { root, backend } = await setup(t);
    const cache = new Cache({ path: path.join(root, 'cache') });
    t.after(() => cache.stop());
    for (const store of [backend, cache]) {
        await store.put('retired', Buffer.from('keep until explicit collection'));
        await store.delete('retired');
    }
    t.mock.timers.tick(120_000);
    await backend.stop();
    await cache.stop();
    assert.equal(cacache.verify.mock.callCount(), 0);
});

test('explicit collection reclaims unreferenced blobs without scrubbing live bytes or clearing tmp', async t => {
    const { root, backend } = await setup(t);
    const cache = new Cache({ path: path.join(root, 'cache') });
    t.after(() => cache.stop());
    for (const store of [backend, cache]) {
        await store.put('a', Buffer.from('shared'));
        await store.put('b', Buffer.from('shared'));
        await store.put('dead', Buffer.from('unreferenced'));
        const live = await cacache.get.info(store.root, 'a');
        const dead = await cacache.get.info(store.root, 'dead');
        await store.delete('a');
        await store.delete('dead');
        // Collection must not decide to destroy a referenced damaged blob.
        // Recovery/integrity scrubbing is a separate, explicit operation.
        await fs.writeFile(live.path, 'damaged but still referenced');
        await fs.mkdir(path.join(store.root, 'tmp'), { recursive: true });
        await fs.writeFile(path.join(store.root, 'tmp', 'active'), 'upload');
        const result = await store.collectGarbage();
        assert.equal(result.reclaimedCount, 1);
        await assert.rejects(fs.stat(dead.path), { code: 'ENOENT' });
        assert.equal(await fs.readFile(live.path, 'utf8'), 'damaged but still referenced');
        assert.equal(await fs.readFile(path.join(store.root, 'tmp', 'active'), 'utf8'), 'upload');
        assert.ok(await cacache.get.info(store.root, 'b'));
    }
});

test('stream upload writes one payload, preserving generated key, checksums, extraction and range reads', async t => {
    let extractedPath;
    const { stored, backend } = await setup(t, {
        checksums: ['sha256', 'md5'],
        extract: async ({ file }) => {
            extractedPath = file;
            const stat = await fs.stat(file);
            return { extractedSize: stat.size };
        },
    });
    const bytes = Buffer.alloc(2 * 1024 * 1024, 0x61);
    let written = 0;
    const write = nativeFs.write;
    const writev = nativeFs.writev;
    t.mock.method(nativeFs, 'write', function (fd, buffer, offset, length, ...rest) {
        written += length;
        return write.call(this, fd, buffer, offset, length, ...rest);
    });
    t.mock.method(nativeFs, 'writev', function (fd, buffers, ...rest) {
        written += buffers.reduce((n, buffer) => n + buffer.length, 0);
        return writev.call(this, fd, buffers, ...rest);
    });
    const sha256 = createHash('sha256').update(bytes).digest('hex');
    const result = await stored.put(Readable.from([bytes.subarray(0, 8192), bytes.subarray(8192)]), { backends: ['data'] });
    assert.equal(written, bytes.length, 'upload must write the payload only once');
    assert.equal(result.id, `sha256:${sha256}`);
    assert.equal(result.key, `${sha256.slice(0, 2)}/${sha256.slice(2, 4)}/${sha256}`);
    assert.equal(result.checksums.md5, createHash('md5').update(bytes).digest('hex'));
    assert.equal(result.custom.extractedSize, bytes.length);
    assert.equal(result.size, bytes.length);
    const info = await cacache.get.info(backend.root, result.key);
    assert.equal(extractedPath, info.path);
    assert.deepEqual(await fs.readFile(info.path), bytes);
    await assert.rejects(fs.stat(backend.tempDir), { code: 'ENOENT' });
    const chunks = [];
    for await (const chunk of await backend.getRange(result.key, { start: 10, end: 99 })) chunks.push(chunk);
    assert.deepEqual(Buffer.concat(chunks), bytes.subarray(10, 100));
});

test('empty uploads and file-path inputs use the direct blob path', async t => {
    const { root, stored, backend } = await setup(t);
    const empty = await stored.put(Readable.from([]), { key: 'empty', backends: ['data'] });
    assert.equal(empty.size, 0);
    assert.deepEqual(await backend.get('empty'), Buffer.alloc(0));
    const source = path.join(root, 'source.txt');
    await fs.writeFile(source, 'from disk');
    const disk = await stored.put(source, { key: 'disk.txt', backends: ['data'] });
    assert.equal(disk.size, 9);
    assert.equal(await fs.readFile(source, 'utf8'), 'from disk');
    assert.equal((await backend.get('disk.txt')).toString(), 'from disk');
    await assert.rejects(fs.stat(backend.tempDir), { code: 'ENOENT' });
});

test('failed uploads keep the previous key and release the writer for retry', async t => {
    const { stored, backend } = await setup(t);
    await stored.put('original', { key: 'same', backends: ['data'] });
    async function* broken() { yield Buffer.from('partial'); throw new Error('upload interrupted'); }
    await assert.rejects(stored.put(Readable.from(broken()), { key: 'same', backends: ['data'] }), /upload interrupted/);
    assert.equal((await backend.get('same')).toString(), 'original');
    await stored.put(Readable.from(['replacement']), { key: 'same', backends: ['data'] });
    assert.equal((await backend.get('same')).toString(), 'replacement');
});

test('collection waits for an upload to publish its key', async t => {
    const { stored, backend } = await setup(t);
    let entered, release;
    const started = new Promise(resolve => { entered = resolve; });
    const blocked = new Promise(resolve => { release = resolve; });
    async function* slow() { yield Buffer.from('first'); entered(); await blocked; yield Buffer.from('second'); }
    const upload = stored.put(Readable.from(slow()), { key: 'live', backends: ['data'] });
    await started;
    const collection = backend.collectGarbage();
    release();
    await upload;
    await collection;
    assert.equal((await backend.get('live')).toString(), 'firstsecond');
});

test('additional writable destinations do not share the immutable blob inode', async t => {
    const { root, stored, backend } = await setup(t);
    stored.addBackend('files', { driver: 'file', root: path.join(root, 'files') });
    const result = await stored.put(Readable.from(['original']), { key: 'copy.txt', backends: ['files', 'data'] });
    assert.deepEqual(result.locations.map(location => location.backend), ['files', 'data']);
    await fs.writeFile(path.join(root, 'files', 'copy.txt'), 'edited');
    assert.equal((await backend.get('copy.txt')).toString(), 'original');
});

test('remote uploads stream directly into the transient cache and retain sync metadata', async t => {
    const { stored } = await setup(t, { replication: { autoStart: false } });
    const drive = new FakeDrive();
    stored.addBackend('drive', { driver: 'gdrive', ...CREDS, fetch: drive.fetch });
    const bytes = Buffer.from('remote stream');
    let written = 0;
    const write = nativeFs.write;
    t.mock.method(nativeFs, 'write', function (fd, buffer, offset, length, ...rest) {
        written += length;
        return write.call(this, fd, buffer, offset, length, ...rest);
    });
    const result = await stored.put(Readable.from([bytes]), { key: 'uploaded.txt', mimeType: 'text/plain', backends: ['drive'] });
    assert.equal(written, bytes.length, 'remote ingestion must not make a separate staging copy');
    const cached = await stored.cache.get(result.id);
    assert.deepEqual(cached.data, bytes);
    assert.equal(cached.metadata.key, 'uploaded.txt');
    assert.deepEqual(cached.metadata.checksums, result.checksums);
    assert.equal(cached.metadata.mimeType, 'text/plain');
    await assert.rejects(fs.stat(path.join(stored.cache.root, '.tmp')), { code: 'ENOENT' });
    await stored.replicas.drain();
    const remote = [...drive.files.values()].find(file => file.name === 'uploaded.txt');
    assert.deepEqual(remote.data, bytes);
    assert.equal((await stored.locations(result.id))[0].synced, true);
});

test('collection preserves each digest of an entry using multiple hash algorithms', async t => {
    const { root } = await setup(t);
    const cache = new Cache({ path: path.join(root, 'multi'), algorithms: ['sha256', 'sha512'] });
    t.after(() => cache.stop());
    const bytes = Buffer.from('multiple digests');
    await cache.put('multi', bytes);
    assert.equal((await cache.collectGarbage()).reclaimedCount, 0);
    for (const algorithm of ['sha256', 'sha512']) {
        const integrity = `${algorithm}-${createHash(algorithm).update(bytes).digest('base64')}`;
        assert.deepEqual(await cacache.get.byDigest(cache.root, integrity), bytes);
    }
});

test('index compaction keeps the newest value, metadata and aliases without touching blobs', async t => {
    const { root, backend } = await setup(t);
    const cache = new Cache({ path: path.join(root, 'compact') });
    t.after(() => cache.stop());
    for (const store of [cache, backend]) {
        await store.put('edited', Buffer.from('first'), { revision: 1 });
        await store.put('edited', Buffer.from('latest'), { revision: 2 });
        await store.put('alias', Buffer.from('latest'));
        await store.put('gone', Buffer.from('deleted'));
        await store.delete('gone');
        const before = await cacache.get.info(store.root, 'edited');
        const result = await store.compact();
        assert.equal(result.compactedCount, 2);
        assert.deepEqual(await cacache.get.info(store.root, 'edited'), before);
        assert.equal((await cacache.get(store.root, 'edited')).data.toString(), 'latest');
        assert.equal((await cacache.get(store.root, 'alias')).data.toString(), 'latest');
        assert.equal(await cacache.get.info(store.root, 'gone'), null);
        // Inspect the actual history, not just the visible latest value.
        const { default: index } = await import('cacache/lib/entry-index.js');
        const records = await index.bucketEntries(index.bucketPath(store.root, 'edited'));
        assert.equal(records.length, 1);
    }
});

test('an adopted file moves into the blob store without a second copy; caller checksums skip the read', async t => {
    const { root, stored } = await setup(t);
    const bytes = Buffer.from('a finished resumable upload');
    const sha256 = createHash('sha256').update(bytes).digest('hex');
    const part = path.join(root, 'upload.part');
    await fs.writeFile(part, bytes);
    const { ino } = await fs.stat(part);
    const reads = t.mock.method(nativeFs, 'createReadStream');

    const result = await stored.put(part, { backends: ['data'], adopt: true, checksums: { sha256 } });
    assert.equal(result.ok, true);
    assert.equal(result.checksums.sha256, sha256);
    assert.equal(result.size, bytes.length);
    await assert.rejects(fs.stat(part), { code: 'ENOENT' });
    const info = await cacache.get.info(path.join(root, 'blobs'), result.key);
    assert.equal((await fs.stat(info.path)).ino, ino, 'the same inode: renamed, not copied');
    assert.equal(reads.mock.calls.filter(call => call.arguments[0] === part).length, 0);
    assert.ok((await stored.get(result.id)).equals(bytes));

    // Without caller checksums the file is hashed (read once), still moved in;
    // content already stored deduplicates and the handed-over file is dropped.
    await fs.writeFile(part, bytes);
    const again = await stored.put(part, { backends: ['data'], adopt: true });
    assert.equal(again.checksums.sha256, sha256);
    await assert.rejects(fs.stat(part), { code: 'ENOENT' });
});
