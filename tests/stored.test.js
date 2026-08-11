import { test, describe, before, after } from 'node:test';
import assert from 'node:assert';
import fs from 'fs-extra';
import path from 'path';
import { Readable } from 'stream';
import Stored from '../src/index.js';

const TEST_DIR = '~/Desktop/Notes';
const STORED_ROOT = '~/Desktop/Notes.stored';

describe('Stored', async () => {
    let stored;

    before(async () => {
        await fs.ensureDir(TEST_DIR);
        stored = new Stored({
            root: STORED_ROOT,
            checksums: ['sha256', 'md5'],
        });
    });

    after(async () => {
        await stored.stop();
        await fs.remove(TEST_DIR);
        await fs.remove(STORED_ROOT);
    });

    // ─────────────────────────────────────────────────────────────────────────
    // Backend Management
    // ─────────────────────────────────────────────────────────────────────────

    describe('Backend Management', () => {
        test('addBackend() registers a file backend', () => {
            const backend = stored.addBackend('fs:test', { driver: 'file', root: TEST_DIR });
            assert.strictEqual(backend.name, 'fs:test');
            assert.strictEqual(backend.type, 'local');
        });

        test('listBackends() returns registered backends', () => {
            const backends = stored.listBackends();
            assert.ok(backends.includes('fs:test'));
        });

        test('getBackend() returns backend by name', () => {
            const backend = stored.getBackend('fs:test');
            assert.strictEqual(backend.name, 'fs:test');
        });

        test('addBackend() throws on duplicate name', () => {
            assert.throws(() => stored.addBackend('fs:test', { driver: 'file', root: TEST_DIR }));
        });

        test('addBackend() throws on unknown driver', () => {
            assert.throws(() => stored.addBackend('unknown', { driver: 'unknown', root: TEST_DIR }));
        });

        test('remote backends render real native URLs', () => {
            const http = stored.addBackend('http:cdn', { driver: 'http', baseUrl: 'https://cdn.example/' });
            assert.strictEqual(http.nativeUrl('music/rec.mp3'), 'https://cdn.example/music/rec.mp3');

            const s3 = stored.addBackend('s3:prod', { driver: 's3', bucket: 'blobs' });
            assert.strictEqual(s3.nativeUrl('ab/cd/rec.mp3'), 's3://blobs/ab/cd/rec.mp3');
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // put()
    // ─────────────────────────────────────────────────────────────────────────

    describe('put()', () => {
        test('requires target backends', async () => {
            const result = await stored.put(Buffer.from('no targets'), { key: 'x.txt' });
            assert.strictEqual(result.ok, false);
            assert.strictEqual(result.reason, 'no-targets');
        });

        test('stores Buffer and returns metadata', async () => {
            const meta = await stored.put(Buffer.from('test content'), { key: 'buffer.txt', backends: ['fs:test'] });

            assert.strictEqual(meta.ok, true);
            assert.ok(meta.id.startsWith('sha256:'));
            assert.ok(meta.checksums.sha256);
            assert.ok(meta.checksums.md5);
            assert.strictEqual(meta.size, 12);
            assert.ok(meta.mimeType);
            assert.ok(meta.locations.length > 0);
            assert.deepStrictEqual(meta.locations[0].source, {
                provider: 'fs',
                account: 'test',
                container: 'Notes.stored',
                path: 'buffer.txt',
            });
            assert.ok(meta.created);
            assert.ok(meta.modified);
        });

        test('stores string and returns metadata', async () => {
            const meta = await stored.put('string content', { key: 'string.txt', backends: ['fs:test'] });
            assert.ok(meta.id);
            assert.strictEqual(meta.mimeType, 'text/plain');
        });

        test('stores stream and returns metadata', async () => {
            const stream = Readable.from([Buffer.from('stream '), Buffer.from('content')]);
            const meta = await stored.put(stream, { key: 'stream.txt', backends: ['fs:test'] });
            assert.ok(meta.id);
            assert.strictEqual(meta.size, 14);
            assert.strictEqual((await stored.get(meta.id)).toString(), 'stream content');
        });

        test('stores a filesystem path without buffering', async () => {
            const src = path.join(TEST_DIR, 'source-file.bin');
            await fs.writeFile(src, 'from disk');
            const meta = await stored.put(src, { key: 'path.bin', backends: ['fs:test'] });
            assert.strictEqual(meta.size, 9);
            assert.strictEqual((await stored.get(meta.id)).toString(), 'from disk');
        });

        test('auto-generates key from checksum if not provided', async () => {
            const meta = await stored.put(Buffer.from('auto-key'), { backends: ['fs:test'] });
            assert.ok(meta.locations[0].key.includes('/'));
        });

        test('stores custom metadata', async () => {
            const meta = await stored.put(Buffer.from('custom'), {
                key: 'custom.txt',
                backends: ['fs:test'],
                metadata: { tag: 'important' }
            });
            assert.strictEqual(meta.custom.tag, 'important');
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // get()
    // ─────────────────────────────────────────────────────────────────────────

    describe('get()', () => {
        let testMeta;

        before(async () => {
            testMeta = await stored.put(Buffer.from('get test data'), { key: 'get-test.txt', backends: ['fs:test'] });
        });

        test('retrieves data by id', async () => {
            const data = await stored.get(testMeta.id);
            assert.strictEqual(data.toString(), 'get test data');
        });

        test('retrieves data by path', async () => {
            const data = await stored.get('fs:test:get-test.txt');
            assert.strictEqual(data.toString(), 'get test data');
        });

        test('returns null for non-existent id', async () => {
            const data = await stored.get('sha256:nonexistent');
            assert.strictEqual(data, null);
        });

        test('getStream() returns a readable', async () => {
            const stream = await stored.getStream(testMeta.id);
            assert.ok(stream.pipe);
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // stat() & has()
    // ─────────────────────────────────────────────────────────────────────────

    describe('stat() & has()', () => {
        let testMeta;

        before(async () => {
            testMeta = await stored.put(Buffer.from('stat test'), { key: 'stat-test.txt', backends: ['fs:test'] });
        });

        test('stat() returns metadata by id', async () => {
            const meta = await stored.stat(testMeta.id);
            assert.strictEqual(meta.id, testMeta.id);
            assert.strictEqual(meta.size, testMeta.size);
        });

        test('stat() returns null for non-existent', async () => {
            const meta = await stored.stat('sha256:nonexistent');
            assert.strictEqual(meta, null);
        });

        test('has() returns true for existing', async () => {
            assert.strictEqual(await stored.has(testMeta.id), true);
        });

        test('has() returns false for non-existent', async () => {
            assert.strictEqual(await stored.has('sha256:nonexistent'), false);
        });

        test('locations() returns resolvable stored:// URLs', async () => {
            const locations = await stored.locations(testMeta.id);
            assert.strictEqual(locations.length, 1);
            assert.strictEqual(locations[0].url, 'stored://fs:test/stat-test.txt');
            assert.strictEqual(locations[0].backend, 'fs:test');
            assert.strictEqual(locations[0].synced, true);
            const data = await stored.getByUrl(locations[0].url);
            assert.strictEqual(data.toString(), 'stat test');
        });

        test('locations() returns [] for non-existent', async () => {
            assert.deepStrictEqual(await stored.locations('sha256:nonexistent'), []);
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // list()
    // ─────────────────────────────────────────────────────────────────────────

    describe('list()', () => {
        test('iterates all indexed entries', async () => {
            const entries = [];
            for await (const entry of stored.list()) {
                entries.push(entry);
            }
            assert.ok(entries.length > 0);
            assert.ok(entries[0].id);
        });

        test('listBackend() lists backend files directly', async () => {
            const entries = [];
            for await (const entry of stored.listBackend('fs:test')) {
                entries.push(entry);
            }
            assert.ok(entries.length > 0);
            assert.ok(entries[0].key);
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // delete()
    // ─────────────────────────────────────────────────────────────────────────

    describe('delete()', () => {
        test('deletes data and returns deleted backends', async () => {
            const meta = await stored.put(Buffer.from('delete me'), { key: 'delete-test.txt', backends: ['fs:test'] });
            const result = await stored.delete(meta.id);

            assert.strictEqual(result.ok, true);
            assert.ok(result.deleted.includes('fs:test'));
            assert.strictEqual(await stored.has(meta.id), false);
        });

        test('returns not-found for non-existent', async () => {
            const result = await stored.delete('sha256:nonexistent');
            assert.strictEqual(result.ok, false);
            assert.strictEqual(result.reason, 'not-found');
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // scan()
    // ─────────────────────────────────────────────────────────────────────────

    describe('scan()', () => {
        before(async () => {
            await fs.writeFile(path.join(TEST_DIR, 'scan1.txt'), 'scan file 1');
            await fs.writeFile(path.join(TEST_DIR, 'scan2.txt'), 'scan file 2');
        });

        test('indexes existing files', async () => {
            const { files } = await stored.scan('fs:test');
            const scanFiles = files.filter(r => r.key.startsWith('scan'));

            assert.ok(scanFiles.length >= 2);
            assert.ok(scanFiles[0].checksums);
            assert.ok(scanFiles[0].mimeType);
        });

        test('scanned files are retrievable by id', async () => {
            const { files } = await stored.scan('fs:test');
            const file = files.find(r => r.key === 'scan1.txt');

            if (file?.checksums) {
                const id = `sha256:${file.checksums.sha256}`;
                const meta = await stored.stat(id);
                assert.ok(meta);
                assert.deepStrictEqual(meta.locations[0].source, {
                    provider: 'fs',
                    account: 'test',
                    container: 'Notes.stored',
                    path: 'scan1.txt',
                });
            }
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // removeBackend()
    // ─────────────────────────────────────────────────────────────────────────

    describe('removeBackend()', () => {
        test('unregisters and prunes index entries with no surviving location', async () => {
            stored.addBackend('fs:temp', { driver: 'file', root: path.join(TEST_DIR, 'temp') });
            const meta = await stored.put(Buffer.from('temp data'), { key: 't.txt', backends: ['fs:temp'] });
            assert.strictEqual(await stored.has(meta.id), true);

            assert.strictEqual(await stored.removeBackend('fs:temp'), true);
            assert.strictEqual(stored.getBackend('fs:temp'), undefined);
            assert.strictEqual(await stored.has(meta.id), false);
        });

        test('keeps content that still lives on another backend', async () => {
            stored.addBackend('fs:a', { driver: 'file', root: path.join(TEST_DIR, 'a') });
            stored.addBackend('fs:b', { driver: 'file', root: path.join(TEST_DIR, 'b') });
            const meta = await stored.put(Buffer.from('shared content'), { key: 's.txt', backends: ['fs:a', 'fs:b'] });

            await stored.removeBackend('fs:a');

            const surviving = await stored.stat(meta.id);
            assert.ok(surviving);
            assert.deepStrictEqual(surviving.locations.map(l => l.backend), ['fs:b']);
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // Events
    // ─────────────────────────────────────────────────────────────────────────

    describe('Events', () => {
        test('emits put event', async () => {
            let emitted = false;
            stored.once('put', () => { emitted = true; });
            await stored.put(Buffer.from('event test'), { key: 'event.txt', backends: ['fs:test'] });
            assert.strictEqual(emitted, true);
        });

        test('emits delete event', async () => {
            const meta = await stored.put(Buffer.from('delete event'), { key: 'delete-event.txt', backends: ['fs:test'] });
            let emitted = false;
            stored.once('delete', () => { emitted = true; });
            await stored.delete(meta.id);
            assert.strictEqual(emitted, true);
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // Persistence
    // ─────────────────────────────────────────────────────────────────────────

    describe('Persistence', () => {
        test('index persists after reopen', async () => {
            const meta = await stored.put(Buffer.from('persist test'), { key: 'persist.txt', backends: ['fs:test'] });
            const id = meta.id;

            // Close and reopen
            await stored.stop();
            stored = new Stored({ root: STORED_ROOT, checksums: ['sha256', 'md5'] });
            stored.addBackend('fs:test', { driver: 'file', root: TEST_DIR });

            const persisted = await stored.stat(id);
            assert.ok(persisted);
            assert.strictEqual(persisted.id, id);
        });
    });
});

