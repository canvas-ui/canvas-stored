import { test, describe, before, after } from 'node:test';
import assert from 'node:assert';
import fs from 'fs-extra';
import path from 'path';
import { Readable } from 'stream';
import Stored from '../src/index.js';

const TEST_DIR = './test-fixtures';
const STORED_ROOT = './test-stored';

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
    });

    // ─────────────────────────────────────────────────────────────────────────
    // put()
    // ─────────────────────────────────────────────────────────────────────────

    describe('put()', () => {
        test('stores Buffer and returns metadata', async () => {
            const meta = await stored.put(Buffer.from('test content'), { key: 'buffer.txt' });

            assert.ok(meta.id.startsWith('sha256:'));
            assert.ok(meta.checksums.sha256);
            assert.ok(meta.checksums.md5);
            assert.strictEqual(meta.size, 12);
            assert.ok(meta.mimeType);
            assert.ok(meta.locations.length > 0);
            assert.deepStrictEqual(meta.locations[0].source, {
                provider: 'fs',
                account: 'test',
                container: 'test-fixtures',
                path: 'buffer.txt',
            });
            assert.ok(meta.created);
            assert.ok(meta.modified);
        });

        test('stores string and returns metadata', async () => {
            const meta = await stored.put('string content', { key: 'string.txt' });
            assert.ok(meta.id);
            assert.strictEqual(meta.mimeType, 'text/plain');
        });

        test('stores stream and returns metadata', async () => {
            const stream = Readable.from([Buffer.from('stream '), Buffer.from('content')]);
            const meta = await stored.put(stream, { key: 'stream.txt' });
            assert.ok(meta.id);
            assert.strictEqual(meta.size, 14);
        });

        test('auto-generates key from checksum if not provided', async () => {
            const meta = await stored.put(Buffer.from('auto-key'));
            assert.ok(meta.locations[0].key.includes('/'));
        });

        test('stores custom metadata', async () => {
            const meta = await stored.put(Buffer.from('custom'), {
                key: 'custom.txt',
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
            testMeta = await stored.put(Buffer.from('get test data'), { key: 'get-test.txt' });
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

        test('returns stream when requested', async () => {
            const stream = await stored.get(testMeta.id, { stream: true });
            assert.ok(stream.pipe);
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // stat() & has()
    // ─────────────────────────────────────────────────────────────────────────

    describe('stat() & has()', () => {
        let testMeta;

        before(async () => {
            testMeta = await stored.put(Buffer.from('stat test'), { key: 'stat-test.txt' });
        });

        test('stat() returns metadata by id', () => {
            const meta = stored.stat(testMeta.id);
            assert.strictEqual(meta.id, testMeta.id);
            assert.strictEqual(meta.size, testMeta.size);
        });

        test('stat() returns null for non-existent', () => {
            const meta = stored.stat('sha256:nonexistent');
            assert.strictEqual(meta, null);
        });

        test('has() returns true for existing', () => {
            assert.strictEqual(stored.has(testMeta.id), true);
        });

        test('has() returns false for non-existent', () => {
            assert.strictEqual(stored.has('sha256:nonexistent'), false);
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

        test('lists backend files directly', async () => {
            const entries = [];
            for await (const entry of stored.list({ backend: 'fs:test' })) {
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
            const meta = await stored.put(Buffer.from('delete me'), { key: 'delete-test.txt' });
            const result = await stored.delete(meta.id);

            assert.ok(result.deleted.includes('fs:test'));
            assert.strictEqual(stored.has(meta.id), false);
        });

        test('returns empty array for non-existent', async () => {
            const result = await stored.delete('sha256:nonexistent');
            assert.deepStrictEqual(result.deleted, []);
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
            const results = await stored.scan('fs:test');
            const scanFiles = results.filter(r => r.key.startsWith('scan'));

            assert.ok(scanFiles.length >= 2);
            assert.ok(scanFiles[0].checksums);
            assert.ok(scanFiles[0].mimeType);
        });

        test('scanned files are retrievable by id', async () => {
            const results = await stored.scan('fs:test');
            const file = results.find(r => r.key === 'scan1.txt');

            if (file?.checksums) {
                const id = `sha256:${file.checksums.sha256}`;
                const meta = stored.stat(id);
                assert.ok(meta);
                assert.deepStrictEqual(meta.locations[0].source, {
                    provider: 'fs',
                    account: 'test',
                    container: 'test-fixtures',
                    path: 'scan1.txt',
                });
            }
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // Events
    // ─────────────────────────────────────────────────────────────────────────

    describe('Events', () => {
        test('emits put event', async () => {
            let emitted = false;
            stored.once('put', () => { emitted = true; });
            await stored.put(Buffer.from('event test'), { key: 'event.txt' });
            assert.strictEqual(emitted, true);
        });

        test('emits delete event', async () => {
            const meta = await stored.put(Buffer.from('delete event'), { key: 'delete-event.txt' });
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
            const meta = await stored.put(Buffer.from('persist test'), { key: 'persist.txt' });
            const id = meta.id;

            // Close and reopen
            await stored.stop();
            stored = new Stored({ root: STORED_ROOT, checksums: ['sha256', 'md5'] });
            stored.addBackend('fs:test', { driver: 'file', root: TEST_DIR });

            const persisted = stored.stat(id);
            assert.ok(persisted);
            assert.strictEqual(persisted.id, id);
        });
    });
});

