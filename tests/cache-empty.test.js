import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { createHash } from 'node:crypto';
import Cache from '../src/cache/index.js';
import CacacheBackend from '../src/backends/cacache/index.js';

for (const chunks of [[], [Buffer.alloc(0)], [Buffer.from('content')]]) {
    test(`cache streams preserve ${chunks.reduce((n, chunk) => n + chunk.length, 0)} bytes from ${chunks.length} chunks`, async t => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), 'stored-empty-cache-'));
        const cache = new Cache({ path: root });
        t.after(async () => { await cache.stop(); await fs.rm(root, { recursive: true, force: true }); });
        const bytes = Buffer.concat(chunks);
        const output = cache.putStream('file', { owner: 'document' });
        let integrity;
        output.on('integrity', value => { integrity = String(value); });
        await pipeline(Readable.from(chunks), output);
        assert.equal(integrity, `sha256-${createHash('sha256').update(bytes).digest('base64')}`);
        const stored = await cache.get('file');
        assert.deepEqual(stored.data, bytes);
        assert.equal(stored.size, bytes.length);
        assert.equal(stored.metadata.owner, 'document');
        // EOF must also release the mutation queue for subsequent operations.
        await cache.put('next', Buffer.from('next'));
        await cache.verify();
        assert.deepEqual((await cache.get('file')).data, bytes);
    });
}

test('managed backend commits and reads a zero-byte file', async t => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'stored-empty-backend-'));
    const backend = new CacacheBackend('data', { root: path.join(root, 'data') });
    t.after(async () => { await backend.stop(); await fs.rm(root, { recursive: true, force: true }); });
    const source = path.join(root, 'empty');
    await fs.writeFile(source, Buffer.alloc(0));
    assert.deepEqual(await backend.commit('empty', source), { key: 'empty', size: 0 });
    assert.deepEqual(await backend.get('empty'), Buffer.alloc(0));
    assert.equal((await backend.stat('empty')).size, 0);
    await backend.collectGarbage();
    assert.deepEqual(await backend.get('empty'), Buffer.alloc(0));
});
