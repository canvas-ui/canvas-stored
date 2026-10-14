import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import Stored from '../src/index.js';

test('legacy snapshot scans yield to I/O and cancelled empty scans preserve indexed locations', async t => {
    const root = await fs.mkdtemp('/tmp/stored-scan-liveness-');
    const stored = new Stored({ root, replication: { autoStart: false } });
    t.after(async () => { await stored.stop(); await fs.rm(root, { recursive: true, force: true }); });
    stored.addBackend('files', { driver: 'file', root: `${root}/files`, watch: false });
    const backend = stored.getBackend('files');
    const rows = Array.from({ length: 300 }, (_, i) => ({ backend: 'files', key: `${i}.txt`, checksums: { sha256: i.toString(16).padStart(64, '0') }, size: 0, mimeType: 'text/plain', modified: 1 }));
    backend.scan = async () => rows;
    let heartbeat = false;
    const marker = new Promise(resolve => setImmediate(() => { heartbeat = true; resolve(); }));
    await stored.scan('files');
    assert.equal(heartbeat, true); await marker;
    let cancel = false;
    backend.scan = async () => { cancel = true; return []; };
    await assert.rejects(stored.scan('files', { isCancelled: () => cancel }), { code: 'RESYNC_CANCELLED' });
    assert.equal((await stored.stat('files:1.txt')).locations[0].key, '1.txt');

    cancel = false;
    stored.once('change', () => { cancel = true; });
    backend.scan = async () => [{ ...rows[0], key: 'new.txt', checksums: { sha256: 'f'.repeat(64) } }];
    await assert.rejects(stored.scan('files', { isCancelled: () => cancel }), { code: 'RESYNC_CANCELLED' });
    assert.equal((await stored.stat('files:1.txt')).locations[0].key, '1.txt', 'cancellation on the final row is not an absence snapshot');
});
