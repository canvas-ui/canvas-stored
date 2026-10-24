import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { openKeyValueStore, verifyDatabase } from '../src/storage/lmdb.js';

test('the store opens named sub-databases; verify walks them in a child and refuses a damaged file', async t => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'stored-lmdb-'));
    t.after(() => fs.rm(dir, { recursive: true, force: true }));
    assert.equal(await verifyDatabase(dir), null, 'no database yet');

    const db = openKeyValueStore({ path: dir, name: 'metadata' });
    const paths = db.openDB('paths');
    db.putSync('a', { size: 1 });
    paths.putSync('home:x', 'a');
    assert.deepEqual(db.get('a'), { size: 1 });
    await db.close();

    const { datasets } = await verifyDatabase(dir);
    assert.equal(datasets.metadata.count, 1);
    assert.equal(datasets.paths.count, 1);

    const file = path.join(dir, 'data.mdb');
    const handle = await fs.open(file, 'r+');
    try { await handle.write(Buffer.alloc(8192, 0xff), 0, 8192, 0); } finally { await handle.close(); }
    await assert.rejects(verifyDatabase(dir), { code: 'DATABASE_INTEGRITY_FAILED' });
});
