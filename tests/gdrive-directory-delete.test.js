import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import Stored from '../src/index.js';
import { FakeDrive, CREDS } from './helpers/fake-drive.js';

async function setup(t) {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'drive-folder-delete-'));
    const stored = new Stored({ root });
    t.after(async () => { await stored.stop(); await fs.rm(root, { recursive: true, force: true }); });
    const drive = new FakeDrive();
    stored.addBackend('drive', { driver: 'gdrive', ...CREDS, fetch: drive.fetch });
    for (const key of ['Photos/a.txt', 'Photos/nested/b.txt', 'Photos-more/a.txt', 'Other/a.txt']) {
        assert.equal((await stored.writeObject('drive', key, Buffer.from('shared content'), { ifNoneMatch: '*' })).ok, true);
    }
    return { stored, drive, backend: stored.getBackend('drive') };
}

test('Drive deletes a whole native folder and only unlinks its indexed descendants', async t => {
    const { stored, drive } = await setup(t);
    const folder = [...drive.files.values()].find(file => file.name === 'Photos');
    const unlinks = [];
    stored.on('object:unlink', event => unlinks.push(event));
    assert.equal((await stored.removeDirectory('drive', 'Photos', { origin: 'backend-delete:test' })).ok, true);
    assert.equal(drive.files.get(folder.id).trashed, true, 'uses native Drive trash');
    assert.equal(stored.index.get('drive:Photos/a.txt'), null);
    assert.equal(stored.index.get('drive:Photos/nested/b.txt'), null);
    assert.ok(stored.index.get('drive:Photos-more/a.txt'));
    assert.ok(stored.index.get('drive:Other/a.txt'));
    assert.equal(unlinks.length, 2);
    assert.ok(unlinks.every(event => event.origin === 'backend-delete:test'));
    assert.equal((await stored.removeDirectory('drive', 'Photos')).ok, true, 'retry is idempotent');
    assert.equal(unlinks.length, 2, 'no repeated unlink events');
});

test('Drive folder failure and read-only configuration preserve the index', async t => {
    const { stored, backend } = await setup(t);
    backend.deleteContainer = async () => { throw new Error('permission denied'); };
    await assert.rejects(stored.removeDirectory('drive', 'Photos'), /permission denied/);
    assert.ok(stored.index.get('drive:Photos/a.txt'));
    backend.config.readOnly = true;
    await assert.rejects(stored.removeDirectory('drive', 'Photos'), /read-only/);
    assert.ok(stored.index.get('drive:Photos/nested/b.txt'));
});

test('Drive folder removal rejects backend roots and unsafe paths before deleting anything', async t => {
    const { stored, backend } = await setup(t);
    backend.deleteContainer = async () => { assert.fail('unsafe deletion reached driver'); };
    for (const key of ['', '/', '../Photos', 'Photos/../../Other']) await assert.rejects(stored.removeDirectory('drive', key), /Invalid folder key/);
    assert.ok(stored.index.get('drive:Photos/a.txt'));
});
