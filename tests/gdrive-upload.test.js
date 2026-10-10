import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import Stored from '../src/index.js';
import { FakeDrive, CREDS } from './helpers/fake-drive.js';

test('Drive keyed uploads create and index bytes without watching, preserving existing names', async t => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'drive-upload-'));
    const stored = new Stored({ root });
    t.after(async () => { await stored.stop(); await fs.rm(root, { recursive: true, force: true }); });
    const drive = new FakeDrive();
    stored.addBackend('drive', { driver: 'gdrive', ...CREDS, fetch: drive.fetch });
    const result = await stored.writeObject('drive', 'folder/new.txt', Buffer.from('uploaded'), { ifNoneMatch: '*' });
    assert.equal(result.ok, true);
    assert.equal(result.size, 8);
    const file = [...drive.files.values()].find(f => f.name === 'new.txt');
    assert.equal(file.data.toString(), 'uploaded');
    assert.equal(stored.getBackend('drive').watching, false);
    assert.equal((await stored.writeObject('drive', 'folder/new.txt', Buffer.from('replace'), { ifNoneMatch: '*' })).reason, 'precondition-failed');
    assert.equal(file.data.toString(), 'uploaded');

    drive.addFile('external.txt', 'keep');
    drive.addFolder('existing-folder');
    drive.addNativeDoc('native-doc');
    for (const key of ['external.txt', 'existing-folder', 'native-doc']) {
        const refused = await stored.writeObject('drive', key, Buffer.from('replace'), { ifNoneMatch: '*' });
        assert.equal(refused.reason, 'precondition-failed', key);
    }
    const concurrent = await Promise.all(['one', 'two'].map(content =>
        stored.writeObject('drive', 'race.txt', Buffer.from(content), { ifNoneMatch: '*' })));
    assert.equal(concurrent.filter(r => r.ok).length, 1);
    assert.equal(concurrent.filter(r => r.reason === 'precondition-failed').length, 1);
    assert.equal([...drive.files.values()].filter(f => f.name === 'race.txt').length, 1);
    assert.equal((await stored.writeObject('drive', 'external.txt', Buffer.from('replace'))).reason, 'unsupported-backend');
});
