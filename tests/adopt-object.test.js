import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
import Stored from '../src/index.js';

test('writeObject adopts a handed-over file: placed by link, not copied; kept when the write is refused', async t => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'stored-adopt-'));
    const stored = new Stored({ root: path.join(root, 'stored') });
    stored.on('error', () => {});
    const home = path.join(root, 'home');
    await fs.mkdir(home);
    stored.addBackend('home', { driver: 'file', root: home, tempDir: path.join(root, 'home-tmp'), watch: false });
    t.after(async () => { await stored.stop(); await fs.rm(root, { recursive: true, force: true }); });

    const bytes = Buffer.from('a long video cut');
    const sha256 = createHash('sha256').update(bytes).digest('hex');
    const part = path.join(root, 'upload.part');
    await fs.writeFile(part, bytes);
    const { ino } = await fs.stat(part);

    const written = await stored.writeObject('home', 'video/cut.mov', part, { adopt: true, checksums: { sha256 }, ifNoneMatch: '*' });
    assert.equal(written.ok, true, JSON.stringify(written));
    assert.equal(written.sha256, sha256);
    assert.equal((await fs.stat(path.join(home, 'video/cut.mov'))).ino, ino, 'the same inode: linked, not copied');
    await assert.rejects(fs.stat(part), { code: 'ENOENT' });

    await fs.writeFile(part, bytes);
    const refused = await stored.writeObject('home', 'video/cut.mov', part, { adopt: true, ifNoneMatch: '*' });
    assert.equal(refused.ok, false);
    assert.ok((await fs.readFile(part)).equals(bytes), 'the caller keeps its bytes for a retry');
});
