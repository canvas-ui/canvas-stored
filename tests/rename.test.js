import { test, describe, before, after } from 'node:test';
import assert from 'node:assert';
import fs from 'fs-extra';
import path from 'path';
import Stored from '../src/index.js';

const TEST_DIR = './test-rename-fixtures';
const STORED_ROOT = './test-rename-stored';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function onceEvent(emitter, event, timeoutMs = 5000) {
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`timeout waiting for ${event}`)), timeoutMs);
        emitter.once(event, (payload) => { clearTimeout(timer); resolve(payload); });
    });
}

// Inode-based rename matching: same (dev, ino) at a new path is a move, not a
// delete+add — on scan it skips the rehash, on watch it pairs unlink+add into
// a single in-place rewrite (no transient absence for consumers).
describe('rename matching', async () => {
    let stored;

    before(async () => {
        await fs.ensureDir(TEST_DIR);
        stored = new Stored({ root: STORED_ROOT, checksums: ['sha256'] });
    });

    after(async () => {
        await stored.stop();
        await fs.remove(TEST_DIR);
        await fs.remove(STORED_ROOT);
    });

    test('scan reuses checksums for a renamed file (no rehash)', async () => {
        stored.addBackend('fs:ren', { driver: 'file', root: path.join(TEST_DIR, 'scan') });
        const root = path.join(TEST_DIR, 'scan');
        const oldPath = path.join(root, 'a.txt');
        const newPath = path.join(root, 'b.txt');

        // Pin mtime to a whole second so it can be restored exactly.
        const t = new Date(Math.floor(Date.now() / 1000) * 1000 - 5000);
        await fs.writeFile(oldPath, 'rename-me-content');
        await fs.utimes(oldPath, t, t);
        await stored.scan('fs:ren');

        const before = stored.index.get('fs:ren:a.txt');
        assert.ok(before, 'file indexed under its original key');
        assert.ok(before.locations[0].ino != null, 'inode recorded in location metadata');
        const id = before.id;
        const sha = before.checksums.sha256;

        // Move, then rewrite the bytes with SAME length and restore the mtime:
        // if the rescan trusted the inode match it will keep the OLD checksums —
        // which is exactly the proof that no rehash happened.
        await fs.move(oldPath, newPath);
        await fs.writeFile(newPath, 'rename-me-CONTENT'); // same length, different bytes
        await fs.utimes(newPath, t, t);
        await stored.scan('fs:ren');

        const after = stored.index.get('fs:ren:b.txt');
        assert.ok(after, 'renamed key resolves');
        assert.equal(after.id, id, 'same document identity');
        assert.equal(after.checksums.sha256, sha, 'checksums reused — file was not rehashed');
        assert.equal(stored.index.get('fs:ren:a.txt'), null, 'old key dropped');
        assert.equal(after.locations.filter((l) => l.backend === 'fs:ren').length, 1, 'single location, rewritten in place');
    });

    test('watcher pairs unlink+add into one rename (no unlink emitted)', async () => {
        const root = path.join(TEST_DIR, 'watch');
        await fs.ensureDir(root);
        const backend = stored.addBackend('fs:watch', { driver: 'file', root });
        await backend.watch();
        await sleep(300); // let chokidar settle

        const added = onceEvent(stored, 'file:add');
        await fs.writeFile(path.join(root, 'w1.txt'), 'watched content');
        const first = await added;
        assert.equal(first.key, 'w1.txt');

        const unlinks = [];
        stored.on('object:unlink', (e) => unlinks.push(e));

        const readded = onceEvent(stored, 'object:add');
        await fs.move(path.join(root, 'w1.txt'), path.join(root, 'w2.txt'));
        const renamed = await readded;

        assert.equal(renamed.key, 'w2.txt');
        assert.equal(renamed.renamedFrom, 'w1.txt', 'add is marked as the rename it is');
        assert.equal(renamed.id, first.id, 'identity survives the move');

        // Past the rename window: the held unlink must have been claimed, not fired.
        await sleep(1300);
        assert.deepEqual(unlinks, [], 'no unlink reached consumers');

        const meta = stored.index.get('fs:watch:w2.txt');
        assert.ok(meta);
        assert.equal(stored.index.get('fs:watch:w1.txt'), null, 'old key rewritten away');
        assert.equal(meta.locations.filter((l) => l.backend === 'fs:watch').length, 1);
    });

    test('a genuine delete still unlinks after the rename window', async () => {
        const root = path.join(TEST_DIR, 'watch');
        const unlinked = onceEvent(stored, 'object:unlink', 5000);
        await fs.remove(path.join(root, 'w2.txt'));
        const gone = await unlinked;
        assert.equal(gone.key, 'w2.txt');
        assert.equal(stored.index.get('fs:watch:w2.txt'), null);
    });
});
