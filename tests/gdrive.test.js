import { test, describe, beforeEach } from 'node:test';
import assert from 'node:assert';
import crypto from 'crypto';
import { Readable } from 'stream';
import GdriveBackend from '../src/backends/gdrive/index.js';
import { FakeDrive, CREDS } from './helpers/fake-drive.js';

const sha256 = (s) => crypto.createHash('sha256').update(s).digest('hex');
const collect = async (gen) => { const out = []; for await (const x of gen) out.push(x); return out; };

describe('GdriveBackend', () => {
    let drive;
    let backend;

    beforeEach(() => {
        drive = new FakeDrive();
        backend = new GdriveBackend('gdrive:test', { driver: 'gdrive', ...CREDS, fetch: drive.fetch });
    });

    test('is a remote, enumerable, writable backend', () => {
        assert.strictEqual(backend.type, 'remote');
        assert.strictEqual(backend.remote, true);
        assert.strictEqual(backend.transport, 'gdrive');
        assert.deepStrictEqual(backend.capabilities, { read: true, write: true, delete: true, canEnumerate: true, remote: true });
    });

    test('verifyRoot() reports auth failures and missing roots with a reason', async () => {
        drive.failNextToken = true;
        assert.deepStrictEqual((await backend.verifyRoot()).ok, false);
        assert.strictEqual((await backend.verifyRoot()).ok, true);

        const missing = new GdriveBackend('x', { ...CREDS, folderId: 'nope', fetch: drive.fetch });
        const res = await missing.verifyRoot();
        assert.strictEqual(res.ok, false);
        assert.strictEqual(res.reason, 'root-missing');
    });

    test('scan() keys files by name path, takes checksums from the API, skips native docs', async () => {
        const docs = drive.addFolder('Docs');
        const sub = drive.addFolder('2026', docs.id);
        drive.addFile('readme.txt', 'hello', drive.rootId, 'text/plain');
        drive.addFile('report.pdf', 'pdf-bytes', sub.id, 'application/pdf');
        drive.addNativeDoc('Notes', docs.id);
        const callsBefore = drive.calls.length;

        const seen = [];
        const result = await backend.scan({ algorithms: ['sha256'], onFile: (f) => seen.push(f.key) });
        assert.strictEqual(result.complete, true);
        assert.deepStrictEqual(result.files.map((f) => f.key).sort(), ['Docs/2026/report.pdf', 'readme.txt']);
        assert.deepStrictEqual(seen.sort(), ['Docs/2026/report.pdf', 'readme.txt']);
        const readme = result.files.find((f) => f.key === 'readme.txt');
        assert.deepStrictEqual(readme.checksums, { sha256: sha256('hello') });
        assert.strictEqual(readme.size, 5);
        assert.strictEqual(readme.mimeType, 'text/plain');
        assert.strictEqual(readme.backend, 'gdrive:test');
        assert.strictEqual(readme.dev, 'gdrive');
        assert.ok(readme.ino);
        // No media download happened — hashes came from metadata.
        assert.ok(!drive.calls.slice(callsBefore).some((c) => c.path.includes('alt=media')));
        assert.match(backend.nativeUrl('readme.txt'), /^https:\/\/drive\.google\.com\/file\/d\/f\d+\/view$/);
    });

    test('scan() downloads only for algorithms the API does not serve', async () => {
        drive.addFile('a.bin', 'abc');
        const result = await backend.scan({ algorithms: ['sha256', 'sha512'] });
        assert.strictEqual(result.files[0].checksums.sha512, crypto.createHash('sha512').update('abc').digest('hex'));
        assert.ok(drive.calls.some((c) => c.path.includes('alt=media')));
    });

    test('duplicate sibling names get a stable id suffix; "/" in names is escaped', async () => {
        drive.addFile('dup.txt', 'one');
        drive.addFile('dup.txt', 'two');
        drive.addFile('a/b.txt', 'slash');
        const keys = (await backend.scan()).files.map((f) => f.key).sort();
        assert.strictEqual(keys.length, 3);
        assert.ok(keys.includes('dup.txt'));
        assert.ok(keys.some((k) => /^dup \(f\d+\)\.txt$/.test(k)));
        assert.ok(keys.includes('a∕b.txt'));
    });

    test('shape() lists folders without hashing; list() honours prefix', async () => {
        const docs = drive.addFolder('Docs');
        drive.addFolder('Empty', docs.id);
        drive.addFile('x.txt', 'x', docs.id);
        drive.addFile('y.txt', 'y');
        assert.deepStrictEqual(await backend.shape(), { dirs: ['Docs', 'Docs/Empty'], files: 2 });
        assert.deepStrictEqual((await collect(backend.list({ prefix: 'Docs' }))).map((r) => r.key), ['Docs/x.txt']);
    });

    test('get()/getRange()/stat() resolve a cold key through the API', async () => {
        const docs = drive.addFolder('Docs');
        drive.addFile('hello.txt', 'hello world', docs.id, 'text/plain');
        assert.strictEqual((await backend.get('Docs/hello.txt')).toString(), 'hello world');
        const stream = await backend.get('Docs/hello.txt', { stream: true });
        assert.strictEqual(Buffer.concat(await collect(stream)).toString(), 'hello world');
        const ranged = await backend.getRange('Docs/hello.txt', { start: 6, end: 10 });
        assert.strictEqual(Buffer.concat(await collect(ranged)).toString(), 'world');
        const st = await backend.stat('Docs/hello.txt');
        assert.strictEqual(st.size, 11);
        assert.strictEqual(await backend.stat('Docs/missing.txt'), null);
        assert.strictEqual(await backend.get('nope/nope.txt'), null);
    });

    test('put() creates missing folders and uploads; a second put updates in place', async () => {
        const res = await backend.put('New/Deep/file.txt', Buffer.from('v1'), { mimeType: 'text/plain' });
        assert.strictEqual(res.key, 'New/Deep/file.txt');
        const created = drive.files.get(res.id);
        assert.strictEqual(created.data.toString(), 'v1');
        assert.strictEqual(created.mimeType, 'text/plain');
        const deep = [...drive.files.values()].find((f) => f.name === 'Deep');
        assert.deepStrictEqual(created.parents, [deep.id]);

        const again = await backend.put('New/Deep/file.txt', 'v2');
        assert.strictEqual(again.id, res.id, 'same file id — updated, not duplicated');
        assert.strictEqual(drive.files.get(res.id).data.toString(), 'v2');
        assert.strictEqual([...drive.files.values()].filter((f) => f.name === 'file.txt').length, 1);
    });

    test('put() streams a large unknown-length body in resumable chunks', async () => {
        const size = 8 * 1024 * 1024 + 12345; // > one chunk → at least two PUTs
        const payload = crypto.randomBytes(size);
        const source = Readable.from((function* () { for (let i = 0; i < payload.length; i += 65536) yield payload.subarray(i, i + 65536); })());
        const res = await backend.put('big.bin', source);
        assert.strictEqual(res.size, size);
        assert.ok(drive.files.get(res.id).data.equals(payload));
        const puts = drive.calls.filter((c) => c.method === 'PUT');
        assert.ok(puts.length >= 2, `expected chunked upload, got ${puts.length} PUT(s)`);
    });

    test('commit() uploads a file from disk with its size announced', async () => {
        const { default: fs } = await import('fs-extra');
        const { default: os } = await import('os');
        const { default: path } = await import('path');
        const tmp = path.join(os.tmpdir(), `gdrive-commit-${process.pid}.bin`);
        await fs.writeFile(tmp, 'from-disk');
        try {
            const res = await backend.commit('disk.bin', tmp);
            assert.strictEqual(drive.files.get(res.id).data.toString(), 'from-disk');
        } finally {
            await fs.remove(tmp);
        }
    });

    test('delete() trashes by default, hard-deletes when configured', async () => {
        const f = drive.addFile('gone.txt', 'x');
        assert.strictEqual(await backend.delete('gone.txt'), true);
        assert.strictEqual(drive.files.get(f.id).trashed, true);
        assert.strictEqual(await backend.delete('gone.txt'), false, 'trashed files are invisible');
        assert.strictEqual(await backend.stat('gone.txt'), null);

        const hard = new GdriveBackend('h', { ...CREDS, permanentDelete: true, fetch: drive.fetch });
        const g = drive.addFile('hard.txt', 'y');
        assert.strictEqual(await hard.delete('hard.txt'), true);
        assert.strictEqual(drive.files.has(g.id), false);
    });

    test('container ops create, rename/move and trash folders', async () => {
        await backend.createContainer('A/B');
        const b = [...drive.files.values()].find((f) => f.name === 'B');
        assert.ok(b);
        drive.addFile('in-b.txt', 'z', b.id);
        await backend.scan();

        assert.deepStrictEqual(await backend.renameContainer('A/B', 'C/D'), { from: 'A/B', to: 'C/D' });
        assert.strictEqual(drive.files.get(b.id).name, 'D');
        const c = [...drive.files.values()].find((f) => f.name === 'C');
        assert.deepStrictEqual(drive.files.get(b.id).parents, [c.id]);
        assert.ok(await backend.stat('C/D/in-b.txt'), 'cached keys follow the folder');
        assert.strictEqual(backend.nativeUrl('A/B/in-b.txt'), null);

        assert.strictEqual(await backend.deleteContainer('C'), true);
        assert.strictEqual(drive.files.get(c.id).trashed, true);
        assert.strictEqual(await backend.deleteContainer('C'), false);
    });

    test('401 triggers one token refresh and a retry', async () => {
        drive.addFile('a.txt', 'a');
        await backend.scan();
        const before = drive.tokenCalls;
        // Simulate a revoked access token: FakeDrive accepts any `tok-*`, so
        // force it by making the next request carry a stale header through a
        // wrapped fetch that rewrites it once.
        let poisoned = false;
        const wrapped = async (url, init) => {
            if (!poisoned && String(url).includes('/drive/v3/')) {
                poisoned = true;
                return drive.fetch(url, { ...init, headers: { ...init.headers, Authorization: 'Bearer stale' } });
            }
            return drive.fetch(url, init);
        };
        const b2 = new GdriveBackend('g2', { ...CREDS, fetch: wrapped });
        assert.strictEqual((await b2.get('a.txt')).toString(), 'a');
        assert.ok(drive.tokenCalls > before);
    });

    describe('watch()', () => {
        test('translates the changes feed into add/change/unlink with ino = file id', async () => {
            const docs = drive.addFolder('Docs');
            const keep = drive.addFile('keep.txt', 'k', docs.id);
            await backend.scan();
            assert.strictEqual(await backend.watch(), true);
            assert.strictEqual(backend.watching, true);
            const events = [];
            for (const ev of ['file:add', 'file:change', 'file:unlink']) backend.on(ev, (p) => events.push({ ev, ...p }));

            // Nothing changed since the token was taken.
            await backend.poll();
            assert.deepStrictEqual(events, []);

            const added = drive.addFile('new.txt', 'n', docs.id, 'text/plain');
            drive.update(keep.id, 'k2');
            await backend.poll();
            assert.deepStrictEqual(events.map((e) => [e.ev, e.key]), [['file:add', 'Docs/new.txt'], ['file:change', 'Docs/keep.txt']]);
            assert.strictEqual(events[0].ino, added.id);
            assert.deepStrictEqual(events[0].checksums, { sha256: sha256('n') });
            assert.deepStrictEqual(events[1].checksums, { sha256: sha256('k2') });
            events.length = 0;

            // Rename → unlink old + add new sharing the inode (stored pairs them).
            drive.rename(added.id, 'renamed.txt');
            await backend.poll();
            assert.deepStrictEqual(events.map((e) => [e.ev, e.key, e.ino]), [
                ['file:unlink', 'Docs/new.txt', added.id],
                ['file:add', 'Docs/renamed.txt', added.id],
            ]);
            events.length = 0;

            // Move out of the subtree root? (root is My Drive — move to a folder)
            const other = drive.addFolder('Other');
            drive.move(added.id, other.id);
            await backend.poll();
            assert.deepStrictEqual(events.map((e) => [e.ev, e.key]), [
                ['file:unlink', 'Docs/renamed.txt'],
                ['file:add', 'Other/renamed.txt'],
            ]);
            events.length = 0;

            // Trash + hard delete
            drive.trash(added.id);
            drive.remove(keep.id);
            await backend.poll();
            assert.deepStrictEqual(events.map((e) => [e.ev, e.key]), [
                ['file:unlink', 'Other/renamed.txt'],
                ['file:unlink', 'Docs/keep.txt'],
            ]);
            assert.strictEqual(backend.nativeUrl('Docs/keep.txt'), null);

            await backend.stop();
            assert.strictEqual(backend.watching, false);
        });

        test('a folder rename re-keys its subtree as unlink+add pairs', async () => {
            const docs = drive.addFolder('Docs');
            const sub = drive.addFolder('Sub', docs.id);
            const f = drive.addFile('deep.txt', 'd', sub.id);
            await backend.scan();
            await backend.watch();
            const events = [];
            for (const ev of ['file:add', 'file:unlink']) backend.on(ev, (p) => events.push([ev, p.key, p.ino]));
            drive.rename(docs.id, 'Archive');
            await backend.poll();
            assert.deepStrictEqual(events, [
                ['file:unlink', 'Docs/Sub/deep.txt', f.id],
                ['file:add', 'Archive/Sub/deep.txt', f.id],
            ]);
            assert.ok(await backend.stat('Archive/Sub/deep.txt'));
            await backend.stop();
        });

        test('files outside the configured root folder are ignored', async () => {
            const scoped = drive.addFolder('Scoped');
            const b = new GdriveBackend('s', { ...CREDS, folderId: scoped.id, fetch: drive.fetch });
            await b.scan();
            await b.watch();
            const events = [];
            b.on('file:add', (p) => events.push(p.key));
            drive.addFile('outside.txt', 'o');
            drive.addFile('inside.txt', 'i', scoped.id);
            await b.poll();
            assert.deepStrictEqual(events, ['inside.txt']);
            await b.stop();
        });
    });
});
