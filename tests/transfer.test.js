import { test, describe, before, after, beforeEach } from 'node:test';
import assert from 'node:assert';
import fs from 'fs-extra';
import path from 'path';
import Stored from '../src/index.js';

const ROOT = path.resolve('./.test-transfer');
const A = path.join(ROOT, 'a');
const B = path.join(ROOT, 'b');
const BLOBS = path.join(ROOT, 'blobs');
const STORED_ROOT = path.join(ROOT, '.stored');

/** Resolve on the next matching event, or reject after `ms`. */
function nextEvent(emitter, name, ms = 2000) {
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`Timed out waiting for "${name}"`)), ms);
        emitter.once(name, (payload) => { clearTimeout(timer); resolve(payload); });
    });
}

describe('Transfer (copy / move)', async () => {
    let stored;

    before(async () => {
        await fs.remove(ROOT);
        await fs.ensureDir(A);
        await fs.ensureDir(B);
        stored = new Stored({ root: STORED_ROOT });
        stored.addBackend('fs:a', { driver: 'file', root: A });
        stored.addBackend('fs:b', { driver: 'file', root: B });
        // Not a filesystem — forces the streaming transfer path even though a
        // plain rename would do for two dirs on one disk.
        stored.addBackend('blob:store', { driver: 'cacache', root: BLOBS });
        stored.addBackend('http:cdn', { driver: 'http', baseUrl: 'https://cdn.example/' });
    });

    after(async () => {
        await stored.stop();
        await fs.remove(ROOT);
    });

    // ─────────────────────────────────────────────────────────────────────────
    // copy()
    // ─────────────────────────────────────────────────────────────────────────

    describe('copy()', () => {
        test('adds a location without changing content identity', async () => {
            const meta = await stored.put(Buffer.from('copy me'), { key: 'docs/copy.txt', backends: ['fs:a'] });

            const result = await stored.copy(meta.id, { to: 'fs:b' });

            assert.strictEqual(result.ok, true);
            assert.strictEqual(result.id, meta.id);
            assert.deepStrictEqual(result.added, ['stored://fs:b/docs/copy.txt']);
            assert.strictEqual(await fs.pathExists(path.join(B, 'docs/copy.txt')), true);
            // Source untouched, both locations on the same index entry.
            assert.strictEqual(await fs.pathExists(path.join(A, 'docs/copy.txt')), true);
            const stat = await stored.stat(meta.id);
            assert.deepStrictEqual(stat.locations.map(l => l.backend).sort(), ['fs:a', 'fs:b']);
            assert.strictEqual((await stored.getByUrl('stored://fs:b/docs/copy.txt')).toString(), 'copy me');
        });

        test('honours an explicit destination key', async () => {
            const meta = await stored.put(Buffer.from('rekeyed'), { key: 'k1.txt', backends: ['fs:a'] });
            const result = await stored.copy(meta.id, { to: 'fs:b', key: 'renamed/k2.txt' });

            assert.strictEqual(result.ok, true);
            assert.deepStrictEqual(result.added, ['stored://fs:b/renamed/k2.txt']);
            assert.strictEqual((await stored.getByUrl('stored://fs:b/renamed/k2.txt')).toString(), 'rekeyed');
        });

        test('emits object:location:add carrying the full post-mutation location set', async () => {
            const meta = await stored.put(Buffer.from('event payload'), { key: 'ev.txt', backends: ['fs:a'] });

            const eventPromise = nextEvent(stored, 'object:location:add');
            await stored.copy(meta.id, { to: 'fs:b' });
            const event = await eventPromise;

            assert.strictEqual(event.id, meta.id);
            assert.strictEqual(event.kind, 'file');
            assert.strictEqual(event.location.url, 'stored://fs:b/ev.txt');
            assert.deepStrictEqual(event.locations.map(l => l.url).sort(), [
                'stored://fs:a/ev.txt',
                'stored://fs:b/ev.txt',
            ]);
        });

        test('is a no-op when the target already holds the key', async () => {
            const meta = await stored.put(Buffer.from('already there'), { key: 'dup.txt', backends: ['fs:a', 'fs:b'] });
            const result = await stored.copy(meta.id, { to: 'fs:b' });

            assert.strictEqual(result.ok, true);
            assert.strictEqual(result.unchanged, true);
            assert.deepStrictEqual(result.added, []);
        });

        test('refuses a read-only target', async () => {
            const meta = await stored.put(Buffer.from('read only'), { key: 'ro.txt', backends: ['fs:a'] });
            const result = await stored.copy(meta.id, { to: 'http:cdn' });

            assert.strictEqual(result.ok, false);
            assert.strictEqual(result.reason, 'read-only-target');
        });

        test('refuses unknown content and unknown targets', async () => {
            assert.strictEqual((await stored.copy('sha256:deadbeef', { to: 'fs:b' })).reason, 'not-found');

            const meta = await stored.put(Buffer.from('unknown target'), { key: 'ut.txt', backends: ['fs:a'] });
            assert.strictEqual((await stored.copy(meta.id, { to: 'fs:nope' })).reason, 'unknown-backend');
            assert.strictEqual((await stored.copy(meta.id, { to: [] })).reason, 'no-targets');
        });

        test('refuses to propagate bytes that no longer match the index', async () => {
            const meta = await stored.put(Buffer.from('original bytes'), { key: 'rot.txt', backends: ['fs:a'] });
            // Rewrite behind stored's back (no watcher on these backends), the
            // way an external editor or a stale NFS handle would.
            await fs.writeFile(path.join(A, 'rot.txt'), 'tampered bytes');

            const result = await stored.copy(meta.id, { to: 'fs:b' });

            assert.strictEqual(result.ok, false);
            assert.strictEqual(result.reason, 'checksum-mismatch');
            assert.strictEqual(result.expected, meta.id);
            assert.notStrictEqual(result.actual, meta.id);
            assert.strictEqual(await fs.pathExists(path.join(B, 'rot.txt')), false);
        });

        test('refuses an offline target', async () => {
            stored.addBackend('fs:absent', {
                driver: 'file',
                root: path.join(ROOT, 'never-mounted'),
                createRoot: false,
            });
            const meta = await stored.put(Buffer.from('offline target'), { key: 'off.txt', backends: ['fs:a'] });

            const result = await stored.copy(meta.id, { to: 'fs:absent' });

            assert.strictEqual(result.ok, false);
            assert.strictEqual(result.reason, 'target-offline');
            assert.strictEqual(result.detail, 'root-missing');
            await stored.removeBackend('fs:absent');
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // move()
    // ─────────────────────────────────────────────────────────────────────────

    describe('move()', () => {
        test('relocates within one filesystem and keeps the content id', async () => {
            const meta = await stored.put(Buffer.from('move me'), { key: 'mv.txt', backends: ['fs:a'] });

            const result = await stored.move(meta.id, { to: 'fs:b' });

            assert.strictEqual(result.ok, true);
            assert.strictEqual(result.state, 'complete');
            assert.strictEqual(result.from.url, 'stored://fs:a/mv.txt');
            assert.strictEqual(result.to.url, 'stored://fs:b/mv.txt');
            assert.strictEqual(await fs.pathExists(path.join(A, 'mv.txt')), false);
            assert.strictEqual(await fs.pathExists(path.join(B, 'mv.txt')), true);

            const stat = await stored.stat(meta.id);
            assert.strictEqual(stat.id, meta.id);
            assert.deepStrictEqual(stat.locations.map(l => l.backend), ['fs:b']);
            assert.strictEqual((await stored.get(meta.id)).toString(), 'move me');
        });

        test('emits object:move, never an unlink', async () => {
            const meta = await stored.put(Buffer.from('move event'), { key: 'mv-ev.txt', backends: ['fs:a'] });

            let unlinked = false;
            const onUnlink = () => { unlinked = true; };
            stored.on('object:unlink', onUnlink);
            const eventPromise = nextEvent(stored, 'object:move');
            await stored.move(meta.id, { to: 'fs:b' });
            const event = await eventPromise;
            stored.off('object:unlink', onUnlink);

            assert.strictEqual(event.id, meta.id);
            assert.strictEqual(event.from.url, 'stored://fs:a/mv-ev.txt');
            assert.strictEqual(event.to.url, 'stored://fs:b/mv-ev.txt');
            assert.deepStrictEqual(event.locations.map(l => l.url), ['stored://fs:b/mv-ev.txt']);
            assert.strictEqual(unlinked, false);
        });

        test('streams across drivers that cannot rename, verifying content', async () => {
            const meta = await stored.put(Buffer.from('cross driver'), { key: 'cd.txt', backends: ['fs:a'] });

            const result = await stored.move(meta.id, { to: 'blob:store' });

            assert.strictEqual(result.ok, true);
            assert.strictEqual(result.state, 'complete');
            assert.strictEqual(await fs.pathExists(path.join(A, 'cd.txt')), false);
            const stat = await stored.stat(meta.id);
            assert.deepStrictEqual(stat.locations.map(l => l.backend), ['blob:store']);
            assert.strictEqual((await stored.get(meta.id)).toString(), 'cross driver');
        });

        test('refuses a multi-target move', async () => {
            const meta = await stored.put(Buffer.from('fan out'), { key: 'fan.txt', backends: ['fs:a'] });
            const result = await stored.move(meta.id, { to: ['fs:b', 'blob:store'] });

            assert.strictEqual(result.ok, false);
            assert.strictEqual(result.reason, 'move-single-target');
            // Nothing moved — the source is exactly where it was.
            assert.strictEqual(await fs.pathExists(path.join(A, 'fan.txt')), true);
        });

        test('leaves the source in place when the target is offline', async () => {
            stored.addBackend('fs:absent2', {
                driver: 'file',
                root: path.join(ROOT, 'also-never-mounted'),
                createRoot: false,
            });
            const meta = await stored.put(Buffer.from('keep me'), { key: 'keep.txt', backends: ['fs:a'] });

            const result = await stored.move(meta.id, { to: 'fs:absent2' });

            assert.strictEqual(result.ok, false);
            assert.strictEqual(result.reason, 'target-offline');
            assert.strictEqual(await fs.pathExists(path.join(A, 'keep.txt')), true);
            assert.deepStrictEqual((await stored.stat(meta.id)).locations.map(l => l.backend), ['fs:a']);
            await stored.removeBackend('fs:absent2');
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // delete() — location events
    // ─────────────────────────────────────────────────────────────────────────

    describe('delete()', () => {
        test('emits object:location:remove when other locations survive', async () => {
            const meta = await stored.put(Buffer.from('two homes'), { key: 'two.txt', backends: ['fs:a', 'fs:b'] });

            const eventPromise = nextEvent(stored, 'object:location:remove');
            await stored.delete(meta.id, { backends: ['fs:a'] });
            const event = await eventPromise;

            assert.strictEqual(event.id, meta.id);
            assert.strictEqual(event.reason, 'deleted');
            assert.strictEqual(event.location.url, 'stored://fs:a/two.txt');
            assert.deepStrictEqual(event.locations.map(l => l.url), ['stored://fs:b/two.txt']);
        });
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// Remote backends
// ─────────────────────────────────────────────────────────────────────────────

describe('Remote backends', async () => {
    const REMOTE_ROOT = path.resolve('./.test-remote');
    const NAS = path.join(REMOTE_ROOT, 'nas');
    const LOCAL = path.join(REMOTE_ROOT, 'local');
    let stored;

    before(async () => {
        await fs.remove(REMOTE_ROOT);
        await fs.ensureDir(NAS);
        await fs.ensureDir(LOCAL);
        stored = new Stored({ root: path.join(REMOTE_ROOT, '.stored') });
        stored.addBackend('fs:local', { driver: 'file', root: LOCAL });
        // Detection reads the real mount table, so a genuine network mount is
        // simulated the way an operator override would state it.
        stored.addBackend('fs:nas', { driver: 'file', root: NAS, remote: true, transport: 'cifs' });
    });

    after(async () => {
        await stored.stop();
        // Cache-on-read is fire-and-forget (see #read), so a cacache write can
        // still land while we tear down — retry rather than fail the suite on
        // an ENOTEMPTY race that says nothing about the code under test.
        for (let attempt = 0; attempt < 5; attempt += 1) {
            try { await fs.remove(REMOTE_ROOT); break; }
            catch { await new Promise(resolve => setTimeout(resolve, 50)); }
        }
    });

    test('a backend on local disk is not flagged remote', () => {
        const backend = stored.getBackend('fs:local');
        assert.strictEqual(backend.remote, false);
        assert.strictEqual(backend.capabilities.remote, false);
    });

    test('explicit config marks a mount remote and names its transport', () => {
        const backend = stored.getBackend('fs:nas');
        assert.strictEqual(backend.remote, true);
        assert.strictEqual(backend.transport, 'cifs');
        assert.strictEqual(backend.capabilities.remote, true);
        // `type` still describes the WRITE path: a network mount is written
        // synchronously like any other POSIX path.
        assert.strictEqual(backend.type, 'local');
    });

    test('locations carry remoteness for offline reasoning', async () => {
        const meta = await stored.put(Buffer.from('on the nas'), { key: 'nas.txt', backends: ['fs:nas'] });
        const [location] = await stored.locations(meta.id);

        assert.strictEqual(location.remote, true);
        assert.strictEqual(location.transport, 'cifs');
        assert.strictEqual(location.source.remote, true);
        assert.strictEqual(location.source.transport, 'cifs');
    });

    test('local locations stay sparse', async () => {
        const meta = await stored.put(Buffer.from('on local disk'), { key: 'local.txt', backends: ['fs:local'] });
        const [location] = await stored.locations(meta.id);

        assert.strictEqual(location.remote, false);
        assert.strictEqual('remote' in location.source, false);
        assert.strictEqual('transport' in location.source, false);
    });

    test('reads prefer a local location over a network one', async () => {
        const meta = await stored.put(Buffer.from('two copies'), { key: 'both.txt', backends: ['fs:nas', 'fs:local'] });
        // Diverge the bytes on disk so the read reveals which location served
        // it (stored still believes both hold the indexed content).
        await fs.writeFile(path.join(NAS, 'both.txt'), 'from the nas');
        await fs.writeFile(path.join(LOCAL, 'both.txt'), 'from local disk');
        await stored.cache.delete(meta.id).catch(() => {});

        assert.strictEqual((await stored.get(meta.id)).toString(), 'from local disk');
    });

    test('refuses to watch a network mount without explicit polling', async () => {
        const backend = stored.getBackend('fs:nas');
        const stateEvent = nextEvent(stored, 'backend:state');

        assert.strictEqual(await backend.watch(), false);
        assert.strictEqual(backend.watching, false);

        const state = await stateEvent;
        assert.strictEqual(state.reason, 'remote-watch-requires-polling');
        assert.strictEqual(state.watching, false);
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// Stale index entries
// ─────────────────────────────────────────────────────────────────────────────

describe('Stale locations', async () => {
    const ROOT2 = path.resolve('./.test-stale')
    const A2 = path.join(ROOT2, 'a')
    const B2 = path.join(ROOT2, 'b')
    let stored

    before(async () => {
        await fs.remove(ROOT2)
        await fs.ensureDir(A2)
        await fs.ensureDir(B2)
        stored = new Stored({ root: path.join(ROOT2, '.stored') })
        stored.addBackend('fs:a', { driver: 'file', root: A2 })
        stored.addBackend('fs:b', { driver: 'file', root: B2 })
    })

    after(async () => {
        await stored.stop()
        await fs.remove(ROOT2)
    })

    test('deleteByUrl forgets the location it just deleted', async () => {
        const meta = await stored.put(Buffer.from('two homes'), { key: 'd.txt', backends: ['fs:a', 'fs:b'] })

        const eventPromise = nextEvent(stored, 'object:location:remove')
        const res = await stored.deleteByUrl('stored://fs:b/d.txt')
        const event = await eventPromise

        assert.strictEqual(res.ok, true)
        assert.strictEqual(event.reason, 'deleted')
        // The index must not keep claiming bytes we removed ourselves — a later
        // copy would otherwise be skipped as "already there".
        assert.deepStrictEqual((await stored.stat(meta.id)).locations.map(l => l.backend), ['fs:a'])
    })

    test('re-copies onto a backend whose bytes vanished behind our back', async () => {
        const meta = await stored.put(Buffer.from('vanishing act'), { key: 'v.txt', backends: ['fs:a', 'fs:b'] })
        // Bytes gone, index untouched: exactly what an external delete or an
        // unmounted share leaves behind.
        await fs.remove(path.join(B2, 'v.txt'))

        const result = await stored.copy(meta.id, { to: 'fs:b' })

        assert.strictEqual(result.ok, true)
        assert.strictEqual(result.unchanged, undefined)
        assert.deepStrictEqual(result.added, ['stored://fs:b/v.txt'])
        assert.strictEqual((await stored.getByUrl('stored://fs:b/v.txt')).toString(), 'vanishing act')
    })
})

// ─────────────────────────────────────────────────────────────────────────────
// Destination-key conflicts
// ─────────────────────────────────────────────────────────────────────────────

describe('Key conflicts', async () => {
    const ROOT3 = path.resolve('./.test-conflict');
    const A3 = path.join(ROOT3, 'a');
    const B3 = path.join(ROOT3, 'b');
    let stored;

    before(async () => {
        await fs.remove(ROOT3);
        await fs.ensureDir(A3);
        await fs.ensureDir(B3);
        stored = new Stored({ root: path.join(ROOT3, '.stored') });
        stored.addBackend('fs:a', { driver: 'file', root: A3 });
        stored.addBackend('fs:b', { driver: 'file', root: B3 });
    });

    after(async () => {
        await stored.stop();
        await fs.remove(ROOT3);
    });

    test('refuses to overwrite foreign bytes by default', async () => {
        const mine = await stored.put(Buffer.from('mine'), { key: 'src.txt', backends: ['fs:a'] });
        await stored.put(Buffer.from('theirs'), { key: 'taken.txt', backends: ['fs:b'] });

        const result = await stored.copy(mine.id, { to: 'fs:b', key: 'taken.txt' });

        assert.strictEqual(result.ok, false);
        assert.strictEqual(result.reason, 'target-exists');
        // Their bytes are untouched — that is the whole point.
        assert.strictEqual((await stored.getByUrl('stored://fs:b/taken.txt')).toString(), 'theirs');
    });

    test('onConflict:rename picks the next free variant', async () => {
        const mine = await stored.put(Buffer.from('rename me'), { key: 'src2.txt', backends: ['fs:a'] });
        await stored.put(Buffer.from('occupied'), { key: 'shots/2024/07/20240712_181203.jpg', backends: ['fs:b'] });

        const result = await stored.copy(mine.id, {
            to: 'fs:b',
            key: 'shots/2024/07/20240712_181203.jpg',
            onConflict: 'rename',
        });

        assert.strictEqual(result.ok, true);
        assert.deepStrictEqual(result.added, ['stored://fs:b/shots/2024/07/20240712_181203-1.jpg']);
        assert.strictEqual((await stored.getByUrl('stored://fs:b/shots/2024/07/20240712_181203.jpg')).toString(), 'occupied');
        assert.strictEqual((await stored.getByUrl('stored://fs:b/shots/2024/07/20240712_181203-1.jpg')).toString(), 'rename me');
    });

    test('onConflict:overwrite replaces the bytes on purpose', async () => {
        const mine = await stored.put(Buffer.from('winner'), { key: 'src3.txt', backends: ['fs:a'] });
        await stored.put(Buffer.from('loser'), { key: 'clobber.txt', backends: ['fs:b'] });

        const result = await stored.copy(mine.id, { to: 'fs:b', key: 'clobber.txt', onConflict: 'overwrite' });

        assert.strictEqual(result.ok, true);
        assert.strictEqual((await stored.getByUrl('stored://fs:b/clobber.txt')).toString(), 'winner');
    });

    test('a move renames around a conflict and still releases the source', async () => {
        const mine = await stored.put(Buffer.from('moving photo'), { key: 'move-src.jpg', backends: ['fs:a'] });
        await stored.put(Buffer.from('already here'), { key: 'Fotky/2024/07/20240712_181203.jpg', backends: ['fs:b'] });

        const result = await stored.move(mine.id, {
            to: 'fs:b',
            key: 'Fotky/2024/07/20240712_181203.jpg',
            onConflict: 'rename',
        });

        assert.strictEqual(result.ok, true);
        assert.strictEqual(result.to.url, 'stored://fs:b/Fotky/2024/07/20240712_181203-1.jpg');
        assert.strictEqual(await fs.pathExists(path.join(A3, 'move-src.jpg')), false);
        assert.deepStrictEqual((await stored.stat(mine.id)).locations.map(l => l.key), ['Fotky/2024/07/20240712_181203-1.jpg']);
    });

    test('the same content on the destination key is not a conflict', async () => {
        const meta = await stored.put(Buffer.from('identical'), { key: 'same.txt', backends: ['fs:a', 'fs:b'] });
        const result = await stored.copy(meta.id, { to: 'fs:b', key: 'same.txt' });

        assert.strictEqual(result.ok, true);
        assert.strictEqual(result.unchanged, true);
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// Key normalization
// ─────────────────────────────────────────────────────────────────────────────

describe('Key normalization', async () => {
    const ROOT4 = path.resolve('./.test-keynorm');
    let stored;

    before(async () => {
        await fs.remove(ROOT4);
        await fs.ensureDir(path.join(ROOT4, 'a'));
        await fs.ensureDir(path.join(ROOT4, 'b'));
        stored = new Stored({ root: path.join(ROOT4, '.stored') });
        stored.addBackend('fs:a', { driver: 'file', root: path.join(ROOT4, 'a') });
        stored.addBackend('fs:b', { driver: 'file', root: path.join(ROOT4, 'b') });
    });

    after(async () => {
        await stored.stop();
        await fs.remove(ROOT4);
    });

    test('collapses empty segments so the index matches the filesystem', async () => {
        const meta = await stored.put(Buffer.from('normalize me'), { key: 'n.txt', backends: ['fs:a'] });
        // What a key template with an unresolved token produces.
        const result = await stored.copy(meta.id, { to: 'fs:b', key: '/Fotky///2019//x.txt' });

        assert.strictEqual(result.ok, true);
        assert.deepStrictEqual(result.added, ['stored://fs:b/Fotky/2019/x.txt']);
        assert.strictEqual(await fs.pathExists(path.join(ROOT4, 'b', 'Fotky/2019/x.txt')), true);
    });

    test('refuses a key that normalizes to nothing', async () => {
        const meta = await stored.put(Buffer.from('empty key'), { key: 'e.txt', backends: ['fs:a'] });
        assert.strictEqual((await stored.copy(meta.id, { to: 'fs:b', key: '///' })).reason, 'invalid-key');
    });
});
