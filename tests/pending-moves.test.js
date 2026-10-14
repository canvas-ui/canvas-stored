import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import Stored from '../src/index.js';

async function setup(t) {
    const root = await fs.mkdtemp('/tmp/stored-pending-moves-');
    const stored = new Stored({ root, replication: { autoStart: false } });
    stored.on('error', () => {});
    t.after(async () => { await stored.stop(); await fs.rm(root, { recursive: true, force: true }); });
    return { stored, root, pending: stored.index.openDB('pending-moves.v1') };
}

test('an unavailable destination backlog performs no unchanged journal writes and yields to navigation', async t => {
    const { stored, pending } = await setup(t);
    stored.index.transaction(() => {
        for (let n = 0; n < 1000; n++) pending.putSync(`sha256:${n}`, [{
            sourceLocation: { backend: 'home', key: `Photos/${n}.jpg` },
            target: 'gdrive:disabled', destKey: `Archive/${n}.jpg`,
        }]);
    });
    const prototype = Object.getPrototypeOf(pending);
    const original = prototype.putSync;
    let writes = 0;
    t.mock.method(prototype, 'putSync', function (...args) {
        if (this.name === 'pending-moves.v1') writes++;
        return original.apply(this, args);
    });
    let navigationRan = false;
    setImmediate(() => { navigationRan = true; });
    const started = performance.now();
    await stored.resumePendingMoves();
    t.diagnostic(`1000 pending moves: ${(performance.now() - started).toFixed(1)} ms; ${writes} journal writes`);
    assert.equal(writes, 0, 'waiting for uploads must not synchronously rewrite the entire durable backlog');
    assert.equal(navigationRan, true, 'background recovery must yield to the event loop');
    assert.equal(pending.getKeysCount(), 1000, 'waiting operations must remain recoverable');
});

test('an unchanged recovery error is persisted once, then recovery finishes when the backend returns', async t => {
    const { stored, root, pending } = await setup(t);
    const sourceConfig = { driver: 'cacache', root: path.join(root, 'source') };
    stored.addBackend('source', sourceConfig);
    stored.addBackend('target', { driver: 'file', root: path.join(root, 'target'), watch: false });
    const file = await stored.put(Buffer.from('recoverable bytes'), { backends: ['source'] });
    await stored.copy(file.id, { from: `stored://source/${file.key}`, to: 'target', key: 'photo.jpg' });
    pending.putSync(file.id, [{ sourceLocation: { backend: 'source', key: file.key }, target: 'target', destKey: 'photo.jpg' }]);
    // Keep the acknowledged target in the index, but make source retirement unavailable.
    stored.removeBackend('source');
    const prototype = Object.getPrototypeOf(pending);
    const original = prototype.putSync;
    let writes = 0;
    t.mock.method(prototype, 'putSync', function (...args) {
        if (this.name === 'pending-moves.v1') writes++;
        return original.apply(this, args);
    });
    await stored.resumePendingMoves();
    assert.equal(writes, 1);
    assert.equal(pending.get(file.id)[0].error, 'backend-offline');
    await stored.resumePendingMoves();
    assert.equal(writes, 1, 'the same failure must not incur another synchronous commit');
    stored.addBackend('source', sourceConfig);
    await stored.resumePendingMoves();
    assert.equal(pending.get(file.id), undefined);
    assert.equal(await stored.getBackend('source').stat(file.key), null);
    assert.equal(await fs.readFile(path.join(root, 'target', 'photo.jpg'), 'utf8'), 'recoverable bytes');
});
