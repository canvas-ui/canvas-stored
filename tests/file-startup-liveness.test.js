import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import fse from 'fs-extra';
import FileBackend from '../src/backends/file/index.js';
import Stored from '../src/index.js';

test('a pending root initialization yields, and stopping does not resurrect its watcher', async t => {
    const base = await fs.promises.mkdtemp('/tmp/stored-root-liveness-');
    const root = `${base}/files`;
    const ensureDir = fse.ensureDir;
    let release;
    const gate = new Promise(resolve => { release = resolve; });
    t.mock.method(fse, 'ensureDirSync', () => { throw new Error('Synchronous backend initialization'); });
    t.mock.method(fse, 'ensureDir', value => value === root ? gate.then(() => ensureDir(root)) : ensureDir(value));
    const backend = new FileBackend('nas', { root });
    t.after(async () => { release(); await backend.ready(); await backend.stop(); await fs.promises.rm(base, { recursive: true, force: true }); });
    let verified = false;
    const verification = backend.verifyRoot().then(value => { verified = true; return value; });
    const watching = backend.watch();
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(verified, false, 'filesystem work can wait while timers and sockets run');
    await backend.stop();
    release();
    assert.equal((await verification).ok, true);
    assert.equal(await watching, false);
    assert.equal(backend.watching, false);
});

test('external mounts are never created during asynchronous registration', async t => {
    const base = await fs.promises.mkdtemp('/tmp/stored-root-external-');
    t.after(() => fs.promises.rm(base, { recursive: true, force: true }));
    t.mock.method(fse, 'ensureDir', () => { throw new Error('External mount must not be created'); });
    const backend = new FileBackend('nas', { root: `${base}/missing`, createRoot: false });
    await backend.ready();
    assert.equal((await backend.verifyRoot()).ok, false);
    assert.equal(fs.existsSync(`${base}/missing`), false);
    await backend.stop();
});

test('path ingestion uses asynchronous stat and retains the original bytes', async t => {
    const base = await fs.promises.mkdtemp('/tmp/stored-path-liveness-');
    const stored = new Stored({ root: `${base}/stored`, replication: { autoStart: false } });
    t.after(async () => { await stored.stop(); await fs.promises.rm(base, { recursive: true, force: true }); });
    stored.addBackend('files', { driver: 'file', root: `${base}/files`, watch: false });
    const source = `${base}/source.txt`;
    await fs.promises.writeFile(source, 'Keep all source bytes');
    const statSync = fs.statSync;
    t.mock.method(fs, 'statSync', (file, ...args) => {
        assert.notEqual(file, source, 'never synchronously stat an ingestion source');
        return statSync(file, ...args);
    });
    assert.equal((await stored.put(source, { backends: ['files'], key: 'copy.txt' })).ok, true);
    assert.equal(await fs.promises.readFile(`${base}/files/copy.txt`, 'utf8'), 'Keep all source bytes');
    assert.equal(await fs.promises.readFile(source, 'utf8'), 'Keep all source bytes');
});
