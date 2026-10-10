import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import { Readable } from 'node:stream';
import Stored from '../src/index.js';
import GdriveBackend from '../src/backends/gdrive/index.js';
import { FakeDrive, CREDS } from './helpers/fake-drive.js';

test('real fetch treats Drive 308 with Location as a chunk acknowledgement', async t => {
    const drive = new FakeDrive();
    const ranges = [];
    const bytes = Buffer.alloc(8 * 1024 * 1024 + 3, 42);
    const server = http.createServer(async (req, res) => {
        const chunks = [];
        for await (const chunk of req) chunks.push(chunk);
        const body = Buffer.concat(chunks);
        ranges.push(req.headers['content-range']);
        assert.equal(body.length, Number(req.headers['content-length']));
        if (ranges.length === 1) {
            res.writeHead(308, { Location: session, Range: `bytes=0-${body.length - 1}` });
            res.end();
        } else if (ranges[1] === ranges[0]) {
            res.writeHead(400);
            res.end(JSON.stringify({ error: { message: 'chunk replayed as a redirect' } }));
        } else {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(drive.resource(drive.addFile('large.bin', bytes))));
        }
    });
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    const session = `http://127.0.0.1:${server.address().port}/session`;
    t.after(() => new Promise(resolve => { server.close(resolve); server.closeAllConnections(); }));
    const backend = new GdriveBackend('drive', { ...CREDS, fetch: async (url, init) => {
        if (url === session) return fetch(url, init);
        const response = await drive.fetch(url, init);
        if (response.headers.has('location')) return new Response(null, { status: 200, headers: { Location: session } });
        return response;
    } });
    const result = await backend.put('large.bin', Readable.from([bytes]));
    assert.equal(result.size, bytes.length);
    assert.deepEqual(ranges, ['bytes 0-8388607/*', 'bytes 8388608-8388610/8388611']);
});

test('upload transport failures preserve their cause and report an upstream failure', async () => {
    const drive = new FakeDrive();
    const cause = Object.assign(new Error('socket closed'), { code: 'UND_ERR_SOCKET' });
    const failure = new TypeError('fetch failed', { cause });
    const backend = new GdriveBackend('drive', { ...CREDS, fetch: async (url, init) => {
        if (new URL(url).pathname === '/upload/session') throw failure;
        return drive.fetch(url, init);
    } });
    await assert.rejects(backend.put('photo.jpg', Buffer.from('photo')), error => {
        assert.equal(error.statusCode, 502);
        assert.equal(error.status, null);
        assert.equal(error.cause, failure);
        assert.equal(error.transportCode, 'UND_ERR_SOCKET');
        assert.match(error.message, /UND_ERR_SOCKET/);
        assert.ok(!error.message.includes('?id='));
        return true;
    });
});

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
