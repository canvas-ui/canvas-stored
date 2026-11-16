import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { createHash } from 'node:crypto';
import S3Backend from '../src/backends/s3/index.js';
import Stored from '../src/index.js';
import { FakeS3 } from './helpers/fake-s3.js';
import { startS3Http } from './helpers/s3-http.js';
const hash = data => createHash('sha256').update(data).digest('hex');
const blobKey = sum => `${sum.slice(0, 2)}/${sum.slice(2, 4)}/${sum}`;
const driver = (client, config = {}) => new S3Backend('s3:test', { driver: 's3', bucket: 'test', prefix: 'scope', client, ...config });

test('S3 prefix, Unicode and percent keys round-trip without treating ETags as checksums', async t => {
    const client = new FakeS3(), s3 = driver(client); t.after(() => s3.stop());
    for (const key of ['Kuchyňa/a %.txt', '2026/b.txt', 'c.txt']) client.add('scope/' + key, key);
    client.add('scope-other/hidden', 'hidden');
    const scanned = await s3.scan();
    assert.equal(scanned.complete, true); assert.equal(scanned.files.length, 3);
    for (const row of scanned.files) assert.equal(row.checksums.sha256, hash(row.key));
    assert.equal((await s3.get('Kuchyňa/a %.txt')).toString(), 'Kuchyňa/a %.txt');
    assert.equal(s3.nativeUrl('Kuchyňa/a %.txt'), 's3://test/scope/Kuchy%C5%88a/a%20%25.txt');
    assert.deepEqual((await s3.shape()).dirs.sort(), ['2026', 'Kuchyňa']);
    const range = await s3.getRange('2026/b.txt', { start: 0, end: 3 });
    const chunks = []; for await (const c of range) chunks.push(c);
    assert.equal(Buffer.concat(chunks).toString(), '2026');
    for (const key of ['../x', '/x', 'a//b', '.canvas-probe/x']) await assert.rejects(s3.get(key), { code: 'INVALID_KEY' });
});
test('S3 probes conditional creation, preserves a collision and deletes unless that is turned off', async t => {
    const client = new FakeS3(), s3 = driver(client, { permanentDelete: false }); t.after(() => s3.stop());
    await s3.put('invoice.txt', Buffer.from('invoice'));
    await assert.rejects(s3.put('invoice.txt', 'replacement'), { code: 'EEXIST' });
    assert.equal((await s3.get('invoice.txt')).toString(), 'invoice');
    assert.ok(![...client.objects.keys()].some(k => k.includes('.canvas-probe')));
    assert.equal(client.calls.filter(c => c.op === 'PutObjectCommand' && c.input.Key.includes('.canvas-probe')).length, 3);
    await assert.rejects(s3.delete('invoice.txt'), { code: 'DELETE_DISABLED' });
    assert.equal(s3.capabilities.delete, false);
    const deleting = driver(client); t.after(() => deleting.stop());
    assert.equal(deleting.capabilities.delete, true);
    assert.equal(await deleting.delete('invoice.txt'), true);
    assert.equal(client.objects.has('scope/invoice.txt'), false);
});
test('S3 refuses endpoints that ignore If-None-Match before touching user keys', async t => {
    const client = new FakeS3(); client.ignoreConditional = true;
    const s3 = driver(client); t.after(() => s3.stop());
    client.add('scope/precious', 'keep');
    await assert.rejects(s3.put('precious', 'danger'), { code: 'CONDITIONAL_WRITES_UNSUPPORTED' });
    assert.equal(client.objects.get('scope/precious').data.toString(), 'keep');
    assert.equal(client.calls.filter(c => c.op === 'PutObjectCommand' && c.input.Key === 'scope/precious').length, 0);
});
test('S3 readonly config and explicit credential requirement', async t => {
    assert.throws(() => new S3Backend('s3', { bucket: 'test' }), { code: 'INVALID_CONFIG' });
    const s3 = driver(new FakeS3(), { readOnly: true }); t.after(() => s3.stop());
    await assert.rejects(s3.put('x', 'x'), { code: 'READ_ONLY' });
    assert.equal(s3.capabilities.write, false);
});
test('S3 polling does not turn failed listing or unreadable objects into deletes', async t => {
    const client = new FakeS3(), s3 = driver(client); t.after(() => s3.stop());
    client.add('scope/a', 'a'); client.add('scope/b', 'b');
    await s3.poll(); const deleted = []; s3.on('file:unlink', e => deleted.push(e.key));
    client.objects.delete('scope/a'); client.listFailure = true;
    assert.equal(await s3.poll(), false); assert.deepEqual(deleted, []);
    client.listFailure = false; client.failKey = 'scope/b';
    assert.equal(await s3.poll(), false); assert.deepEqual(deleted, []);
    client.failKey = null; assert.equal(await s3.poll(), true); assert.deepEqual(deleted, ['a']);
});
test('S3 scan catches changed bytes with unchanged size/time and mid-read edits', async t => {
    const client = new FakeS3(), s3 = driver(client); t.after(() => s3.stop());
    client.add('scope/a', 'old');
    const first = await s3.scan(), time = client.objects.get('scope/a').LastModified;
    client.add('scope/a', 'new'); client.objects.get('scope/a').LastModified = time;
    assert.notEqual((await s3.scan()).files[0].checksums.sha256, first.files[0].checksums.sha256);
    client.add('scope/a', 'third'); client.afterGet = key => client.add(key, 'raced');
    const raced = await s3.scan(); assert.equal(raced.complete, false); assert.equal(raced.files[0].checksums, null);
});
test('S3 incomplete pagination is reported as incomplete, not an empty backend', async t => {
    const client = new FakeS3(), original = client.send.bind(client);
    client.send = command => command.constructor.name === 'ListObjectsV2Command' ? { IsTruncated: true, Contents: [] } : original(command);
    const s3 = driver(client); t.after(() => s3.stop());
    assert.equal((await s3.scan()).complete, false);
});
test('S3 streams with a content type and rejects oversized uploads before any network write', async t => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 's3-size-'));
    const client = new FakeS3(), s3 = driver(client);
    t.after(async () => { await s3.stop(); await fs.rm(root, { recursive: true, force: true }); });
    const file = path.join(root, 'spool');
    await fs.writeFile(file, '# Invoice');
    await s3.createFrom('invoice.md', file);
    assert.equal((await s3.stat('invoice.md')).mimeType, 'text/markdown');
    client.add('scope/external.json', '{"invoice":1}');
    client.objects.get('scope/external.json').ContentType = 'application/octet-stream';
    assert.equal((await s3.scan()).files.find(row => row.key === 'external.json').mimeType, 'application/json');
    client.calls.length = 0;
    await fs.truncate(file, 5 * 1024 ** 3 + 1);
    await assert.rejects(s3.createFrom('too-big', file), { code: 'OBJECT_TOO_LARGE' });
    assert.deepEqual(client.calls, []);
});
test('Stored S3 filing verifies every target, keeps staging on failure and recovers after reopen', async t => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'stored-s3-'));
    const client = new FakeS3();
    const open = () => {
        const stored = new Stored({ root, checksums: ['sha256'], replication: { autoStart: false } });
        stored.addBackend('data', { driver: 'cacache', root: path.join(root, 'data') });
        stored.addBackend('s3:test', { driver: 's3', bucket: 'test', client });
        return stored;
    };
    let stored = open(); t.after(async () => { await stored.stop(); await fs.rm(root, { recursive: true, force: true }); });
    const upload = await stored.put(Buffer.from('accounting'), { backends: ['data'], key: 'upload' });
    client.failKey = 'backup/invoice.txt';
    stored.replicas.submit({ id: 's3-recovery', owner: 'doc:1', contentId: upload.id, source: { backend: 'data', key: 'upload' },
        replicas: ['primary/invoice.txt', 'backup/invoice.txt'].map(key => ({ backend: 's3:test', key, mirror: false })), release: [{ backend: 'data', key: 'upload' }] });
    await stored.replicas.drain();
    assert.equal(stored.replicas.get('s3-recovery').state, 'blocked');
    assert.equal((await stored.getBackend('data').get('upload')).toString(), 'accounting');
    assert.equal(client.objects.get('primary/invoice.txt').data.toString(), 'accounting');
    await stored.stop(); stored = open(); client.failKey = null;
    await stored.replicas.retry('s3-recovery');
    assert.equal(stored.replicas.get('s3-recovery').state, 'complete');
    assert.equal(await stored.getBackend('data').get('upload'), null);
    assert.equal(client.objects.get('backup/invoice.txt').data.toString(), 'accounting');
    assert.throws(() => stored.replicas.submit({ id: 'unsafe', owner: 'doc:2', contentId: upload.id, replicas: [{ backend: 's3:test', key: 'mirror' }] }), { code: 'UNSUPPORTED_RETIRE' });
});

test('S3 Object Lock: a probe version that cannot be removed is hidden, and the connection still verifies', async t => {
    const client = new FakeS3(); client.versioned = true; client.objectLock = true;
    const s3 = driver(client); t.after(() => s3.stop());
    assert.deepEqual(await s3.verifyWrites(), { ok: true, conditionalCreate: true, sha256: false });
    assert.ok(![...client.objects.keys()].some(k => k.includes('.canvas-probe')));
});
test('S3 blob keys are unconditional, deduplicated by checksum, multipart when large and never listed', async t => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 's3-blob-'));
    const client = new FakeS3(), s3 = driver(client, { mode: 'cas' });
    t.after(async () => { await s3.stop(); await fs.rm(root, { recursive: true, force: true }); });
    const file = path.join(root, 'blob');
    await fs.writeFile(file, 'blob bytes');
    const sum = hash('blob bytes'), key = blobKey(sum);
    const row = await s3.createFrom(key, file, { sha256: sum });
    assert.deepEqual([row.size, row.sha256], [10, sum]);
    // This endpoint ignores x-amz-checksum-sha256: one probe, then Content-MD5.
    assert.equal(client.calls.filter(c => c.op === 'PutObjectCommand' && c.input.Key.includes('.canvas-probe')).length, 1);
    assert.ok(client.calls.find(c => c.input.Key === `scope/${key}` && c.op === 'PutObjectCommand').input.ContentMD5);
    // A new blob costs one HEAD and one PUT.
    assert.deepEqual(client.calls.filter(c => c.input.Key === `scope/${key}`).map(c => c.op), ['HeadObjectCommand', 'PutObjectCommand']);
    client.calls.length = 0;
    await s3.createFrom(key, file);
    assert.deepEqual(client.calls.map(c => c.op), ['HeadObjectCommand']);
    // Keys that are not a checksum, or not this file's, never reach the bucket.
    await assert.rejects(s3.createFrom('Universe/blob.txt', file, { sha256: sum }), { code: 'INVALID_KEY' });
    await assert.rejects(s3.createFrom(blobKey(hash('other')), file, { sha256: sum }), { code: 'INVALID_KEY' });
    await assert.rejects(s3.createFrom(blobKey(hash('other')), file), { code: 'SOURCE_CORRUPT' });
    assert.equal(client.objects.size, 1);
    await fs.truncate(file, 64 * 1024 ** 2 + 5);
    const largeSum = hash(await fs.readFile(file)), large = blobKey(largeSum);
    const largeRow = await s3.createFrom(large, file);
    assert.deepEqual([largeRow.size, largeRow.sha256], [64 * 1024 ** 2 + 5, largeSum]);
    assert.equal(client.calls.filter(c => c.op === 'UploadPartCommand').length, 2);
    assert.equal((await s3.stat(large)).sha256, largeSum);
    // CAS mode: nothing is listed, a blob takes its sidecar with it.
    client.add('scope/visible.txt', 'visible');
    assert.deepEqual((await s3.scan()).files, []);
    assert.equal(s3.listed(key), false);
    assert.deepEqual([s3.capabilities.blobs, s3.capabilities.paths, s3.capabilities.upload, s3.capabilities.canEnumerate], [true, false, false, false]);
    assert.equal(await s3.writeSidecar(key, { schema: 'data/schema/file', data: { title: 'Blob' } }), true);
    assert.equal(await s3.writeSidecar(key, { schema: 'changed' }), false);
    assert.equal((await s3.readSidecar(key)).data.title, 'Blob');
    await s3.delete(key);
    assert.equal(client.objects.has(`scope/${key}`) || client.objects.has(`scope/${key}.json`), false);
    const paths = driver(new FakeS3()); t.after(() => paths.stop());
    assert.equal(paths.capabilities.blobs, false);
});
test('S3 endpoint that checks SHA-256: uploads send the checksum the caller already has and no MD5', async t => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 's3-sha-'));
    const client = new FakeS3(); client.checksums = true;
    const s3 = driver(client, { mode: 'cas' });
    t.after(async () => { await s3.stop(); await fs.rm(root, { recursive: true, force: true }); });
    assert.equal((await s3.verifyWrites()).sha256, true);
    assert.ok(![...client.objects.keys()].some(k => k.includes('.canvas-probe')));
    const file = path.join(root, 'blob');
    await fs.writeFile(file, 'checked bytes');
    const sum = hash('checked bytes'), key = blobKey(sum);
    await s3.createFrom(key, file, { sha256: sum });
    const sent = client.calls.find(c => c.op === 'PutObjectCommand' && c.input.Key === `scope/${key}`).input;
    assert.equal(sent.ChecksumSHA256, Buffer.from(sum, 'hex').toString('base64'));
    assert.equal(sent.ContentMD5, undefined);
    assert.equal((await s3.stat(key)).sha256, sum);
    // Bytes that do not match the checksum in the key are refused by the endpoint.
    const wrong = blobKey(hash('other bytes'));
    await assert.rejects(s3.createFrom(wrong, file), { name: 'BadDigest' });
    assert.equal(await s3.stat(wrong), null);
});
for (const checksums of [true, false]) {
    test(`S3 uploads run on the upload thread through the AWS SDK (${checksums ? 'SHA-256' : 'MD5'} endpoint)`, async t => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), 's3-thread-'));
        const remote = await startS3Http({ checksums });
        const s3 = new S3Backend('s3:test', { driver: 's3', mode: 'cas', bucket: 'bucket', prefix: 'blobs', endpoint: remote.endpoint, accessKeyId: 'id', secretAccessKey: 'secret' });
        t.after(async () => { await s3.stop(); await remote.stop(); await fs.rm(root, { recursive: true, force: true }); });
        const names = ['one', 'two', 'three', 'four'];
        const rows = await Promise.all(names.map(async name => {
            const file = path.join(root, name);
            await fs.writeFile(file, `bytes of ${name}`);
            return s3.createFrom(blobKey(hash(`bytes of ${name}`)), file, { sha256: hash(`bytes of ${name}`) });
        }));
        assert.deepEqual(rows.map(r => r.sha256), names.map(name => hash(`bytes of ${name}`)));
        const puts = remote.requests.filter(r => r.method === 'PUT' && !r.key.includes('.canvas-probe'));
        assert.equal(puts.length, 4);
        assert.ok(puts.every(r => Boolean(r.headers['x-amz-checksum-sha256']) === checksums && Boolean(r.headers['content-md5']) === !checksums));
        assert.ok(puts.every(r => !Object.keys(r.headers).some(h => h.includes('crc'))));
        assert.equal(remote.objects.get(`/bucket/blobs/${blobKey(hash('bytes of one'))}`).body.toString(), 'bytes of one');
        assert.ok(![...remote.objects.keys()].some(k => k.includes('.canvas-probe')));
        // A source that no longer hashes to its key fails on the thread and reports back.
        const file = path.join(root, 'one');
        await fs.writeFile(file, 'changed');
        await assert.rejects(s3.createFrom(blobKey(hash('never uploaded')), file), checksums ? { name: 'BadDigest' } : { code: 'SOURCE_CORRUPT' });
    });
}
test('Stored uploads a blob to its local store and a remote blob prefix; a scan keeps the unlisted location', async t => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'stored-s3-primary-'));
    const client = new FakeS3();
    const stored = new Stored({ root, checksums: ['sha256'], replication: { autoStart: false } });
    t.after(async () => { await stored.stop(); await fs.rm(root, { recursive: true, force: true }); });
    stored.addBackend('data', { driver: 'cacache', root: path.join(root, 'data') });
    stored.addBackend('s3:test', { driver: 's3', bucket: 'test', prefix: 'scope', client, mode: 'cas', permanentDelete: true });
    const sum = hash('ledger');
    const remoteKey = `${sum.slice(0, 2)}/${sum.slice(2, 4)}/${sum}`;
    const upload = await stored.put(Buffer.from('ledger'), { backends: ['data', 's3:test'] });
    assert.equal(upload.key, `${sum.slice(0, 2)}/${sum.slice(2, 4)}/${sum}`);
    assert.deepEqual(upload.locations.map(l => [l.backend, l.key, l.synced]), [['data', upload.key, true], ['s3:test', remoteKey, false]]);
    await stored.replicas.drain();
    assert.equal(client.objects.get(`scope/${remoteKey}`).data.toString(), 'ledger');
    // The upload is verified from the checksum recorded with it, not by reading it back.
    assert.equal(client.objects.get(`scope/${remoteKey}`).Metadata.sha256, sum);
    assert.equal(client.calls.filter(c => c.op === 'GetObjectCommand' && c.input.Key === `scope/${remoteKey}`).length, 0);
    const synced = () => stored.stat(upload.id).then(meta => meta.locations.find(l => l.backend === 's3:test'));
    assert.equal((await synced()).synced, true);
    assert.equal((await stored.scan('s3:test')).count, 0);
    assert.equal((await synced()).key, remoteKey);
    const puts = () => client.calls.filter(c => c.op === 'PutObjectCommand' && c.input.Key === `scope/${remoteKey}`).length;
    const before = puts();
    const again = await stored.put(Buffer.from('ledger'), { backends: ['data', 's3:test'] });
    assert.equal(again.locations.find(l => l.backend === 's3:test').synced, true);
    await stored.replicas.drain();
    assert.equal(puts(), before);
    assert.equal((await stored.deleteByUrl(`stored://s3:test/${remoteKey}`)).ok, true);
    assert.equal(client.objects.has(`scope/${remoteKey}`), false);
    // A copy onto the content store ignores the chosen name.
    const named = await stored.put(Buffer.from('named'), { backends: ['data'], key: 'docs/named.txt' });
    const copied = await stored.copy(named.id, { to: 's3:test', key: 'elsewhere/named.txt' });
    const namedSum = hash('named'), namedKey = `${namedSum.slice(0, 2)}/${namedSum.slice(2, 4)}/${namedSum}`;
    assert.deepEqual(copied.added, [`stored://s3:test/${namedKey}`]);
    await stored.replicas.drain();
    assert.equal(client.objects.get(`scope/${namedKey}`).data.toString(), 'named');
    assert.equal((await stored.copy(named.id, { to: 's3:test' })).unchanged, true);
});

test('Stored uploads to a content store side by side, two requests each, and blocked copies can be retried or dismissed', async t => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'stored-s3-lane-'));
    const client = new FakeS3();
    let active = 0, peak = 0;
    const send = client.send.bind(client);
    client.send = async command => {
        active++; peak = Math.max(peak, active);
        try { await new Promise(resolve => setTimeout(resolve, 5)); return await send(command); } finally { active--; }
    };
    const stored = new Stored({ root, checksums: ['sha256'], replication: { autoStart: false } });
    t.after(async () => { await stored.stop(); await fs.rm(root, { recursive: true, force: true }); });
    stored.addBackend('data', { driver: 'cacache', root: path.join(root, 'data') });
    stored.addBackend('s3:test', { driver: 's3', bucket: 'test', prefix: 'scope', client, mode: 'cas' });
    await stored.getBackend('s3:test').verifyWrites();
    client.calls.length = 0;
    const uploads = [];
    for (let i = 0; i < 12; i++) uploads.push(await stored.put(Buffer.from(`file ${i}`), { backends: ['data', 's3:test'] }));
    await stored.replicas.drain();
    assert.ok(peak > 1, 'uploads overlap');
    assert.equal(client.calls.length, 24);
    assert.deepEqual([...new Set(client.calls.map(c => c.op))].sort(), ['HeadObjectCommand', 'PutObjectCommand']);
    for (const upload of uploads) assert.equal((await stored.stat(upload.id)).locations.find(l => l.backend === 's3:test').synced, true);
    assert.deepEqual(stored.replicas.list({ active: true }).operations, []);

    // A refused upload blocks its own copy only.
    const refused = hash('refused');
    client.failKey = `scope/${blobKey(refused)}`;
    const failing = await stored.put(Buffer.from('refused'), { backends: ['data', 's3:test'] });
    const fine = await stored.put(Buffer.from('fine'), { backends: ['data', 's3:test'] });
    await stored.replicas.drain();
    assert.equal((await stored.stat(fine.id)).locations.find(l => l.backend === 's3:test').synced, true);
    const [blocked] = stored.replicas.list({ active: true }).operations;
    assert.equal(blocked.state, 'blocked');
    // Dismissing it drops the placeholder location and keeps the local copy.
    assert.equal(await stored.replicas.cancelBlocked(), 1);
    assert.deepEqual(stored.replicas.list({ active: true }).operations, []);
    assert.deepEqual((await stored.stat(failing.id)).locations.map(l => l.backend), ['data']);
    assert.equal((await stored.get(failing.id)).toString(), 'refused');
    // Copying again once the endpoint accepts it: a blocked copy is retried in place.
    assert.equal((await stored.copy(failing.id, { to: 's3:test' })).ok, true);
    await stored.replicas.drain();
    assert.equal(stored.replicas.list({ active: true }).operations[0].state, 'blocked');
    client.failKey = null;
    assert.equal(stored.replicas.retryBlocked(), 1);
    await stored.replicas.drain();
    assert.equal(client.objects.get(`scope/${blobKey(refused)}`).data.toString(), 'refused');
    assert.deepEqual(stored.replicas.list({ active: true }).operations, []);
});

test('Stored uploads to chosen S3 paths side by side, outside the write lock, and a taken name blocks only its own copy', async t => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'stored-s3-paths-'));
    const client = new FakeS3();
    let active = 0, peak = 0, release = null;
    const send = client.send.bind(client);
    client.send = async command => {
        active++; peak = Math.max(peak, active);
        try {
            if (release && command.constructor.name === 'PutObjectCommand') await release;
            await new Promise(resolve => setTimeout(resolve, 5));
            return await send(command);
        } finally { active--; }
    };
    const stored = new Stored({ root, checksums: ['sha256'], replication: { autoStart: false } });
    t.after(async () => { await stored.stop(); await fs.rm(root, { recursive: true, force: true }); });
    stored.addBackend('data', { driver: 'cacache', root: path.join(root, 'data') });
    stored.addBackend('s3:test', { driver: 's3', bucket: 'test', prefix: 'scope', client });
    await stored.getBackend('s3:test').verifyWrites();
    await stored.getBackend('s3:test').verifyRoot();
    client.calls.length = 0;
    const uploads = [];
    for (let i = 0; i < 12; i++) uploads.push(await stored.put(Buffer.from(`report ${i}`), { backends: ['s3:test'], key: `Reports/${i}.txt` }));
    await stored.replicas.drain();
    assert.ok(peak > 1, 'uploads overlap');
    // One liveness check for the batch, then a HEAD and a PUT per file.
    assert.deepEqual(client.calls.reduce((n, c) => ({ ...n, [c.op]: (n[c.op] || 0) + 1 }), {}), { ListObjectsV2Command: 1, HeadObjectCommand: 12, PutObjectCommand: 12 });
    assert.equal(client.objects.get('scope/Reports/7.txt').data.toString(), 'report 7');
    assert.equal(client.objects.get('scope/Reports/7.txt').Metadata.sha256, hash('report 7'));
    for (const upload of uploads) assert.equal((await stored.stat(upload.id)).locations[0].synced, true);

    // While an upload is in flight, other writes are not queued behind it.
    let open; release = new Promise(resolve => { open = resolve; });
    const slow = await stored.put(Buffer.from('slow upload'), { backends: ['s3:test'], key: 'Reports/slow.txt' });
    const draining = stored.replicas.drain();
    const local = await stored.put(Buffer.from('local write'), { backends: ['data'] });
    assert.equal(local.ok, true);
    assert.equal((await stored.stat(slow.id)).locations[0].synced, false);
    open(); release = null;
    await draining;
    assert.equal((await stored.stat(slow.id)).locations[0].synced, true);

    // A name that already holds other bytes is a conflict; the same bytes are the copy.
    client.add('scope/Reports/taken.txt', 'someone else');
    client.add('scope/Reports/same.txt', 'already there');
    client.objects.get('scope/Reports/same.txt').Metadata = { sha256: hash('already there') };
    const taken = await stored.put(Buffer.from('mine'), { backends: ['s3:test'], key: 'Reports/taken.txt' });
    const same = await stored.put(Buffer.from('already there'), { backends: ['s3:test'], key: 'Reports/same.txt' });
    await stored.replicas.drain();
    const open2 = stored.replicas.list({ active: true }).operations;
    assert.deepEqual(open2.map(op => [op.state, op.error.code]), [['blocked', 'TARGET_CONFLICT']]);
    assert.equal(client.objects.get('scope/Reports/taken.txt').data.toString(), 'someone else');
    assert.equal((await stored.stat(taken.id)).locations[0].synced, false);
    assert.equal((await stored.stat(same.id)).locations[0].synced, true);
    assert.equal(client.calls.filter(c => c.op === 'PutObjectCommand' && c.input.Key === 'scope/Reports/same.txt').length, 0);
});

test('forgetting a removed backend cancels work that targets it and owners stop claiming its replicas', async t => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'stored-s3-forget-'));
    const client = new FakeS3();
    const stored = new Stored({ root, checksums: ['sha256'], replication: { autoStart: false } });
    t.after(async () => { await stored.stop(); await fs.rm(root, { recursive: true, force: true }); });
    stored.addBackend('data', { driver: 'cacache', root: path.join(root, 'data') });
    stored.addBackend('s3:test', { driver: 's3', bucket: 'test', prefix: 'scope', client, mode: 'cas' });
    const done = await stored.put(Buffer.from('uploaded'), { backends: ['data', 's3:test'] });
    await stored.replicas.drain();
    client.failKey = `scope/${blobKey(hash('stuck'))}`;
    const stuck = await stored.put(Buffer.from('stuck'), { backends: ['data', 's3:test'] });
    await stored.replicas.drain();
    assert.equal(stored.replicas.counts().blocked, 1);
    assert.ok(stored.replicas.owners().some(owner => owner.replicas.some(r => r.backend === 's3:test')));
    await stored.removeBackend('s3:test');
    assert.equal(await stored.replicas.forgetBackend('s3:test'), 1);
    assert.deepEqual(stored.replicas.counts(), { pending: 0, running: 0, blocked: 0 });
    assert.ok(!stored.replicas.owners().some(owner => owner.replicas.some(r => r.backend === 's3:test')));
    for (const put of [done, stuck]) assert.deepEqual((await stored.stat(put.id)).locations.map(l => l.backend), ['data']);
    assert.equal((await stored.get(stuck.id)).toString(), 'stuck');
});
