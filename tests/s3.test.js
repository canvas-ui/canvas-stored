import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { createHash } from 'node:crypto';
import S3Backend from '../src/backends/s3/index.js';
import Stored from '../src/index.js';
import { FakeS3 } from './helpers/fake-s3.js';
const hash = data => createHash('sha256').update(data).digest('hex');
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
test('S3 probes conditional creation, preserves a collision and refuses implicit deletion', async t => {
    const client = new FakeS3(), s3 = driver(client); t.after(() => s3.stop());
    await s3.put('invoice.txt', Buffer.from('invoice'));
    await assert.rejects(s3.put('invoice.txt', 'replacement'), { code: 'EEXIST' });
    assert.equal((await s3.get('invoice.txt')).toString(), 'invoice');
    assert.ok(![...client.objects.keys()].some(k => k.includes('.canvas-probe')));
    assert.equal(client.calls.filter(c => c.op === 'PutObjectCommand' && c.input.Key.includes('.canvas-probe')).length, 3);
    await assert.rejects(s3.delete('invoice.txt'), { code: 'DELETE_DISABLED' });
    assert.equal(s3.capabilities.delete, false);
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
test('S3 blob keys are unconditional, deduplicated by size, multipart when large and never listed', async t => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 's3-blob-'));
    const client = new FakeS3(), s3 = driver(client, { mode: 'cas', permanentDelete: true });
    t.after(async () => { await s3.stop(); await fs.rm(root, { recursive: true, force: true }); });
    const file = path.join(root, 'blob');
    await fs.writeFile(file, 'blob bytes');
    const key = 'ab/cd/abcd';
    assert.equal((await s3.createFrom(key, file)).size, 10);
    // This endpoint ignores x-amz-checksum-sha256: one probe, then Content-MD5.
    assert.equal(client.calls.filter(c => c.op === 'PutObjectCommand' && c.input.Key.includes('.canvas-probe')).length, 1);
    assert.ok(client.calls.find(c => c.input.Key === 'scope/ab/cd/abcd' && c.op === 'PutObjectCommand').input.ContentMD5);
    client.calls.length = 0;
    await s3.createFrom(key, file);
    assert.equal(client.calls.filter(c => c.op === 'PutObjectCommand').length, 0);
    await fs.truncate(file, 64 * 1024 ** 2 + 5);
    const large = 'ef/01/ef01';
    assert.equal((await s3.createFrom(large, file)).size, 64 * 1024 ** 2 + 5);
    assert.equal(client.calls.filter(c => c.op === 'UploadPartCommand').length, 2);
    assert.ok(client.objects.has('scope/ab/cd/abcd'));
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
    const sum = hash('checked bytes');
    await s3.createFrom('aa/bb/known', file, { sha256: sum });
    const sent = client.calls.find(c => c.op === 'PutObjectCommand' && c.input.Key === 'scope/aa/bb/known').input;
    assert.equal(sent.ChecksumSHA256, Buffer.from(sum, 'hex').toString('base64'));
    assert.equal(sent.ContentMD5, undefined);
    assert.equal((await s3.stat('aa/bb/known')).sha256, sum);
    // A checksum that does not match the bytes is refused by the endpoint.
    await assert.rejects(s3.createFrom('aa/bb/wrong', file, { sha256: hash('other bytes') }), { name: 'BadDigest' });
    assert.equal(await s3.stat('aa/bb/wrong'), null);
    // Without a checksum from the caller the driver computes it.
    await s3.createFrom('aa/bb/computed', file);
    assert.equal((await s3.stat('aa/bb/computed')).sha256, sum);
});
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
