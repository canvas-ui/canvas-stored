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
    assert.equal(client.calls.filter(c => c.op === 'PutObjectCommand' && c.input.Key.includes('.canvas-probe')).length, 2);
    await assert.rejects(s3.delete('invoice.txt'), { code: 'RECOVERY_UNAVAILABLE' });
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
