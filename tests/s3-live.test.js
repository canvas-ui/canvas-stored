import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { randomUUID, createHash } from 'node:crypto';
import { S3Client, CreateBucketCommand, DeleteBucketCommand, ListObjectsV2Command, DeleteObjectCommand } from '@aws-sdk/client-s3';
import S3Backend from '../src/backends/s3/index.js';
import Stored from '../src/index.js';

// Opt-in. Creates its own bucket; never opens a user's existing bucket.
test('real S3 server: signed streaming writes, Unicode listing, conflicts, polling and Stored replication', { skip: !process.env.CANVAS_S3_TEST_ENDPOINT, timeout: 60000 }, async t => {
    const config = { driver: 's3', endpoint: process.env.CANVAS_S3_TEST_ENDPOINT,
        region: process.env.CANVAS_S3_TEST_REGION || 'us-east-1', forcePathStyle: true,
        accessKeyId: process.env.CANVAS_S3_TEST_ACCESS_KEY || 'canvas-test',
        secretAccessKey: process.env.CANVAS_S3_TEST_SECRET_KEY || 'canvas-test-password',
        bucket: `canvas-test-${randomUUID()}` };
    const client = new S3Client({ region: config.region, endpoint: config.endpoint, forcePathStyle: true,
        credentials: { accessKeyId: config.accessKeyId, secretAccessKey: config.secretAccessKey },
        requestChecksumCalculation: 'WHEN_REQUIRED', responseChecksumValidation: 'WHEN_REQUIRED' });
    await client.send(new CreateBucketCommand({ Bucket: config.bucket, ...(config.region !== 'us-east-1' ? { CreateBucketConfiguration: { LocationConstraint: config.region } } : {}) }));
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'canvas-s3-live-'));
    const s3 = new S3Backend('s3', { ...config, prefix: 'scope', permanentDelete: true });
    let stored;
    t.after(async () => {
        await stored?.stop(); await s3.stop();
        for (;;) {
            const page = await client.send(new ListObjectsV2Command({ Bucket: config.bucket }));
            if (!page.Contents?.length) break;
            for (const item of page.Contents) await client.send(new DeleteObjectCommand({ Bucket: config.bucket, Key: item.Key }));
        }
        await client.send(new DeleteBucketCommand({ Bucket: config.bucket })); client.destroy();
        await fs.rm(root, { recursive: true, force: true });
    });
    assert.equal((await s3.verifyRoot()).ok, true);
    await s3.verifyWrites();
    const key = 'Kuchyňa/report %.txt';
    const bytes = Buffer.alloc(8 * 1024 * 1024 + 17, 'x');
    const file = path.join(root, 'upload'); await fs.writeFile(file, bytes);
    await s3.createFrom(key, file);
    assert.deepEqual(await s3.get(key), bytes);
    await assert.rejects(s3.put(key, 'collision'), { code: 'EEXIST' });
    const scan = await s3.scan(); assert.equal(scan.complete, true);
    assert.equal(scan.files[0].key, key);
    assert.equal(scan.files[0].checksums.sha256, createHash('sha256').update(bytes).digest('hex'));
    const chunks = []; for await (const chunk of await s3.getRange(key, { start: 2, end: 7 })) chunks.push(chunk);
    assert.equal(Buffer.concat(chunks).length, 6);
    await s3.poll(); const deleted = []; s3.on('file:unlink', event => deleted.push(event.key));
    await s3.delete(key); await s3.poll(); assert.deepEqual(deleted, [key]);
    stored = new Stored({ root: path.join(root, 'stored'), checksums: ['sha256'], replication: { autoStart: false } });
    stored.addBackend('data', { driver: 'cacache', root: path.join(root, 'data') });
    stored.addBackend('s3', config);
    const upload = await stored.put(Buffer.from('real accounting bytes'), { backends: ['data'], key: 'staged' });
    stored.replicas.submit({ id: 'live-replica', owner: 'live-doc', contentId: upload.id, source: { backend: 'data', key: 'staged' },
        replicas: ['one/file.txt', 'two/file.txt'].map(key => ({ backend: 's3', key, mirror: false })), release: [{ backend: 'data', key: 'staged' }] });
    await stored.replicas.drain();
    assert.equal(stored.replicas.get('live-replica').state, 'complete', JSON.stringify(stored.replicas.get('live-replica').error));
    assert.equal(await stored.getBackend('data').get('staged'), null);
    assert.equal((await stored.getBackend('s3').get('two/file.txt')).toString(), 'real accounting bytes');
});
