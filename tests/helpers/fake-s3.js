import { createHash } from 'node:crypto';
import { Readable } from 'node:stream';
export const credentials = { accessKeyId: 'test-key', secretAccessKey: 'test-secret' };
const fail = (name, status) => Object.assign(new Error(name), { name, $metadata: { httpStatusCode: status } });
export class FakeS3 {
    objects = new Map(); calls = []; ignoreConditional = false; listFailure = false; failKey = null; pageSize = 2; clock = 0; afterGet = null;
    add(key, value) {
        const data = Buffer.from(value);
        this.objects.set(key, { data, ETag: `"${createHash('md5').update(data).digest('hex')}-2"`, LastModified: new Date(1700000000000 + ++this.clock), ContentType: 'text/plain' });
    }
    async send(command) {
        const op = command.constructor.name, input = command.input;
        this.calls.push({ op, input });
        const { Key, Prefix = '' } = input;
        if (this.failKey === Key && !Key?.includes('.canvas-probe')) throw fail('AccessDenied', 403);
        const row = this.objects.get(Key);
        if (op === 'ListObjectsV2Command') {
            if (this.listFailure) throw fail('AccessDenied', 403);
            const all = [...this.objects.keys()].filter(k => k.startsWith(Prefix)).sort();
            const offset = Number(input.ContinuationToken || 0), count = Math.min(input.MaxKeys || this.pageSize, this.pageSize);
            return { EncodingType: 'url', Contents: all.slice(offset, offset + count).map(k => ({ Key: encodeURIComponent(k), Size: this.objects.get(k).data.length, ...this.objects.get(k) })),
                IsTruncated: all.length > offset + count, NextContinuationToken: all.length > offset + count ? String(offset + count) : undefined };
        }
        if (op === 'PutObjectCommand') {
            if (row && input.IfNoneMatch === '*' && !this.ignoreConditional) throw fail('PreconditionFailed', 412);
            const chunks = [];
            if (typeof input.Body === 'string' || Buffer.isBuffer(input.Body)) chunks.push(Buffer.from(input.Body));
            else for await (const chunk of input.Body) chunks.push(chunk);
            const data = Buffer.concat(chunks);
            if (input.ContentMD5 && createHash('md5').update(data).digest('base64') !== input.ContentMD5) throw fail('BadDigest', 400);
            this.add(Key, data);
            this.objects.get(Key).ContentType = input.ContentType || 'application/octet-stream';
            return { ETag: this.objects.get(Key).ETag };
        }
        if (op === 'DeleteObjectCommand') { this.objects.delete(Key); return {}; }
        if (!row) throw fail('NoSuchKey', 404);
        if (op === 'HeadObjectCommand') return { ...row, ContentLength: row.data.length };
        if (op === 'GetObjectCommand') {
            if (input.IfMatch && input.IfMatch !== row.ETag) throw fail('PreconditionFailed', 412);
            let data = row.data;
            if (input.Range) {
                const [, from, to] = input.Range.match(/^bytes=(\d+)-(\d*)$/);
                data = data.subarray(Number(from), to ? Number(to) + 1 : undefined);
            }
            const result = { ...row, ContentLength: data.length, Body: Readable.from([data]) };
            this.afterGet?.(Key); return result;
        }
        throw new Error(`Unexpected command ${op}`);
    }
    destroy() {}
}
