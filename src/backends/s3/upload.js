import { createReadStream } from 'node:fs';
import { createHash } from 'node:crypto';
import { PutObjectCommand, CreateMultipartUploadCommand, UploadPartCommand, CompleteMultipartUploadCommand,
    AbortMultipartUploadCommand } from '@aws-sdk/client-s3';
import { checksumFile } from '../../utils/checksum.js';

export const PART = 64 * 1024 ** 2;
const MAX_PARTS = 10_000;
const SHA256 = /^[a-f0-9]{64}$/;
const status = error => error?.$metadata?.httpStatusCode;

/** Driver-level meaning of an SDK failure: a failed precondition is `EEXIST`,
 * throttling and server faults are retryable. */
export function classify(error) {
    if (status(error) === 412) return Object.assign(new Error('S3 precondition failed: destination exists or source changed'), { code: 'EEXIST' });
    if (status(error) === 409 || status(error) >= 500 || ['SlowDown', 'RequestTimeout', 'TimeoutError'].includes(error.name)) error.retryable = true;
    return error;
}

/**
 * Upload one local file to one key and return `{ ETag, VersionId, sha256 }`.
 *
 * `native` says the endpoint checks `x-amz-checksum-sha256`: the upload then
 * carries the caller's `sha256` and the file is not hashed here. Otherwise the
 * file is read once for its MD5 (sent as Content-MD5) and its SHA-256, which
 * must equal the caller's. `multipart` sends the file in parts, each with its
 * own MD5: a multipart SHA-256 is a checksum of part checksums, not the file's.
 * The file's SHA-256 is recorded as object metadata either way.
 */
export async function uploadFile(client, { Bucket, Key, file, size, ContentType, sha256, native = false, multipart = false, IfNoneMatch }) {
    const send = async (Command, input) => {
        try { return await client.send(new Command({ Bucket, Key, ...input })); }
        catch (error) { throw classify(error); }
    };
    const known = SHA256.test(sha256 || '') ? sha256 : null;
    const corrupt = actual => Object.assign(new Error(`Upload source does not hash to its recorded SHA-256 (${actual})`), { code: 'SOURCE_CORRUPT' });
    if (!multipart) {
        const sums = native && known ? { sha256: known } : await checksumFile(file, ['md5', 'sha256']);
        if (known && sums.sha256 !== known) throw corrupt(sums.sha256);
        const Body = createReadStream(file);
        try {
            const result = await send(PutObjectCommand, { Body, ContentLength: size, ContentType, Metadata: { sha256: sums.sha256 },
                ...(native ? { ChecksumSHA256: Buffer.from(sums.sha256, 'hex').toString('base64') }
                    : { ContentMD5: Buffer.from(sums.md5, 'hex').toString('base64') }),
                ...(IfNoneMatch ? { IfNoneMatch } : {}) });
            return { ETag: result.ETag, VersionId: result.VersionId, sha256: sums.sha256 };
        } finally { Body.destroy(); }
    }
    const partSize = Math.max(PART, Math.ceil(size / MAX_PARTS));
    // The parts are hashed as they are read for their MD5, so the whole file's
    // SHA-256 is checked in the same pass before the upload is completed.
    const whole = createHash('sha256');
    const { UploadId } = await send(CreateMultipartUploadCommand, { ContentType, ...(known ? { Metadata: { sha256: known } } : {}) });
    try {
        const Parts = [];
        for (let start = 0, PartNumber = 1; start < size; start += partSize, PartNumber++) {
            const end = Math.min(start + partSize, size) - 1;
            const md5 = createHash('md5');
            for await (const chunk of createReadStream(file, { start, end })) { md5.update(chunk); whole.update(chunk); }
            const Body = createReadStream(file, { start, end });
            try {
                const part = await send(UploadPartCommand, { UploadId, PartNumber, Body, ContentLength: end - start + 1, ContentMD5: md5.digest('base64') });
                Parts.push({ PartNumber, ETag: part.ETag });
            } finally { Body.destroy(); }
        }
        const actual = whole.digest('hex');
        if (known && actual !== known) throw corrupt(actual);
        const result = await send(CompleteMultipartUploadCommand, { UploadId, MultipartUpload: { Parts } });
        return { ETag: result.ETag, VersionId: result.VersionId, sha256: actual };
    } catch (error) {
        await send(AbortMultipartUploadCommand, { UploadId }).catch(() => {});
        throw error;
    }
}
