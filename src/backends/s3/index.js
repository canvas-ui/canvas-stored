import { createReadStream, promises as fs } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { S3Client, ListObjectsV2Command, HeadObjectCommand, GetObjectCommand, PutObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import StorageBackend from '../StorageBackend.js';
import { checksumBuffer, checksumFile, checksumStream } from '../../utils/checksum.js';
import { detectMimeFromHead } from '../../utils/mime.js';

const MAX_PUT = 5 * 1024 ** 3;
const PROBES = '.canvas-probe/';
const fault = (code, message, retryable = false) => Object.assign(new Error(message), { code, retryable });
const status = error => error?.$metadata?.httpStatusCode;
const missing = error => status(error) === 404 && !['NoSuchBucket', 'NotFoundBucket'].includes(error.name);
const bodyBuffer = async body => {
    if (Buffer.isBuffer(body)) return body;
    const chunks = [];
    for await (const chunk of body) chunks.push(Buffer.from(chunk));
    return Buffer.concat(chunks);
};

/** One existing bucket/prefix. Creates are conditional; destructive policy
 * capabilities stay disabled. ETags/version IDs are observation tokens, never
 * content checksums or stable inodes. No implicit host IAM credentials. */
export default class S3Backend extends StorageBackend {
    #client; #bucket; #prefix; #timer; #polling; #rows = new Map(); #hashes = new Map(); #writeCheck;
    constructor(name, config = {}) {
        super(name, config);
        this.type = 'remote';
        if (!config.bucket || /[\s/\\]/.test(config.bucket)) throw fault('INVALID_CONFIG', 'S3 bucket is required and cannot contain slashes or whitespace');
        this.#bucket = String(config.bucket);
        this.#prefix = String(config.prefix || '').replace(/^\/+|\/+$/g, '');
        if (this.#prefix && !this.#safe(this.#prefix)) throw fault('INVALID_CONFIG', 'Invalid S3 prefix');
        if (this.#prefix) this.#prefix += '/';
        let endpoint;
        if (config.endpoint) {
            endpoint = new URL(config.endpoint);
            if (!['http:', 'https:'].includes(endpoint.protocol) || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) throw fault('INVALID_CONFIG', 'S3 endpoint must be an HTTP(S) URL without credentials, query or fragment');
        }
        const credentials = config.credentials || (config.accessKeyId && config.secretAccessKey ? {
            accessKeyId: config.accessKeyId, secretAccessKey: config.secretAccessKey,
            ...(config.sessionToken ? { sessionToken: config.sessionToken } : {}),
        } : null);
        if (!credentials && !config.client) throw fault('INVALID_CONFIG', 'S3 accessKeyId and secretAccessKey are required');
        this.#client = config.client || new S3Client({
            region: config.region || 'us-east-1', endpoint: endpoint?.href,
            forcePathStyle: config.forcePathStyle ?? Boolean(endpoint), credentials,
            requestChecksumCalculation: 'WHEN_REQUIRED', responseChecksumValidation: 'WHEN_REQUIRED',
            requestHandler: { connectionTimeout: 10_000, requestTimeout: 120_000 }, maxAttempts: 3,
        });
    }
    get capabilities() { return { read: true, write: this.config.readOnly !== true, delete: this.config.readOnly !== true && this.config.permanentDelete === true, canEnumerate: true, remote: true }; }
    get transport() { return 's3'; }
    get watching() { return Boolean(this.#timer); }
    #safe(key) { return !/[\x00-\x1f\\]/.test(key) && !key.split('/').some(s => !s || s === '.' || s === '..'); }
    #key(key) {
        if (typeof key !== 'string' || !this.#safe(key) || key.startsWith(PROBES)) throw fault('INVALID_KEY', 'Invalid or reserved S3 object key');
        return this.#prefix + key;
    }
    nativeUrl(key) { return `s3://${this.#bucket}/${this.#key(key).split('/').map(encodeURIComponent).join('/')}`; }
    async #send(Command, input) {
        try { return await this.#client.send(new Command({ Bucket: this.#bucket, ...input })); }
        catch (error) {
            if (status(error) === 412) throw fault('EEXIST', 'S3 precondition failed: destination exists or source changed');
            if (status(error) === 409 || status(error) >= 500 || ['SlowDown', 'RequestTimeout', 'TimeoutError'].includes(error.name)) error.retryable = true;
            throw error;
        }
    }
    async verifyRoot() {
        try {
            // Prefix-scoped permissions need not permit HeadBucket.
            await this.#send(ListObjectsV2Command, { Prefix: this.#prefix, MaxKeys: 1 });
            return { ok: true, fsid: null };
        } catch (error) { return { ok: false, reason: error.name || error.code || 'unreachable', error: error.message, fsid: null }; }
    }
    /** Some endpoints ignore conditional headers. Probe a unique private key;
     * never fall back to a racy HEAD followed by an unconditional user write. */
    async verifyWrites() {
        if (!this.canWrite) throw fault('READ_ONLY', 'S3 backend is read-only');
        if (!this.#writeCheck) this.#writeCheck = this.#probeWrites().catch(error => { this.#writeCheck = null; throw error; });
        return this.#writeCheck;
    }
    async #probeWrites() {
        const Key = this.#prefix + PROBES + randomUUID();
        const versions = new Set();
        try {
            const first = await this.#send(PutObjectCommand, { Key, Body: Buffer.from('canvas-probe'), IfNoneMatch: '*' });
            versions.add(first.VersionId);
            let rejected = false;
            try {
                const second = await this.#send(PutObjectCommand, { Key, Body: Buffer.from('must-not-replace'), IfNoneMatch: '*' });
                versions.add(second.VersionId);
            } catch (error) { if (error.code === 'EEXIST') rejected = true; else throw error; }
            const found = await this.#send(GetObjectCommand, { Key });
            const bytes = await bodyBuffer(found.Body);
            if (!rejected || bytes.toString() !== 'canvas-probe') throw fault('CONDITIONAL_WRITES_UNSUPPORTED', 'S3 endpoint does not enforce If-None-Match; user-key writes are disabled');
            return { ok: true, conditionalCreate: true };
        } finally {
            // Exact random keys only, including any versions created by a
            // nonconforming endpoint; never sweep a user prefix.
            if (!versions.size) versions.add(undefined);
            for (const VersionId of versions) await this.#send(DeleteObjectCommand, { Key, ...(VersionId ? { VersionId } : {}) });
        }
    }
    #row(key, result) {
        return { backend: this.name, key, size: Number(result.ContentLength ?? result.Size ?? 0),
            modified: result.LastModified?.getTime() ?? null, mimeType: result.ContentType || 'application/octet-stream',
            id: JSON.stringify([result.ETag || null, result.VersionId || null]), etag: result.ETag, versionId: result.VersionId };
    }
    async stat(key) {
        try { return this.#row(key, await this.#send(HeadObjectCommand, { Key: this.#key(key) })); }
        catch (error) { if (missing(error)) return null; throw error; }
    }
    async get(key, { stream = false, ifMatch, versionId } = {}) {
        try {
            const result = await this.#send(GetObjectCommand, { Key: this.#key(key), ...(ifMatch ? { IfMatch: ifMatch } : {}), ...(versionId ? { VersionId: versionId } : {}) });
            return stream ? result.Body : bodyBuffer(result.Body);
        } catch (error) { if (missing(error)) return null; throw error; }
    }
    async getRange(key, { start = 0, end } = {}) {
        if (!Number.isSafeInteger(start) || start < 0 || (end != null && (!Number.isSafeInteger(end) || end < start))) throw fault('INVALID_RANGE', 'Invalid byte range');
        try { return (await this.#send(GetObjectCommand, { Key: this.#key(key), Range: `bytes=${start}-${end ?? ''}` })).Body; }
        catch (error) { if (missing(error)) return null; throw error; }
    }
    async put(key, data) {
        if (!Buffer.isBuffer(data) && typeof data !== 'string') throw fault('SIZE_REQUIRED', 'Use createFrom for streamed S3 uploads with a known size');
        const buffer = Buffer.isBuffer(data) ? data : Buffer.from(data);
        return this.#create(key, buffer, buffer.length, checksumBuffer(buffer, ['md5']), await detectMimeFromHead(buffer.subarray(0, 4096), key));
    }
    async commit(key, file) { return this.createFrom(key, file); }
    async createFrom(key, file) {
        const { size } = await fs.stat(file);
        if (size > MAX_PUT) throw fault('OBJECT_TOO_LARGE', 'S3 uploads currently support objects up to 5 GiB; multipart upload is not enabled');
        const handle = await fs.open(file, 'r');
        let head;
        try {
            const buffer = Buffer.alloc(4096);
            const { bytesRead } = await handle.read(buffer, 0, buffer.length, 0);
            head = buffer.subarray(0, bytesRead);
        } finally { await handle.close(); }
        const sums = await checksumFile(file, ['md5']);
        const input = createReadStream(file);
        try { return await this.#create(key, input, size, sums, await detectMimeFromHead(head, key)); }
        finally { input.destroy(); }
    }
    async #create(key, Body, ContentLength, checksums, ContentType) {
        const Key = this.#key(key);
        if (ContentLength > MAX_PUT) throw fault('OBJECT_TOO_LARGE', 'S3 uploads currently support objects up to 5 GiB');
        await this.verifyWrites();
        await this.#send(PutObjectCommand, { Key, Body, ContentLength, ContentType, ContentMD5: Buffer.from(checksums.md5, 'hex').toString('base64'), IfNoneMatch: '*' });
        this.#hashes.delete(key);
        return this.stat(key);
    }
    async delete(key) {
        if (!this.canDelete) throw fault('RECOVERY_UNAVAILABLE', 'S3 removal requires explicit permanentDelete; recoverable mirror removal is not implemented');
        await this.#send(DeleteObjectCommand, { Key: this.#key(key) });
        this.#hashes.delete(key);
        return true;
    }
    async *list({ prefix = '' } = {}) {
        if (prefix && !this.#safe(prefix.replace(/\/$/, ''))) throw fault('INVALID_KEY', 'Invalid list prefix');
        let token;
        const seen = new Set();
        do {
            const result = await this.#send(ListObjectsV2Command, { Prefix: this.#prefix + prefix, ContinuationToken: token, EncodingType: 'url' });
            for (const item of result.Contents || []) {
                const absolute = result.EncodingType === 'url' ? decodeURIComponent(item.Key) : item.Key;
                if (typeof absolute !== 'string' || !absolute.startsWith(this.#prefix)) throw fault('INVALID_LIST', 'S3 listing escaped the configured prefix');
                const key = absolute.slice(this.#prefix.length);
                if (!key || key.endsWith('/') || key.startsWith(PROBES)) continue;
                if (!this.#safe(key)) throw fault('UNSUPPORTED_KEY', `S3 key cannot be represented safely: ${key}`);
                yield this.#row(key, item);
            }
            if (!result.IsTruncated) break;
            token = result.NextContinuationToken;
            if (!token || seen.has(token)) throw fault('INVALID_PAGINATION', 'S3 returned an incomplete listing without a new continuation token');
            seen.add(token);
        } while (true);
    }
    async shape() {
        const dirs = new Set(); let files = 0;
        for await (const row of this.list()) {
            files++;
            const parts = row.key.split('/'); parts.pop();
            while (parts.length) { dirs.add(parts.join('/')); parts.pop(); }
        }
        return { dirs: [...dirs], files };
    }
    async scan({ algorithms = ['sha256'], onFile } = {}) {
        const files = [], errors = { root: null, dirs: [], files: [] };
        this.emit('scan:start', { backend: this.name });
        try {
            for await (const listed of this.list()) {
                let row = { ...listed, checksums: null };
                try {
                    const before = await this.stat(listed.key);
                    if (!before) throw fault('SOURCE_CHANGED', 'S3 object disappeared during scan');
                    const cached = this.#hashes.get(before.key);
                    let checksums = cached?.id === before.id && cached.size === before.size && cached.modified === before.modified && algorithms.every(a => cached.checksums[a]) ? cached.checksums : null;
                    let mimeType = checksums ? cached.mimeType : before.mimeType;
                    if (!checksums) {
                        const stream = await this.get(before.key, { stream: true, ifMatch: before.etag, versionId: before.versionId });
                        if (!stream) throw fault('SOURCE_CHANGED', 'S3 object disappeared during scan');
                        const heads = []; let remaining = 4096;
                        checksums = await checksumStream((async function* () {
                            for await (const chunk of stream) {
                                if (remaining) { const head = Buffer.from(chunk).subarray(0, remaining); heads.push(head); remaining -= head.length; }
                                yield chunk;
                            }
                        })(), algorithms);
                        if (!mimeType || mimeType === 'application/octet-stream') mimeType = await detectMimeFromHead(Buffer.concat(heads), before.key);
                        const after = await this.stat(before.key);
                        if (!after || after.id !== before.id || after.size !== before.size || after.modified !== before.modified) throw fault('SOURCE_CHANGED', 'S3 object changed during scan');
                    }
                    row = { ...before, checksums, mimeType };
                    this.#hashes.set(row.key, row);
                } catch (error) { errors.files.push({ key: listed.key, code: error.code || error.name }); }
                files.push(row);
                if (onFile) await onFile(row);
            }
        } catch (error) { errors.root = error.code || error.name || 'unreachable'; }
        const complete = !errors.root && !errors.files.length;
        if (complete) {
            const present = new Set(files.map(row => row.key));
            for (const key of this.#hashes.keys()) if (!present.has(key)) this.#hashes.delete(key);
        }
        this.emit('scan:complete', { backend: this.name, count: files.length, complete, errors });
        return { files, complete, errors, fsid: null };
    }
    async poll() {
        if (this.#polling) return this.#polling;
        this.#polling = (async () => {
            const snapshot = await this.scan();
            if (!snapshot.complete) return false;
            const next = new Map(snapshot.files.map(row => [row.key, row]));
            for (const [key, row] of next) {
                const previous = this.#rows.get(key);
                if (!previous || previous.id !== row.id || previous.modified !== row.modified) this.emit(previous ? 'file:change' : 'file:add', { ...row, stats: { size: row.size, mtime: row.modified } });
            }
            for (const key of this.#rows.keys()) if (!next.has(key)) this.emit('file:unlink', { backend: this.name, key });
            this.#rows = next;
            return true;
        })();
        try { return await this.#polling; } finally { this.#polling = null; }
    }
    async watch() {
        if (this.#timer) return true;
        this.#timer = setInterval(() => { this.poll().catch(error => this.emit('watch:error', error)); }, Math.max(5000, Number(this.config.pollInterval) || 60000));
        this.#timer.unref?.();
        await this.poll();
        return true;
    }
    async stop() { clearInterval(this.#timer); this.#timer = null; await this.#polling; this.#client.destroy?.(); }
}
