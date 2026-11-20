import { promises as fs } from 'node:fs';
import { createHash, randomUUID } from 'node:crypto';
import { Worker } from 'node:worker_threads';
import { S3Client, ListObjectsV2Command, HeadObjectCommand, GetObjectCommand, PutObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import StorageBackend from '../StorageBackend.js';
import { checksumBuffer, checksumStream } from '../../utils/checksum.js';
import { detectMimeFromHead } from '../../utils/mime.js';
import { PART, classify, uploadFile } from './upload.js';

const MAX_PUT = 5 * 1024 ** 3;
const PROBES = '.canvas-probe/';
// `<blob key>.json` is the optional sidecar of a blob in `mode: 'cas'`.
const SIDECAR = '.json';
const SHA256 = /^[a-f0-9]{64}$/;
const BLOB_KEY = /^(?:.*\/)?([a-f0-9]{64})$/;
const locked = error => status(error) === 403 || ['AccessDenied', 'InvalidRequest', 'ObjectLocked'].includes(error?.name);
const fault = (code, message, retryable = false) => Object.assign(new Error(message), { code, retryable });
const status = error => error?.$metadata?.httpStatusCode;
const missing = error => status(error) === 404 && !['NoSuchBucket', 'NotFoundBucket'].includes(error.name);
const bodyBuffer = async body => {
    if (Buffer.isBuffer(body)) return body;
    const chunks = [];
    for await (const chunk of body) chunks.push(Buffer.from(chunk));
    return Buffer.concat(chunks);
};

/** One existing bucket/prefix in one of two modes, fixed when it is connected.
 * `mode: 'path'` (default) holds objects under chosen keys: listed, scanned,
 * conditional creates. `mode: 'cas'` makes the prefix a content store: every key
 * is `<aa>/<bb>/<sha256>` directly under it, nothing is listed or scanned, and
 * writes are unconditional because the same key always holds the same bytes.
 * Removal is on unless `permanentDelete` is false. File uploads run on the
 * backend's own thread (`upload-worker.js`). ETags and
 * version IDs are observation tokens, never content checksums or stable
 * inodes. No implicit host IAM credentials. */
export default class S3Backend extends StorageBackend {
    #client; #bucket; #prefix; #timer; #polling; #rows = new Map(); #hashes = new Map(); #writeCheck; #checksumCheck;
    #clientConfig = null; #worker = null; #jobs = new Map(); #jobSeq = 0;
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
        const clientConfig = {
            region: config.region || 'us-east-1', endpoint: endpoint?.href,
            forcePathStyle: config.forcePathStyle ?? Boolean(endpoint), credentials,
            requestChecksumCalculation: 'WHEN_REQUIRED', responseChecksumValidation: 'WHEN_REQUIRED',
            requestHandler: { connectionTimeout: 10_000, requestTimeout: 120_000 }, maxAttempts: 3,
        };
        this.#client = config.client || new S3Client(clientConfig);
        // An injected client or a credential provider function cannot cross
        // into a thread; those uploads run on the calling thread.
        if (!config.client && typeof credentials !== 'function') this.#clientConfig = clientConfig;
    }
    /** Run one file upload on the backend's upload thread. */
    #upload(job) {
        const input = { Bucket: this.#bucket, ...job };
        if (!this.#clientConfig) return uploadFile(this.#client, input);
        if (!this.#worker) {
            const worker = new Worker(new URL('./upload-worker.js', import.meta.url), { workerData: { client: this.#clientConfig } });
            worker.unref();
            const lost = error => {
                if (this.#worker === worker) this.#worker = null;
                for (const [id, { reject }] of this.#jobs) { this.#jobs.delete(id); reject(fault('UPLOAD_INTERRUPTED', `S3 upload thread stopped: ${error?.message || 'exit'}`, true)); }
            };
            worker.on('message', ({ id, result, error }) => {
                const pending = this.#jobs.get(id);
                if (!pending) return;
                this.#jobs.delete(id);
                if (error) pending.reject(Object.assign(new Error(error.message), error));
                else pending.resolve(result);
            });
            worker.on('error', lost);
            worker.on('exit', () => lost());
            this.#worker = worker;
        }
        return new Promise((resolve, reject) => {
            const id = ++this.#jobSeq;
            this.#jobs.set(id, { resolve, reject });
            this.#worker.postMessage({ id, job: input });
        });
    }
    static getCapabilities(config = {}) {
        const cas = config.mode === 'cas', writable = config.readOnly !== true;
        return { ...super.getCapabilities(config), delete: writable && config.permanentDelete !== false,
            canEnumerate: !cas, remote: true, paths: !cas, upload: !cas && writable, blobs: cas && writable };
    }
    get #cas() { return this.config.mode === 'cas'; }
    /** A content store is never listed; a scan is no evidence about its keys. */
    listed() { return !this.#cas; }
    get transport() { return 's3'; }
    get uploadConcurrency() { return 6; }
    get watching() { return Boolean(this.#timer); }
    #safe(key) { return ![...key].some(c => c < ' ' || c === '\\') && !key.split('/').some(s => !s || s === '.' || s === '..'); }
    #key(key) {
        if (typeof key !== 'string' || !this.#safe(key) || key.startsWith(PROBES)) throw fault('INVALID_KEY', 'Invalid or reserved S3 object key');
        return this.#prefix + key;
    }
    nativeUrl(key) { return `s3://${this.#bucket}/${this.#key(key).split('/').map(encodeURIComponent).join('/')}`; }
    async #send(Command, input) {
        try { return await this.#client.send(new Command({ Bucket: this.#bucket, ...input })); }
        catch (error) { throw classify(error); }
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
        return { ...await this.#writeCheck, sha256: await this.#nativeSha256() };
    }
    /** Whether the endpoint checks `x-amz-checksum-sha256` on upload: it must
     * refuse a wrong value and report the right one back. Anything else,
     * including an endpoint that ignores the header, means Content-MD5. */
    #nativeSha256() {
        if (!this.#checksumCheck) this.#checksumCheck = this.#probeChecksum();
        return this.#checksumCheck;
    }
    async #probeChecksum() {
        const Key = this.#prefix + PROBES + randomUUID(), Body = Buffer.from('canvas-probe');
        const right = createHash('sha256').update(Body).digest('base64'), wrong = createHash('sha256').update('not the body').digest('base64');
        const versions = [];
        try {
            try {
                versions.push((await this.#send(PutObjectCommand, { Key, Body, ChecksumSHA256: wrong })).VersionId);
                return false;
            } catch (error) { if (status(error) !== 400) return false; }
            versions.push((await this.#send(PutObjectCommand, { Key, Body, ChecksumSHA256: right })).VersionId);
            return (await this.#send(HeadObjectCommand, { Key, ChecksumMode: 'ENABLED' })).ChecksumSHA256 === right;
        } catch { return false; }
        finally {
            let hidden = false;
            for (const VersionId of versions.length ? versions : [undefined]) {
                const removed = await this.#send(DeleteObjectCommand, { Key, ...(VersionId ? { VersionId } : {}) }).then(() => true, () => false);
                if (!removed && !hidden) { hidden = true; await this.#send(DeleteObjectCommand, { Key }).catch(() => {}); }
            }
        }
    }
    async #probeWrites() {
        const Key = this.#prefix + PROBES + randomUUID();
        const versions = new Set();
        let outcome;
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
            outcome = { ok: true, conditionalCreate: true };
        } finally {
            // Exact random keys only, including any versions created by a
            // nonconforming endpoint; never sweep a user prefix.
            if (!versions.size) versions.add(undefined);
            let hidden = false;
            for (const VersionId of versions) {
                const removed = await this.#send(DeleteObjectCommand, { Key, ...(VersionId ? { VersionId } : {}) }).then(() => true, error => {
                    if (VersionId && locked(error)) return false;
                    if (outcome) outcome = { error };
                    return true;
                });
                // Object Lock retention keeps the probe version until it
                // expires; hide it behind one delete marker instead.
                if (!removed && !hidden) { hidden = true; await this.#send(DeleteObjectCommand, { Key }).catch(() => {}); }
            }
        }
        if (outcome.error) throw outcome.error;
        return outcome;
    }
    #row(key, result) {
        return { backend: this.name, key, size: Number(result.ContentLength ?? result.Size ?? 0),
            modified: result.LastModified?.getTime() ?? null, mimeType: result.ContentType || 'application/octet-stream',
            id: JSON.stringify([result.ETag || null, result.VersionId || null]), etag: result.ETag, versionId: result.VersionId,
            // Recorded by this driver on an upload the endpoint checked (its
            // SHA-256 or Content-MD5); absent on objects written by anything else.
            sha256: SHA256.test(result.Metadata?.sha256 || '') ? result.Metadata.sha256 : null };
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
        if (this.#cas) throw fault('INVALID_KEY', 'A content store takes files through createFrom');
        const Body = Buffer.isBuffer(data) ? data : Buffer.from(data);
        const native = (await this.verifyWrites()).sha256;
        const { sha256, md5 } = checksumBuffer(Body, native ? ['sha256'] : ['sha256', 'md5']);
        await this.#send(PutObjectCommand, { Key: this.#key(key), Body, ContentLength: Body.length, ContentType: await detectMimeFromHead(Body.subarray(0, 4096), key),
            Metadata: { sha256 }, IfNoneMatch: '*',
            ...(native ? { ChecksumSHA256: Buffer.from(sha256, 'hex').toString('base64') } : { ContentMD5: Buffer.from(md5, 'hex').toString('base64') }) });
        this.#hashes.delete(key);
        return this.stat(key);
    }
    async commit(key, file) { return this.createFrom(key, file); }
    /** Upload a local file. `sha256` (hex) is its checksum when the caller
     * already has it. Returns the row of the object the endpoint accepted. */
    async createFrom(key, file, { sha256 } = {}) {
        const Key = this.#key(key);
        if (!this.canWrite) throw fault('READ_ONLY', 'S3 backend is read-only');
        const { size } = await fs.stat(file);
        if (this.#cas) return this.#createBlob(key, Key, file, size, sha256);
        if (size > MAX_PUT) throw fault('OBJECT_TOO_LARGE', 'S3 uploads to a chosen path support objects up to 5 GiB');
        const handle = await fs.open(file, 'r');
        let head;
        try {
            const buffer = Buffer.alloc(4096);
            const { bytesRead } = await handle.read(buffer, 0, buffer.length, 0);
            head = buffer.subarray(0, bytesRead);
        } finally { await handle.close(); }
        const native = (await this.verifyWrites()).sha256;
        const ContentType = await detectMimeFromHead(head, key);
        const result = await this.#upload({ Key, file, size, ContentType, sha256, native, IfNoneMatch: '*' });
        this.#hashes.delete(key);
        return this.#accepted(key, size, ContentType, result);
    }
    /** A blob's key ends in its SHA-256, so the same key always holds the same
     * bytes: an object already recorded with that checksum is the upload, and
     * a new one costs one HEAD and one PUT. Large blobs go up in parts. */
    async #createBlob(key, Key, file, size, sha256) {
        const named = BLOB_KEY.exec(key)?.[1];
        if (!named || (sha256 && sha256 !== named)) throw fault('INVALID_KEY', 'A content store names every object by its SHA-256');
        const existing = await this.stat(key);
        if (existing?.size === size && existing.sha256 === named) return existing;
        const multipart = size > PART;
        const native = !multipart && await this.#nativeSha256();
        const result = await this.#upload({ Key, file, size, ContentType: 'application/octet-stream', sha256: named, native, multipart });
        return this.#accepted(key, size, 'application/octet-stream', result);
    }
    /** The row of an upload the endpoint just accepted. It checked the bytes
     * against the checksum sent with them, so no HEAD follows. */
    #accepted(key, size, ContentType, result) {
        return this.#row(key, { ContentLength: size, LastModified: new Date(), ContentType,
            ETag: result.ETag, VersionId: result.VersionId, Metadata: { sha256: result.sha256 } });
    }
    /** Write the JSON sidecar of a blob once; an existing one is kept. */
    async writeSidecar(blobKey, document) {
        if (!this.canWrite) throw fault('READ_ONLY', 'S3 backend is read-only');
        const key = blobKey + SIDECAR;
        if (await this.stat(key)) return false;
        const Body = Buffer.from(JSON.stringify(document));
        await this.#send(PutObjectCommand, { Key: this.#key(key), Body, ContentLength: Body.length, ContentType: 'application/json',
            ContentMD5: createHash('md5').update(Body).digest('base64') });
        return true;
    }
    async readSidecar(blobKey) {
        const data = await this.get(blobKey + SIDECAR);
        return data ? JSON.parse(data.toString()) : null;
    }
    async delete(key) {
        if (!this.canDelete) throw fault('DELETE_DISABLED', 'Deletes are turned off for this S3 backend');
        await this.#send(DeleteObjectCommand, { Key: this.#key(key) });
        // A blob takes its sidecar with it.
        if (this.#cas && !key.endsWith(SIDECAR)) await this.#send(DeleteObjectCommand, { Key: this.#key(key + SIDECAR) }).catch(() => {});
        this.#hashes.delete(key);
        return true;
    }
    async *list({ prefix = '' } = {}) {
        if (this.#cas) return;
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
    /** What the prefix holds in the bucket: every current object under it, in
     * both modes, including sidecars and keys this driver would not index. One
     * LIST request per 1000 objects; `onProgress` gets the running totals after
     * each page. Noncurrent versions and unfinished multipart uploads are
     * billed by the provider but appear in no object listing. */
    async usage({ onProgress } = {}) {
        let bytes = 0, objects = 0, token;
        const seen = new Set();
        for (;;) {
            const result = await this.#send(ListObjectsV2Command, { Prefix: this.#prefix, ContinuationToken: token });
            for (const item of result.Contents || []) { bytes += Number(item.Size) || 0; objects++; }
            onProgress?.({ bytes, objects });
            if (!result.IsTruncated) return { bytes, objects };
            token = result.NextContinuationToken;
            if (!token || seen.has(token)) throw fault('INVALID_PAGINATION', 'S3 returned an incomplete listing without a new continuation token');
            seen.add(token);
        }
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
    async stop() {
        clearInterval(this.#timer); this.#timer = null;
        await this.#polling;
        const worker = this.#worker; this.#worker = null;
        await worker?.terminate();
        this.#client.destroy?.();
    }
}
