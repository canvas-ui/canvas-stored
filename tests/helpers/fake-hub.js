import http from 'node:http';
import crypto from 'node:crypto';
import path from 'node:path';
import fs from 'fs-extra';
import { PassThrough } from 'node:stream';
import Stored from '../../src/index.js';

/**
 * The objects protocol (`canvas-server/docs/sync-protocol.md`) over a real
 * hub `Stored` instance, served by plain `node:http`: listing, change feed
 * (with 410), HEAD/GET with Range + ETag/If-None-Match, PUT with
 * preconditions and conflict headers (→ hub `writeObject`, or an in-memory
 * conflict inbox), DELETE, rename, mirror status reports, and `/ping`.
 *
 * Knobs for tests: `hub.offline = true` drops connections (transport error),
 * `hub.failNext.push(503)` answers the next request with that status,
 * `hub.calls` records `{ method, path, status }`.
 */
const REASON_STATUS = {
    'precondition-failed': [412, 'PRECONDITION_FAILED'],
    'checksum-mismatch': [422, 'CHECKSUM_MISMATCH'],
    'invalid-key': [400, 'INVALID_KEY'],
    'same-key': [400, 'INVALID_KEY'],
    'not-found': [404, 'NOT_FOUND'],
    'target-exists': [409, 'TARGET_EXISTS'],
    'unknown-backend': [404, 'BACKEND_NOT_FOUND'],
    'unsupported-backend': [400, 'UNSUPPORTED_BACKEND'],
    'read-only-target': [403, 'BACKEND_READ_ONLY'],
    'read-only-backend': [403, 'BACKEND_READ_ONLY'],
    'target-offline': [503, 'BACKEND_OFFLINE'],
    'source-offline': [503, 'BACKEND_OFFLINE'],
};

const envelope = (statusCode, payload, { message = 'OK', code = null, count } = {}) => ({
    status: statusCode < 400 ? 'success' : 'error',
    statusCode,
    message,
    payload,
    ...(count != null ? { count } : {}),
    ...(code ? { code } : {}),
});

export async function createFakeHub({ root, token = 'device-token', instanceId = 'hub-instance-1', backend = 'workspace:home', workspaceId = 'ws1', watch = true } = {}) {
    const home = path.join(root, 'home');
    await fs.ensureDir(home);
    const stored = new Stored({ root: path.join(root, '.stored'), checksums: ['sha256'] });
    stored.on('error', () => {});
    stored.addBackend(backend, { driver: 'file', root: home, watch, stabilityThreshold: 100 });
    let docSeq = 1000;
    const docIds = new Map();   // sha256 → docId (recycled-ish, never to be trusted by clients)
    const docIdFor = (sha) => { if (!docIds.has(sha)) docIds.set(sha, ++docSeq); return docIds.get(sha); };

    const hub = {
        url: null,
        stored,
        home,
        backend,
        workspaceId,
        instanceId,
        token,
        inbox: [],
        mirrors: new Map(),
        calls: [],
        failNext: [],
        offline: false,
        server: null,
        async close() { await new Promise((r) => hub.server.close(r)); await stored.stop(); },
        async put(key, source, options = {}) { return stored.writeObject(backend, key, source, { origin: 'other-device', ...options }); },
        async remove(key, options = {}) { return stored.removeObject(backend, key, { origin: 'other-device', ...options }); },
        async rename(from, to, options = {}) { return stored.renameObject(backend, from, to, { origin: 'other-device', ...options }); },
        async read(key) { return stored.getByUrl(`stored://${backend}/${key}`); },
        sha(key) { return stored.index.get(`${backend}:${key}`)?.checksums?.sha256 ?? null; },
        listKeys() { return stored.listObjects(backend, { limit: 100000 }).objects.map((o) => o.key); },
        puts(key = null) { return hub.calls.filter((c) => c.method === 'PUT' && (!key || c.key === key)); },
    };

    const send = (res, statusCode, body, headers = {}) => {
        const json = JSON.stringify(body);
        res.writeHead(statusCode, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(json), ...headers });
        res.end(json);
    };
    const fail = (res, result) => {
        const [statusCode, code] = REASON_STATUS[result?.reason] || [500, 'OBJECT_OPERATION_FAILED'];
        const payload = {};
        for (const k of ['current', 'expected', 'actual', 'key', 'detail']) if (result?.[k] !== undefined) payload[k] = result[k];
        send(res, statusCode, envelope(statusCode, Object.keys(payload).length ? payload : null, { message: `Object operation failed: ${result?.reason}`, code }));
    };
    const strip = (v) => (v == null ? undefined : String(v).trim());
    const parseMtime = (v) => { if (v == null || v === '') return null; const n = Number(v); if (Number.isFinite(n)) return n; const t = Date.parse(v); return Number.isFinite(t) ? t : null; };
    const readAll = (req) => new Promise((resolve, reject) => { const chunks = []; req.on('data', (c) => chunks.push(c)); req.on('end', () => resolve(Buffer.concat(chunks))); req.on('error', reject); });

    const objectHeaders = (meta, loc) => ({
        ETag: `"${meta.checksums.sha256}"`,
        'X-Canvas-Sha256': meta.checksums.sha256,
        'X-Canvas-Size': String(loc?.size ?? meta.size ?? 0),
        'X-Canvas-Mtime': loc?.mtime != null ? String(loc.mtime) : '',
        'X-Canvas-Doc-Id': String(docIdFor(meta.checksums.sha256)),
        ...(loc?.mtime != null ? { 'Last-Modified': new Date(loc.mtime).toUTCString() } : {}),
        'Accept-Ranges': 'bytes',
        'Content-Type': meta.mimeType || 'application/octet-stream',
    });

    const handle = async (req, res) => {
        const url = new URL(req.url, 'http://hub');
        const record = { method: req.method, path: url.pathname, status: null, key: null, headers: req.headers };
        hub.calls.push(record);
        const done = (code, body, headers) => { record.status = code; send(res, code, body, headers); };
        if (hub.failNext.length) {
            const status = hub.failNext.shift();
            await readAll(req).catch(() => {});
            return done(status, envelope(status, null, { message: 'injected failure', code: 'INJECTED' }));
        }
        if (url.pathname === '/rest/v2/ping') return done(200, envelope(200, { instanceId, version: 'fake' }));
        const auth = req.headers.authorization || '';
        if (auth !== `Bearer ${token}`) { await readAll(req).catch(() => {}); return done(401, envelope(401, null, { message: 'Unauthorized', code: 'UNAUTHORIZED' })); }

        const m = url.pathname.match(/^\/rest\/v2\/workspaces\/([^/]+)\/(.*)$/);
        if (!m || decodeURIComponent(m[1]) !== workspaceId) return done(404, envelope(404, null, { message: 'Not found', code: 'NOT_FOUND' }));
        const rest = m[2];

        const mirror = rest.match(/^mirrors\/([^/]+)\/status$/);
        if (mirror && req.method === 'POST') {
            const body = JSON.parse((await readAll(req)).toString() || '{}');
            hub.mirrors.set(decodeURIComponent(mirror[1]), { ...body, at: Date.now() });
            return done(200, envelope(200, { mirror: { deviceId: decodeURIComponent(mirror[1]), ...body }, head: stored.head() }));
        }

        const b = rest.match(/^backends\/file\/([^/]+)\/(objects|changes)(?:\/(.*))?$/);
        if (!b) return done(404, envelope(404, null, { message: 'Not found', code: 'NOT_FOUND' }));
        const address = decodeURIComponent(b[1]);
        if (address !== backend) return done(404, envelope(404, null, { message: 'Unknown backend', code: 'BACKEND_NOT_FOUND' }));
        const route = b[2];
        const tail = b[3] != null ? b[3].split('/').map(decodeURIComponent).join('/') : null;

        if (route === 'changes') {
            const since = Math.max(0, Number.parseInt(url.searchParams.get('since') || '0', 10) || 0);
            const limit = Math.min(1000, Number.parseInt(url.searchParams.get('limit') || '1000', 10) || 1000);
            const page = stored.changes({ backend, since, limit });
            if (page.cursorTooOld) return done(410, envelope(410, { since, head: page.head, oldest: page.oldest }, { message: 'Cursor too old', code: 'CURSOR_TOO_OLD' }));
            const changes = page.changes.map((c) => ({ seq: c.seq, ts: c.ts, op: c.op, key: c.key, ...(c.from ? { from: c.from } : {}), sha256: c.id?.startsWith('sha256:') ? c.id.slice(7) : null, size: c.size, mtime: c.mtime, ...(c.origin ? { origin: c.origin } : {}) }));
            return done(200, envelope(200, { changes, head: page.head, oldest: page.oldest, cursor: page.cursor }, { count: changes.length }));
        }

        if (tail == null && req.method === 'GET') {
            const page = stored.listObjects(backend, { prefix: url.searchParams.get('prefix') || '', after: url.searchParams.get('cursor') || null, limit: Math.min(1000, Number(url.searchParams.get('limit')) || 1000) });
            const objects = page.objects.map((o) => ({ key: o.key, sha256: o.checksums?.sha256 ?? null, size: o.size, mtime: o.mtime, mimeType: o.mimeType }));
            return done(200, envelope(200, { objects, cursor: page.cursor, head: stored.head() }, { count: objects.length }));
        }

        if (tail === 'rename' && req.method === 'POST') {
            const body = JSON.parse((await readAll(req)).toString() || '{}');
            const result = await stored.renameObject(backend, body.from, body.to, { ifMatch: strip(body.ifMatch), origin: strip(body.origin) ?? strip(req.headers['x-canvas-origin']) });
            if (!result.ok) { record.status = REASON_STATUS[result.reason]?.[0] ?? 500; return fail(res, result); }
            return done(200, envelope(200, { from: result.from, to: result.to, sha256: result.sha256, seq: result.seq, docId: docIdFor(result.sha256) }, { message: 'Object renamed' }));
        }

        const key = tail;
        record.key = key;
        if (!key) return done(400, envelope(400, null, { message: 'key required', code: 'INVALID_KEY' }));

        if (req.method === 'HEAD' || req.method === 'GET') {
            const meta = stored.index.get(`${backend}:${key}`);
            if (!meta) return done(404, envelope(404, null, { message: 'Not found', code: 'NOT_FOUND' }));
            const loc = meta.locations.find((l) => l.backend === backend && l.key === key);
            const headers = objectHeaders(meta, loc);
            const inm = req.headers['if-none-match'];
            if (inm && inm.replace(/^W\//, '').replace(/"/g, '') === meta.checksums.sha256) { record.status = 304; res.writeHead(304, headers); return res.end(); }
            if (req.method === 'HEAD') { record.status = 200; res.writeHead(200, { ...headers, 'Content-Length': String(loc?.size ?? meta.size ?? 0) }); return res.end(); }
            const size = loc?.size ?? meta.size ?? 0;
            const range = /^bytes=(\d*)-(\d*)$/.exec(req.headers.range || '');
            if (range) {
                let start = range[1] === '' ? null : Number(range[1]);
                let end = range[2] === '' ? null : Number(range[2]);
                if (start == null) { start = Math.max(0, size - end); end = size - 1; }
                else if (end == null || end >= size) end = size - 1;
                if (start > end || start >= size) { record.status = 416; res.writeHead(416, { 'Content-Range': `bytes */${size}` }); return res.end(); }
                const ranged = await stored.getRangeStreamByUrl(`stored://${backend}/${key}`, { start, end });
                record.status = 206;
                res.writeHead(206, { ...headers, 'Content-Range': `bytes ${start}-${end}/${size}`, 'Content-Length': String(end - start + 1) });
                return ranged.stream.pipe(res);
            }
            const stream = await stored.getStreamByUrl(`stored://${backend}/${key}`);
            record.status = 200;
            res.writeHead(200, { ...headers, 'Content-Length': String(size) });
            return stream.pipe(res);
        }

        if (req.method === 'DELETE') {
            const result = await stored.removeObject(backend, key, { ifMatch: strip(req.headers['if-match']), origin: strip(req.headers['x-canvas-origin']) });
            if (!result.ok) { record.status = REASON_STATUS[result.reason]?.[0] ?? 500; return fail(res, result); }
            return done(200, envelope(200, { key, sha256: result.sha256, seq: result.seq, docId: docIdFor(result.sha256) }, { message: 'Object deleted' }));
        }

        if (req.method === 'PUT') {
            const h = req.headers;
            if (h['x-canvas-conflict-of']) {
                const data = await readAll(req);
                const sha256 = crypto.createHash('sha256').update(data).digest('hex');
                const mode = String(h['x-canvas-conflict-mode'] || 'inbox').toLowerCase() === 'rename' ? 'rename' : 'inbox';
                const conflictOf = h['x-canvas-conflict-of'];
                const hubMeta = stored.index.get(`${backend}:${conflictOf}`);
                const entry = { docId: ++docSeq, key, conflictOf, mode, sha256, size: data.length, baseSha256: strip(h['x-canvas-base-sha256']) ?? null, device: strip(h['x-canvas-origin']) ?? null, deviceName: strip(h['x-canvas-device-name']) ?? null, mtime: parseMtime(h['x-canvas-mtime']), data, hubDocId: hubMeta ? docIdFor(hubMeta.checksums.sha256) : null, hubSha256: hubMeta?.checksums?.sha256 ?? null };
                if (mode === 'rename') {
                    const written = await stored.writeObject(backend, key, data, { ifNoneMatch: strip(h['if-none-match']), sha256: strip(h['x-canvas-sha256']), mtime: parseMtime(h['x-canvas-mtime']), origin: strip(h['x-canvas-origin']) });
                    if (!written.ok) { record.status = REASON_STATUS[written.reason]?.[0] ?? 500; return fail(res, written); }
                    entry.seq = written.seq;
                }
                hub.inbox.push(entry);
                const { data: _d, ...payload } = entry;
                return done(201, envelope(201, payload, { message: 'Conflict recorded' }));
            }
            const contentType = strip(h['content-type']);
            const pass = new PassThrough();
            req.pipe(pass);
            const result = await stored.writeObject(backend, key, pass, {
                ifMatch: strip(h['if-match']),
                ifNoneMatch: strip(h['if-none-match']),
                sha256: strip(h['x-canvas-sha256'])?.toLowerCase(),
                mtime: parseMtime(h['x-canvas-mtime']),
                origin: strip(h['x-canvas-origin']),
                mimeType: contentType && contentType !== 'application/octet-stream' ? contentType.split(';')[0].trim() : undefined,
            });
            if (!result.ok) { record.status = REASON_STATUS[result.reason]?.[0] ?? 500; return fail(res, result); }
            const payload = { key, sha256: result.sha256, size: result.size, mtime: result.mtime ?? null, seq: result.seq, docId: docIdFor(result.sha256), previous: result.previous ? { sha256: result.previous.checksums?.sha256 ?? null } : null, unchanged: result.unchanged === true };
            const created = !result.unchanged && !result.previous;
            return done(created ? 201 : 200, envelope(created ? 201 : 200, payload, { message: created ? 'Object created' : (result.unchanged ? 'Object unchanged' : 'Object replaced') }), { ETag: `"${result.sha256}"` });
        }
        return done(405, envelope(405, null, { message: 'Method not allowed', code: 'METHOD_NOT_ALLOWED' }));
    };

    hub.server = http.createServer((req, res) => {
        if (hub.offline) { req.socket.destroy(); return; }
        handle(req, res).catch((err) => {
            if (!res.headersSent) send(res, 500, envelope(500, null, { message: err.message, code: 'INTERNAL' }));
            else res.destroy();
        });
    });
    await new Promise((resolve) => hub.server.listen(0, '127.0.0.1', resolve));
    hub.url = `http://127.0.0.1:${hub.server.address().port}`;
    return hub;
}

export const sha256 = (data) => crypto.createHash('sha256').update(data).digest('hex');
export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
export async function waitFor(fn, { timeout = 10_000, interval = 25, label = 'condition' } = {}) {
    const until = Date.now() + timeout;
    for (;;) {
        const v = await fn();
        if (v) return v;
        if (Date.now() > until) throw new Error(`Timed out waiting for ${label}`);
        await sleep(interval);
    }
}
