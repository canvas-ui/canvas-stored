import crypto from 'crypto';

/**
 * In-memory stand-in for the subset of the Drive v3 REST API the gdrive
 * driver uses. Passed to the driver as `config.fetch`. Keeps a change log so
 * `changes.list` works, honours Range on media downloads, and implements the
 * resumable upload protocol (308 between chunks, file resource at the end).
 */
export class FakeDrive {
    files = new Map(); // id → { id, name, mimeType, parents, trashed, data(Buffer)|null, modifiedTime, createdTime }
    changes = [];      // { fileId }
    calls = [];        // { method, path }
    sessions = new Map(); // session id → { fileId|null, meta, chunks:[], received }
    tokenCalls = 0;
    failNextToken = false;
    #seq = 0;

    constructor() {
        this.rootId = 'root-id';
        this.files.set(this.rootId, this.#mk({ id: this.rootId, name: 'My Drive', mimeType: 'application/vnd.google-apps.folder', parents: [] }));
    }

    #mk({ id, name, mimeType, parents = [], data = null, trashed = false }) {
        const now = new Date().toISOString();
        return { id, name, mimeType, parents, data, trashed, modifiedTime: now, createdTime: now };
    }

    #id() { this.#seq += 1; return `f${String(this.#seq).padStart(4, '0')}`; }

    #touch(f) {
        f.modifiedTime = new Date(Date.now() + this.#seq).toISOString();
        this.changes.push({ fileId: f.id });
    }

    addFolder(name, parentId = this.rootId) {
        const f = this.#mk({ id: this.#id(), name, mimeType: 'application/vnd.google-apps.folder', parents: [parentId] });
        this.files.set(f.id, f);
        this.changes.push({ fileId: f.id });
        return f;
    }

    addFile(name, content, parentId = this.rootId, mimeType = 'application/octet-stream') {
        const f = this.#mk({ id: this.#id(), name, mimeType, parents: [parentId], data: Buffer.from(content) });
        this.files.set(f.id, f);
        this.changes.push({ fileId: f.id });
        return f;
    }

    addNativeDoc(name, parentId = this.rootId) {
        const f = this.#mk({ id: this.#id(), name, mimeType: 'application/vnd.google-apps.document', parents: [parentId] });
        this.files.set(f.id, f);
        this.changes.push({ fileId: f.id });
        return f;
    }

    rename(id, name) { const f = this.files.get(id); f.name = name; this.#touch(f); }
    move(id, parentId) { const f = this.files.get(id); f.parents = [parentId]; this.#touch(f); }
    update(id, content) { const f = this.files.get(id); f.data = Buffer.from(content); this.#touch(f); }
    trash(id) { const f = this.files.get(id); f.trashed = true; this.#touch(f); }
    remove(id) { this.files.delete(id); this.changes.push({ fileId: id, removed: true }); }

    resource(f) {
        const isFolder = f.mimeType === 'application/vnd.google-apps.folder';
        const native = f.mimeType.startsWith('application/vnd.google-apps.');
        const out = {
            id: f.id, name: f.name, mimeType: f.mimeType, parents: f.parents, trashed: f.trashed,
            modifiedTime: f.modifiedTime, createdTime: f.createdTime,
        };
        if (!isFolder && !native && f.data) {
            out.size = String(f.data.length);
            out.md5Checksum = crypto.createHash('md5').update(f.data).digest('hex');
            out.sha1Checksum = crypto.createHash('sha1').update(f.data).digest('hex');
            out.sha256Checksum = crypto.createHash('sha256').update(f.data).digest('hex');
        }
        return out;
    }

    #json(body, status = 200, headers = {}) {
        return new Response(body === null ? null : JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json', ...headers } });
    }

    #error(status, message) {
        return this.#json({ error: { code: status, message } }, status);
    }

    // Minimal `q` evaluator for the clauses the driver emits.
    #match(f, q) {
        if (!q) return true;
        const clauses = q.split(' and ').map((c) => c.trim());
        for (const c of clauses) {
            let m;
            if ((m = c.match(/^'(.+)' in parents$/))) { if (!f.parents.includes(m[1])) return false; continue; }
            if ((m = c.match(/^name = '(.*)'$/))) { if (f.name !== m[1].replace(/\\'/g, "'").replace(/\\\\/g, '\\')) return false; continue; }
            if ((m = c.match(/^mimeType = '(.*)'$/))) { if (f.mimeType !== m[1]) return false; continue; }
            if ((m = c.match(/^mimeType != '(.*)'$/))) { if (f.mimeType === m[1]) return false; continue; }
            if (c === 'trashed = false') { if (f.trashed) return false; continue; }
            throw new Error(`FakeDrive: unsupported q clause: ${c}`);
        }
        return true;
    }

    fetch = async (url, init = {}) => {
        const u = new URL(url);
        const method = (init.method || 'GET').toUpperCase();
        this.calls.push({ method, path: u.pathname + u.search });

        if (u.hostname === 'oauth2.googleapis.com') {
            this.tokenCalls += 1;
            if (this.failNextToken) { this.failNextToken = false; return this.#json({ error: 'invalid_grant', error_description: 'Token has been revoked' }, 400); }
            return this.#json({ access_token: `tok-${this.tokenCalls}`, expires_in: 3600 });
        }
        if (!String(init.headers?.Authorization || '').startsWith('Bearer tok-')) return this.#error(401, 'Invalid Credentials');

        // Resumable upload session
        if (u.pathname === '/upload/session') {
            const session = this.sessions.get(u.searchParams.get('id'));
            if (!session) return this.#error(404, 'Session not found');
            const range = init.headers['Content-Range'] || '';
            const body = init.body ? Buffer.from(init.body) : Buffer.alloc(0);
            const m = range.match(/^bytes (?:(\d+)-(\d+)|\*)\/(\d+|\*)$/);
            if (!m) return this.#error(400, `Bad Content-Range: ${range}`);
            if (m[1] != null && Number(m[1]) !== session.received) return this.#error(400, `Out-of-order chunk at ${m[1]}, expected ${session.received}`);
            session.chunks.push(body);
            session.received += body.length;
            if (m[3] === '*') return new Response(null, { status: 308, headers: { Range: `bytes=0-${session.received - 1}` } });
            if (Number(m[3]) !== session.received) return this.#error(400, 'Total mismatch');
            const data = Buffer.concat(session.chunks);
            let f;
            if (session.fileId) {
                f = this.files.get(session.fileId);
                f.data = data;
            } else {
                f = this.#mk({ id: this.#id(), name: session.meta.name, mimeType: init.headers['Content-Type'] || 'application/octet-stream', parents: session.meta.parents, data });
                this.files.set(f.id, f);
            }
            this.#touch(f);
            this.sessions.delete(u.searchParams.get('id'));
            return this.#json(this.resource(f));
        }

        if (u.pathname.startsWith('/upload/drive/v3/files')) {
            const fileId = u.pathname.split('/')[5] || null;
            if (fileId && !this.files.has(fileId)) return this.#error(404, 'File not found');
            const meta = init.body ? JSON.parse(init.body) : {};
            const id = `s${this.#id()}`;
            this.sessions.set(id, { fileId, meta, chunks: [], received: 0 });
            return new Response(null, { status: 200, headers: { Location: `https://www.googleapis.com/upload/session?id=${id}` } });
        }

        if (u.pathname === '/drive/v3/changes/startPageToken') return this.#json({ startPageToken: String(this.changes.length) });
        if (u.pathname === '/drive/v3/changes') {
            const from = Number(u.searchParams.get('pageToken')) || 0;
            const changes = this.changes.slice(from).map((c) => {
                const f = this.files.get(c.fileId);
                return { changeType: 'file', fileId: c.fileId, removed: !!c.removed, ...(f ? { file: this.resource(f) } : {}) };
            });
            return this.#json({ changes, newStartPageToken: String(this.changes.length) });
        }

        if (u.pathname === '/drive/v3/files' && method === 'GET') {
            const q = u.searchParams.get('q');
            const files = [...this.files.values()].filter((f) => f.id !== this.rootId && this.#match(f, q)).map((f) => this.resource(f));
            return this.#json({ files });
        }
        if (u.pathname === '/drive/v3/files' && method === 'POST') {
            const meta = JSON.parse(init.body);
            const f = this.#mk({ id: this.#id(), name: meta.name, mimeType: meta.mimeType, parents: meta.parents });
            this.files.set(f.id, f);
            this.#touch(f);
            return this.#json({ id: f.id });
        }

        const fm = u.pathname.match(/^\/drive\/v3\/files\/([^/]+)$/);
        if (fm) {
            const id = decodeURIComponent(fm[1]) === 'root' ? this.rootId : decodeURIComponent(fm[1]);
            const f = this.files.get(id);
            if (!f) return this.#error(404, 'File not found');
            if (method === 'GET' && u.searchParams.get('alt') === 'media') {
                const range = init.headers?.Range;
                if (range) {
                    const [, s, e] = range.match(/bytes=(\d+)-(\d+)/);
                    return new Response(f.data.subarray(Number(s), Number(e) + 1), { status: 206 });
                }
                return new Response(f.data, { status: 200 });
            }
            if (method === 'GET') return this.#json(this.resource(f));
            if (method === 'PATCH') {
                const patch = JSON.parse(init.body || '{}');
                if (patch.trashed) f.trashed = true;
                if (patch.name) f.name = patch.name;
                const add = u.searchParams.get('addParents');
                const rm = u.searchParams.get('removeParents');
                if (add) f.parents = [...f.parents.filter((p) => p !== rm), add];
                this.#touch(f);
                return this.#json({ id: f.id });
            }
            if (method === 'DELETE') { this.remove(id); return new Response(null, { status: 204 }); }
        }
        return this.#error(404, `FakeDrive: unhandled ${method} ${u.pathname}`);
    };
}

export const CREDS = { clientId: 'cid', clientSecret: 'csec', refreshToken: 'rtok' };
