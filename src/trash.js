import crypto from 'node:crypto';

// Per-deletion provenance, separate from the deduplicated retained blob pool.
// Restoring one path never consumes the bytes needed by another trash item.
export default class BackendTrash {
    tasks = new Map();
    constructor(stored) { this.stored = stored; }
    get db() { return this._db ??= this.stored.index.openDB('backend-trash-v1'); }
    get days() { return this.stored.retention?.days || 0; }

    prepare(backend, key, { retained = null, directory = false, mtime = null } = {}) {
        if (!this.days || (!directory && !retained)) return null;
        const item = {
            id: crypto.randomUUID(), backend, key, type: directory ? 'directory' : 'file',
            sha256: retained?.sha256 ?? null, size: retained?.size ?? null,
            mimeType: retained?.mimeType ?? null, mtime, deletedAt: Date.now(), state: 'pending',
        };
        this.db.putSync(`item:${item.id}`, item);
        return item;
    }

    finish(item) {
        if (item) this.db.putSync(`item:${item.id}`, { ...item, state: 'active' });
    }

    async importLegacy(backendName) {
        const backend = this.stored.getBackend(backendName);
        if (!backend || !this.days) return;
        for (const retained of this.stored.listRetained({ backend: backendName, limit: Infinity })) {
            const keys = retained.legacyKeys ?? (retained.trashVersion ? [] : retained.keys || []);
            for (const address of keys) {
                if (!address.startsWith(`${backendName}:`)) continue;
                const key = address.slice(backendName.length + 1);
                const deletedAt = retained.legacyAt ?? retained.lastAt;
                const id = crypto.createHash('sha256').update(JSON.stringify([backendName, key, retained.sha256, deletedAt])).digest('hex');
                if (this.db.get(`legacy:${id}`)) continue;
                // Old retention mixed overwrites and deletes. Import absent
                // original paths only, never old versions of a live file.
                const current = await backend.stat(key);
                if (!current) this.db.putSync(`item:${id}`, {
                    id, backend: backendName, key, type: 'file', sha256: retained.sha256,
                    size: retained.size, mimeType: retained.mimeType, mtime: null,
                    deletedAt, state: 'active', legacy: true,
                });
                this.db.putSync(`legacy:${id}`, true);
            }
        }
    }

    async get(backendName, id) {
        let item = this.db.get(`item:${id}`);
        if (!item || item.backend !== backendName || ['restored', 'discarded'].includes(item.state) || !this.days
            || Date.now() - item.deletedAt >= this.days * 86400000) return null;
        if (item.state === 'pending') {
            const backend = this.stored.getBackend(backendName);
            const deleted = item.type === 'directory'
                ? await backend.hasTrashedDirectory(item.id)
                : !await backend.stat(item.key);
            if (!deleted) return null;
            this.finish(item);
            item = { ...item, state: 'active' };
        }
        return item;
    }

    async list(backendName, { cursor = null, limit = 200 } = {}) {
        limit = Math.max(1, Math.min(1000, Number(limit) || 200));
        await this.importLegacy(backendName);
        const items = [];
        for (const { key, value } of [...this.db.getRange()]) {
            if (!key.startsWith('item:') || value.backend !== backendName
                || (cursor && value.id.localeCompare(cursor) <= 0)) continue;
            const item = await this.get(backendName, value.id);
            if (item) items.push(item);
        }
        items.sort((a, b) => a.id.localeCompare(b.id));
        const page = items.slice(0, limit);
        return { items: page.map(({ state: _state, backend: _backend, ...item }) => item),
            cursor: items.length > limit ? page.at(-1).id : null, retention: this.stored.retention };
    }

    withItemLock(id, fn) {
        const task = (this.tasks.get(id) || Promise.resolve()).catch(() => {}).then(fn);
        this.tasks.set(id, task);
        void task.finally(() => { if (this.tasks.get(id) === task) this.tasks.delete(id); }).catch(() => {});
        return task;
    }

    restore(backendName, id, options = {}) {
        return this.withItemLock(id, () => this.restoreItem(backendName, id, options));
    }

    async restoreItem(backendName, id, { origin = null } = {}) {
        const item = await this.get(backendName, id);
        if (!item) return { ok: false, reason: 'not-found', id };
        if (item.state === 'discarding') return { ok: false, reason: 'deletion-pending', id };
        await this.stored.getBackend(backendName).validateTrashTarget?.(item.key);
        const result = item.type === 'directory'
            ? await this.stored.getBackend(backendName).restoreTrashedDirectory(id, item.key)
            : await this.stored.restoreRetained(item.sha256, {
                backend: backendName, key: item.key, ifNoneMatch: '*', mtime: item.mtime, origin,
            });
        if (result?.ok) this.db.putSync(`item:${id}`, { ...item, state: 'restored', restoredAt: Date.now() });
        return { ...result, id, key: item.key, type: item.type };
    }

    hasReference(sha256, exceptId, address = null) {
        for (const { key, value } of this.db.getRange()) {
            if (key.startsWith('item:') && value.id !== exceptId && value.sha256 === sha256
                && !['discarded', 'restored'].includes(value.state)
                && (!address || `${value.backend}:${value.key}` === address)) return true;
        }
        return false;
    }

    discard(backendName, id) {
        return this.withItemLock(id, () => this.discardItem(backendName, id));
    }

    async discardItem(backendName, id) {
        const item = this.db.get(`item:${id}`);
        if (!item || item.backend !== backendName || item.state === 'restored') return { id, ok: false, reason: 'not-found' };
        const result = { id, key: item.key, type: item.type, ok: true };
        if (item.state === 'discarded') return { ...result, alreadyDeleted: true };
        const backend = this.stored.getBackend(backendName);
        if (!backend?.canDelete) return { id, ok: false, reason: 'read-only-backend' };
        // Persist intent before removing bytes. Interrupted/failed deletion can
        // be retried, but must not race a subsequent attempt to restore it.
        this.db.putSync(`item:${id}`, { ...item, state: 'discarding' });
        const finish = () => this.db.putSync(`item:${id}`, { ...item, state: 'discarded', discardedAt: Date.now() });
        if (item.type === 'directory') { await backend.dropTrashedDirectory(id); finish(); }
        else await this.stored.discardTrashBytes(item, finish);
        return result;
    }

    async sweep(now) {
        for (const { key, value } of [...this.db.getRange()]) {
            if (!key.startsWith('item:') || now - value.deletedAt < this.days * 86400000) continue;
            if (value.type === 'directory') {
                const backend = this.stored.getBackend(value.backend);
                if (!backend) continue;
                await backend.dropTrashedDirectory(value.id);
            }
            this.db.removeSync(key);
        }
    }
}
