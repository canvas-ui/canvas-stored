import { test, describe } from 'node:test';
import assert from 'node:assert';
import fs from 'fs-extra';
import os from 'os';
import path from 'path';
import Stored, { Mirror } from '../src/index.js';
import { createFakeHub, sha256, sleep, waitFor } from './helpers/fake-hub.js';

const KEYS = Array.from({ length: 16 }, (_, i) => `Docs/f${i}.txt`);

// A hub that takes `delay` ms to answer, with a count of requests in flight.
function slowFetch(delay) {
    const seen = { PUT: 0, GET: 0, HEAD: 0, DELETE: 0, POST: 0 };
    const flying = { PUT: 0, GET: 0, DELETE: 0, POST: 0 };
    const peak = { PUT: 0, GET: 0, DELETE: 0, POST: 0 };
    const fetchFn = async (url, options = {}) => {
        const method = options.method || 'GET';
        const object = /\/objects\/(Docs\/|rename$)/.test(new URL(url).pathname);
        if (!object) return fetch(url, options);
        seen[method] = (seen[method] || 0) + 1;
        if (method in flying) { flying[method] += 1; peak[method] = Math.max(peak[method], flying[method]); }
        try {
            await sleep(delay);
            return await fetch(url, options);
        } finally {
            if (method in flying) flying[method] -= 1;
        }
    };
    return { fetchFn, seen, peak };
}

async function device(base, name, hub, fetchFn) {
    const dir = path.join(base, name); const folder = path.join(dir, 'folder');
    for (const d of ['folder', 'trash', 'conflicts']) await fs.ensureDir(path.join(dir, d));
    const stored = new Stored({ root: path.join(dir, '.stored'), checksums: ['sha256'] });
    stored.on('error', () => {});
    stored.addBackend('local', { driver: 'file', root: folder, watch: true, stabilityThreshold: 200, followSymlinks: false, tempDir: '.workspace/tmp' });
    stored.addBackend('trash', { driver: 'file', root: path.join(dir, 'trash') });
    stored.addBackend('conflicts', { driver: 'file', root: path.join(dir, 'conflicts') });
    stored.addBackend('remote', { driver: 'canvas', url: hub.url, workspaceId: 'ws1', backend: 'workspace:home', token: 'tok', deviceId: `dev-${name}`, deviceName: name, pollInterval: 500, retryDelays: [5, 10], fetch: fetchFn });
    const mirror = new Mirror(stored, { id: 'm', local: 'local', remote: 'remote', trash: 'trash', conflicts: 'conflicts', deviceId: `dev-${name}`, deviceName: name, debounceMs: 100 });
    mirror.on('error', () => {});
    return { folder, mirror, stop: async () => { await mirror.stop(); await stored.stop(); } };
}

describe('Mirror: transfers to and from the hub run side by side', { timeout: 60_000 }, () => {
    test('uploads overlap and send no HEAD before a new file', async () => {
        const base = path.join(os.tmpdir(), `stored-mirror-lanes-up-${process.pid}`);
        await fs.remove(base);
        const hub = await createFakeHub({ root: path.join(base, 'hub'), token: 'tok' });
        const { fetchFn, seen, peak } = slowFetch(60);
        const a = await device(base, 'a', hub, fetchFn);
        try {
            for (const k of KEYS) await fs.outputFile(path.join(a.folder, k), `data ${k}`);
            await a.mirror.start();
            await waitFor(() => KEYS.every((k) => hub.sha(k) === sha256(`data ${k}`)), { label: 'pushed', timeout: 30_000 });
            await a.mirror.idle();
            assert.ok(peak.PUT > 1, `uploads in flight together: ${peak.PUT}`);
            assert.strictEqual(seen.HEAD, 0, 'a new file is uploaded without a HEAD first');
            assert.strictEqual(seen.PUT, KEYS.length);
        } finally { await a.stop(); await hub.close(); await fs.remove(base); }
    });

    test('deletes and single-file renames overlap', async () => {
        const base = path.join(os.tmpdir(), `stored-mirror-lanes-rm-${process.pid}`);
        await fs.remove(base);
        const hub = await createFakeHub({ root: path.join(base, 'hub'), token: 'tok' });
        const { fetchFn, seen, peak } = slowFetch(60);
        const a = await device(base, 'a', hub, fetchFn);
        try {
            for (const k of KEYS) await fs.outputFile(path.join(a.folder, k), `data ${k}`);
            await a.mirror.start();
            await waitFor(() => KEYS.every((k) => hub.sha(k) === sha256(`data ${k}`)), { label: 'pushed', timeout: 30_000 });
            await a.mirror.idle();
            const gone = KEYS.slice(0, 4); const renamed = KEYS.slice(4, 10);
            for (const k of gone) await fs.remove(path.join(a.folder, k));
            for (const k of renamed) await fs.rename(path.join(a.folder, k), path.join(a.folder, k.replace('/f', '/g')));
            await waitFor(() => gone.every((k) => hub.sha(k) === null)
                && renamed.every((k) => hub.sha(k) === null && hub.sha(k.replace('/f', '/g')) === sha256(`data ${k}`)), { label: 'deleted and renamed', timeout: 30_000 });
            await a.mirror.idle();
            assert.strictEqual(seen.DELETE, gone.length);
            assert.strictEqual(seen.POST, renamed.length);
            assert.strictEqual(seen.PUT, KEYS.length, 'a rename uploads nothing');
            assert.ok(peak.DELETE > 1, `deletes in flight together: ${peak.DELETE}`);
            assert.ok(peak.POST > 1, `renames in flight together: ${peak.POST}`);
        } finally { await a.stop(); await hub.close(); await fs.remove(base); }
    });

    test('downloads overlap', async () => {
        const base = path.join(os.tmpdir(), `stored-mirror-lanes-down-${process.pid}`);
        await fs.remove(base);
        const hub = await createFakeHub({ root: path.join(base, 'hub'), token: 'tok' });
        const { fetchFn, peak } = slowFetch(60);
        for (const k of KEYS) await hub.put(k, Buffer.from(`data ${k}`));
        const b = await device(base, 'b', hub, fetchFn);
        try {
            await b.mirror.start();
            await waitFor(async () => (await Promise.all(KEYS.map((k) => fs.pathExists(path.join(b.folder, k))))).every(Boolean), { label: 'pulled', timeout: 30_000 });
            await b.mirror.idle();
            for (const k of KEYS) assert.strictEqual(await fs.readFile(path.join(b.folder, k), 'utf8'), `data ${k}`);
            assert.ok(peak.GET > 1, `downloads in flight together: ${peak.GET}`);
            assert.deepStrictEqual((await fs.readdir(path.join(b.folder, '.workspace/tmp')).catch(() => [])), [], 'no staged file is left behind');
        } finally { await b.stop(); await hub.close(); await fs.remove(base); }
    });
});
