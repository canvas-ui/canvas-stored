import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import Stored from '../src/index.js';

for (const [label, snapshot] of [
    ['incomplete walk', { files: [], complete: false }],
    ['failed driver', { ok: false, reason: 'offline' }],
    ['failed driver with empty files', { ok: false, files: [], reason: 'offline' }],
    ['missing snapshot', undefined],
]) {
    test(`${label} never removes existing locations`, async t => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), 'canvas-scan-safety-'));
        const home = path.join(root, 'home');
        await fs.mkdir(home);
        const stored = new Stored({ root: path.join(root, 'index') });
        stored.addBackend('home', { driver: 'file', root: home, watch: false });
        t.after(async () => { await stored.stop(); await fs.rm(root, { recursive: true, force: true }); });
        await stored.writeObject('home', 'Architecture/Kitchen/plan.txt', Buffer.from('keep'));
        const before = structuredClone(stored.index.get('home:Architecture/Kitchen/plan.txt'));
        const mock = t.mock.method(stored.getBackend('home'), 'scan', async () => snapshot);
        const result = await stored.scan('home');
        assert.equal(result.complete, false);
        assert.deepEqual(stored.index.get('home:Architecture/Kitchen/plan.txt'), before);
        mock.mock.restore();
        assert.equal((await stored.scan('home')).complete, true);
        assert.equal(stored.index.get('home:Architecture/Kitchen/plan.txt').id, before.id);
    });
}
