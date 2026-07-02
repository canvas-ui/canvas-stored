'use strict';

import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';

import { extract } from '../src/extractors/index.js';
import Stored from '../src/index.js';

// A real 10x5 PNG (valid header → image-size reads dimensions; no EXIF).
const PNG_10x5 = Buffer.from(
    'iVBORw0KGgoAAAANSUhEUgAAAAoAAAAFCAYAAABPWa8CAAAAFElEQVR42mNkYPhfz0AEYBxVSF+FAP5FDvcfRYWgAAAAAElFTkSuQmCC',
    'base64',
);

test('extract: PNG → dimensions', async () => {
    const m = await extract({ data: PNG_10x5 }, { mimeType: 'image/png' });
    assert.equal(m.dimensions?.width, 10);
    assert.equal(m.dimensions?.height, 5);
});

test('extract: text/* → {} (not an extractable modality)', async () => {
    assert.deepEqual(await extract({ data: Buffer.from('hello world') }, { mimeType: 'text/plain' }), {});
});

test('extract: corrupt image → {} (graceful, never throws)', async () => {
    assert.deepEqual(await extract({ data: Buffer.from('definitely not an image') }, { mimeType: 'image/png' }), {});
});

test('extract: empty source → {}', async () => {
    assert.deepEqual(await extract({}, { mimeType: 'image/png' }), {});
});

test('Stored.put runs the injected extractor → custom carries dimensions', async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'stored-extract-'));
    try {
        const s = new Stored({ rootPath: root, extract });
        s.addBackend('data', { driver: 'file', root: path.join(root, 'data'), watch: false });

        const r = await s.put(PNG_10x5, { backends: ['data'], mimeType: 'image/png' });
        assert.equal(r.ok, true);
        assert.equal(r.custom?.dimensions?.width, 10);
        assert.equal(r.custom?.dimensions?.height, 5);

        // Non-extractable content leaves custom empty (no garbage).
        const r2 = await s.put(Buffer.from('plain text'), { backends: ['data'], mimeType: 'text/plain' });
        assert.deepEqual(r2.custom, {});
    } finally {
        await fs.rm(root, { recursive: true, force: true });
    }
});

test('Stored.put without an extractor → no custom extraction', async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'stored-noextract-'));
    try {
        const s = new Stored({ rootPath: root }); // no extract injected
        s.addBackend('data', { driver: 'file', root: path.join(root, 'data'), watch: false });
        const r = await s.put(PNG_10x5, { backends: ['data'], mimeType: 'image/png' });
        assert.equal(r.ok, true);
        assert.deepEqual(r.custom, {});
    } finally {
        await fs.rm(root, { recursive: true, force: true });
    }
});
