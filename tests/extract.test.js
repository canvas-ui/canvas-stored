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

test('extract: text/* → a searchable head of the bytes', async () => {
    const r = await extract({ data: Buffer.from('hello world') }, { mimeType: 'text/plain' });
    assert.equal(r.text.content, 'hello world');
    assert.equal(r.text.truncated, false);
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

        // Text blobs carry a searchable head, so the bytes are findable by what
        // they SAY — a File is otherwise indexed by its name alone.
        const r2 = await s.put(Buffer.from('quarterly figures are up'), { backends: ['data'], mimeType: 'text/plain' });
        assert.equal(r2.custom?.text?.content, 'quarterly figures are up');
        assert.equal(r2.custom?.text?.truncated, false);

        // Non-extractable content leaves custom empty (no garbage).
        const r3 = await s.put(Buffer.from('%PDF-1.4 whatever'), { backends: ['data'], mimeType: 'application/pdf' });
        assert.deepEqual(r3.custom, {});
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

test('text extraction: bounded, binary-safe, and honest about truncation', async () => {
    const { extract } = await import('../src/extractors/index.js');

    // A long file is indexed by its head, and says so.
    const long = await extract({ data: Buffer.alloc(20000, 0x61) }, { mimeType: 'text/plain' });
    assert.equal(long.text.content.length, 8192);
    assert.equal(long.text.truncated, true);

    // text/* that is really bytes (mislabelled binary, UTF-16) indexes nothing
    // rather than noise.
    const binary = await extract({ data: Buffer.from([0x89, 0x50, 0x00, 0x47]) }, { mimeType: 'text/plain' });
    assert.deepEqual(binary, {});

    // Whitespace-only is nothing to search for.
    const blank = await extract({ data: Buffer.from('   \n\t ') }, { mimeType: 'text/plain' });
    assert.deepEqual(blank, {});

    // A temp file is read a block at a time, not slurped.
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'stored-text-'));
    try {
        const file = path.join(dir, 'big.log');
        await fs.writeFile(file, Buffer.alloc(50000, 0x62));
        const fromFile = await extract({ file }, { mimeType: 'text/plain' });
        assert.equal(fromFile.text.content.length, 8192);
        assert.equal(fromFile.text.truncated, true);
    } finally {
        await fs.rm(dir, { recursive: true, force: true });
    }
});
