import { open, mkdir, rename, rm, stat } from 'node:fs/promises';
import { createReadStream } from 'node:fs';
import path from 'node:path';
import { pipeline } from 'node:stream/promises';
import cacache from 'cacache';
import contentPath from 'cacache/lib/content/path.js';
import writeContent from 'cacache/lib/content/write.js';

// The caller holds the root's mutation lock until key publication completes.
// This uses the same content writer as cacache.put.stream(), but defers the
// key until EOF so checksum-derived keys do not require a second payload copy.
export default async function ingestStream(root, source, resolveEntry, algorithms) {
    const output = writeContent.stream(root, { algorithms, memoize: false });
    let integrity, size;
    output.on('integrity', value => { integrity = value; });
    output.on('size', value => { size = value; });
    output.write(Buffer.alloc(0));
    await pipeline(source, output);
    const file = contentPath(root, integrity);
    const handle = await open(file, 'r');
    try { await handle.sync(); } finally { await handle.close(); }
    const { key, metadata } = await resolveEntry({ file, size, integrity });
    if (typeof key !== 'string' || !key) throw new Error('Blob key required');
    const entry = await cacache.index.insert(root, key, integrity, { size, metadata });
    if (!entry) throw new Error('Blob index disappeared during publication');
    return { key, size, file };
}

/**
 * Publish a finished file the caller hands over (a resumable upload's part)
 * by moving it into the content store: no second copy. `sha256` (hex) is the
 * file's digest, which the caller vouches for (it hashed the bytes as they
 * arrived). Content already present is deduplicated and the file dropped.
 * Another filesystem, or a root keyed by other algorithms, streams it in.
 */
export async function adoptFile(root, file, { sha256, size }, resolveEntry, algorithms) {
    if (algorithms.length !== 1 || algorithms[0] !== 'sha256') {
        const result = await ingestStream(root, createReadStream(file), resolveEntry, algorithms);
        await rm(file, { force: true });
        return result;
    }
    const integrity = `sha256-${Buffer.from(sha256, 'hex').toString('base64')}`;
    const target = contentPath(root, integrity);
    const present = await stat(target).then(() => true, () => false);
    if (present) {
        await rm(file, { force: true });
    } else {
        await mkdir(path.dirname(target), { recursive: true });
        try {
            const handle = await open(file, 'r');
            try { await handle.sync(); } finally { await handle.close(); }
            await rename(file, target);
        } catch (error) {
            if (error.code !== 'EXDEV') throw error;
            const result = await ingestStream(root, createReadStream(file), resolveEntry, algorithms);
            await rm(file, { force: true });
            return result;
        }
    }
    const { key, metadata } = await resolveEntry({ file: target, size, integrity });
    if (typeof key !== 'string' || !key) throw new Error('Blob key required');
    const entry = await cacache.index.insert(root, key, integrity, { size, metadata });
    if (!entry) throw new Error('Blob index disappeared during publication');
    return { key, size, file: target };
}
