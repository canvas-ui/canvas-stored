import { open } from 'node:fs/promises';
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
