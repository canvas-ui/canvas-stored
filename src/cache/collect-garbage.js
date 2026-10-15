import { opendir, stat, unlink } from 'node:fs/promises';
import path from 'node:path';
import cacache from 'cacache';
import contentPath from 'cacache/lib/content/path.js';

// Explicit maintenance only. The caller must exclude writes for this root.
// Unlike cacache.verify(), this never reads/hashes payloads, rebuilds the key
// index, or removes tmp files. Integrity scrubbing is a separate operation.
// This still walks the index and holds the live digest paths in memory; do not
// put it on a timer or a shutdown path for a large store.
export default async function collectGarbage(root) {
    const live = new Set();
    for await (const entry of cacache.ls.stream(root)) {
        // Buffer writes may publish more than one hash algorithm. Preserve
        // every digest named by the entry, not just the strongest one.
        for (const integrity of String(entry.integrity).trim().split(/\s+/)) {
            live.add(contentPath(root, integrity));
        }
    }
    const result = { reclaimedCount: 0, reclaimedSize: 0 };
    for await (const file of files(contentPath.contentDir(root))) {
        if (live.has(file)) continue;
        try {
            const info = await stat(file);
            await unlink(file);
            result.reclaimedCount++;
            result.reclaimedSize += info.size;
        } catch (error) {
            if (error.code !== 'ENOENT') throw error;
        }
    }
    return result;
}

async function* files(directory) {
    let entries;
    try { entries = await opendir(directory); }
    catch (error) {
        if (error.code === 'ENOENT') return;
        throw error;
    }
    for await (const entry of entries) {
        const file = path.join(directory, entry.name);
        if (entry.isDirectory()) yield* files(file);
        else if (entry.isFile()) yield file;
    }
}
