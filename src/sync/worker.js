import { parentPort } from 'worker_threads';
import cacache from 'cacache';
import { createWriteStream } from 'fs';
import { mkdir } from 'fs/promises';
import { pipeline } from 'stream/promises';
import { dirname, join } from 'path';

// memoize:false — never hold blobs (up to 20GB) in the worker's RAM; stream
// straight from the cache content store to each target.
const CACHE_OPTS = { memoize: false };

parentPort.on('message', async ({ id, cacheRoot, cacheKey, targets }) => {
    const results = [];

    // Confirm the entry exists before fanning out (cheap index lookup, no read).
    const info = await cacache.get.info(cacheRoot, cacheKey).catch(() => null);
    if (!info) {
        for (const target of targets) {
            results.push({ backend: target.name, success: false, error: 'Cache read failed: entry not found' });
        }
        parentPort.postMessage({ id, results });
        return;
    }

    for (const target of targets) {
        try {
            switch (target.driver) {
                case 'file': {
                    const filePath = join(target.root, target.key);
                    await mkdir(dirname(filePath), { recursive: true });
                    // Fresh read stream per target — streams are single-use and
                    // we never materialize the blob in memory.
                    await pipeline(
                        cacache.get.stream(cacheRoot, cacheKey, CACHE_OPTS),
                        createWriteStream(filePath),
                    );
                    results.push({ backend: target.name, key: target.key, success: true });
                    break;
                }
                // Future: 's3', 'smb', etc.
                default:
                    results.push({ backend: target.name, success: false, error: `Unknown driver: ${target.driver}` });
            }
        } catch (err) {
            results.push({ backend: target.name, success: false, error: err.message });
        }
    }

    parentPort.postMessage({ id, results });
});
