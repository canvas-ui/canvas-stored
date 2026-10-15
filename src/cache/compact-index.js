import cacache from 'cacache';

// Explicit maintenance under the cache root's mutation lock. Retain the newest
// record for each live key, including its metadata. Cacache's default handling
// of deletion markers prevents resurrecting older entries.
export default async function compactIndex(root) {
    let compactedCount = 0;
    for await (const entry of cacache.ls.stream(root)) {
        await cacache.index.compact(root, entry.key, (a, b) => a.key === b.key);
        compactedCount++;
    }
    return { compactedCount };
}
