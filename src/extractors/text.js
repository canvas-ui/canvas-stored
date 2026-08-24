'use strict';

import fs from 'fs';

/**
 * How much of a text blob becomes searchable text.
 *
 * This is a search excerpt, not the file: it rides in the document's metadata,
 * which every listing and every mount carries, so it has to stay small enough
 * that a folder of source files is still cheap to list. 8 KB covers a note, a
 * config, or a source file whole; a long log is indexed by its head and says so
 * (`truncated`), which is honest about what a search can and cannot find.
 */
const MAX_TEXT_BYTES = 8192;

// Text that is really bytes: a NUL in the first block means someone's mime says
// text/* and means it (a mislabelled binary, a UTF-16 file). Indexing that
// yields noise, so it is skipped rather than guessed at.
function looksBinary(buffer) {
    return buffer.includes(0);
}

/**
 * The head of a text blob, as searchable text.
 *
 * Reads at most MAX_TEXT_BYTES — a temp file is opened and partially read
 * rather than slurped, so extracting from a 2 GB log costs one block.
 */
export async function extractText(source, mimeType) {
    let head;
    if (source.data) {
        head = source.data.subarray(0, MAX_TEXT_BYTES);
    } else {
        const handle = await fs.promises.open(source.file, 'r');
        try {
            const buffer = Buffer.allocUnsafe(MAX_TEXT_BYTES);
            const { bytesRead } = await handle.read(buffer, 0, MAX_TEXT_BYTES, 0);
            head = buffer.subarray(0, bytesRead);
        } finally {
            await handle.close();
        }
    }

    if (head.length === 0 || looksBinary(head)) { return {}; }

    // `fatal: false` + stream:true so a multi-byte character split by the read
    // boundary is dropped rather than becoming a replacement char in the index.
    const decoded = new TextDecoder('utf-8', { fatal: false }).decode(head, { stream: true });
    const content = decoded.replace(/�/g, '').trim();
    if (!content) { return {}; }

    const size = source.data
        ? source.data.length
        : await fs.promises.stat(source.file).then((s) => s.size).catch(() => head.length);

    return {
        text: {
            content,
            truncated: size > head.length,
            mimeType: mimeType || 'text/plain',
        },
    };
}

export default extractText;
