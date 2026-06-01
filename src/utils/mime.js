import { fileTypeFromBuffer, fileTypeFromFile } from 'file-type';
import { open } from 'fs/promises';
import path from 'path';

// Extension-based MIME types for common text/code files (magic bytes don't work for these)
const TEXT_MIME_TYPES = {
    '.txt': 'text/plain',
    '.md': 'text/markdown',
    '.json': 'application/json',
    '.xml': 'application/xml',
    '.html': 'text/html',
    '.htm': 'text/html',
    '.css': 'text/css',
    '.js': 'text/javascript',
    '.mjs': 'text/javascript',
    '.ts': 'text/typescript',
    '.tsx': 'text/typescript',
    '.jsx': 'text/javascript',
    '.yaml': 'text/yaml',
    '.yml': 'text/yaml',
    '.csv': 'text/csv',
    '.log': 'text/plain',
    '.sh': 'text/x-shellscript',
    '.bash': 'text/x-shellscript',
    '.zsh': 'text/x-shellscript',
    '.py': 'text/x-python',
    '.rb': 'text/x-ruby',
    '.go': 'text/x-go',
    '.rs': 'text/x-rust',
    '.c': 'text/x-c',
    '.h': 'text/x-c',
    '.cpp': 'text/x-c++',
    '.hpp': 'text/x-c++',
    '.java': 'text/x-java',
    '.sql': 'text/x-sql',
    '.ini': 'text/plain',
    '.conf': 'text/plain',
    '.cfg': 'text/plain',
    '.env': 'text/plain',
    '.toml': 'text/toml',
};

// Heuristic: is this buffer plausibly UTF-8 text? No NUL bytes and >=95%
// printable / whitespace. Cheap fallback for files without magic bytes or a
// known extension (e.g. .ips, .rules, random log files).
function looksLikeText(buf) {
    if (!buf || buf.length === 0) return false;
    let printable = 0;
    for (let i = 0; i < buf.length; i++) {
        const b = buf[i];
        if (b === 0) return false;
        if (b === 9 || b === 10 || b === 13 || (b >= 32 && b < 127) || b >= 128) printable++;
    }
    return printable / buf.length >= 0.95;
}

async function sniffHead(input, bytes = 4096) {
    if (Buffer.isBuffer(input)) return input.subarray(0, bytes);
    const fh = await open(input, 'r');
    try {
        const out = Buffer.alloc(bytes);
        const { bytesRead } = await fh.read(out, 0, bytes, 0);
        return out.subarray(0, bytesRead);
    } finally { await fh.close(); }
}

// Streaming variant: detect from an already-captured head buffer (first few KB)
// plus the storage key's extension. Used by the streaming put path where the
// full blob is never materialized.
export async function detectMimeFromHead(head, key) {
    try {
        if (head && head.length) {
            const result = await fileTypeFromBuffer(head);
            if (result?.mime) return result.mime;
        }
        if (typeof key === 'string') {
            const ext = path.extname(key).toLowerCase();
            if (TEXT_MIME_TYPES[ext]) return TEXT_MIME_TYPES[ext];
        }
        if (head && looksLikeText(head)) return 'text/plain';
        return 'application/octet-stream';
    } catch {
        return 'application/octet-stream';
    }
}

export async function detectMimeType(input) {
    try {
        const result = Buffer.isBuffer(input)
            ? await fileTypeFromBuffer(input)
            : await fileTypeFromFile(input);
        if (result?.mime) return result.mime;

        if (typeof input === 'string') {
            const ext = path.extname(input).toLowerCase();
            if (TEXT_MIME_TYPES[ext]) return TEXT_MIME_TYPES[ext];
        }

        const head = await sniffHead(input).catch(() => null);
        if (head && looksLikeText(head)) return 'text/plain';

        return 'application/octet-stream';
    } catch {
        return 'application/octet-stream';
    }
}

