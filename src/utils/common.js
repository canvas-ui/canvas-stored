import fs from 'fs';
import path from 'path';

const DEFAULT_ROOT = './.stored';

/** Resolve layout: `<root>/{index,cache,data}` with per-subdir overrides. */
export function resolveStoredPaths(config = {}) {
    const root = path.resolve(config.root ?? DEFAULT_ROOT);
    return {
        root,
        index: path.resolve(config.index?.path ?? path.join(root, 'index')),
        cache: path.resolve(config.cache?.path ?? path.join(root, 'cache')),
        data: path.resolve(config.data?.path ?? path.join(root, 'data')),
    };
}

export function isJson(input) {
    if (typeof input !== 'object' || input === null) return false;
    try {
        JSON.stringify(input);
        return true;
    } catch { return false; }
}

export function isFile(input) {
    if (typeof input !== 'string') return false;
    try { return fs.statSync(input).isFile(); }
    catch { return false; }
}

export function isBuffer(input) {
    return Buffer.isBuffer(input);
}

export function isStream(input) {
    return input && typeof input.pipe === 'function';
}
