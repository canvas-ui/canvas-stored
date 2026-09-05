import picomatch from 'picomatch';

/**
 * Key hygiene shared by the mirror engine and the canvas driver. Keys are
 * relative, '/'-separated paths in NFC — the spelling the hub uses on the
 * wire (`docs/sync-protocol.md`). Everything a device filesystem can produce
 * that the hub (or the other platform) would refuse is caught here and
 * surfaced as a `skip {reason}` instead of a silent loss.
 */

// Mirror-side defaults on top of the hub's effective exclusions.
export const MIRROR_IGNORE_DEFAULTS = Object.freeze([
    '.workspace/**',
    '.stored-tmp/**',
    '**/.DS_Store',
    '**/Thumbs.db',
    '**/desktop.ini',
    '**/~$*',
    '**/*.tmp',
    '**/*.part',
    '**/*.crdownload',
    '**/.~lock.*#',
    '**/*.swp',
]);

const MAX_KEY_BYTES = 4096;
const MAX_SEGMENT_BYTES = 255;
const WINDOWS_RESERVED = /^(con|prn|aux|nul|com[1-9]|lpt[1-9])(\..*)?$/i;
// eslint-disable-next-line no-control-regex
const WINDOWS_ILLEGAL = /[<>:"|?*\x00-\x1f]/;

/** NFC + '/' separators, no empty / leading / trailing segments. */
export function normalizeKey(key) {
    return String(key ?? '')
        .normalize('NFC')
        .replace(/\\/g, '/')
        .split('/')
        .filter((s) => s.length > 0)
        .join('/');
}

/**
 * Why a (normalized) key cannot be synced, or null when it can. Reasons:
 * `empty`, `nul`, `traversal`, `absolute`, `too-long`, `segment-too-long`,
 * `reserved-name`, `illegal-chars`, `trailing-dot-or-space`.
 */
export function validateKey(key) {
    if (typeof key !== 'string' || key.length === 0) return 'empty';
    if (key.includes('\0')) return 'nul';
    if (key.startsWith('/') || /^[a-zA-Z]:[\\/]/.test(key)) return 'absolute';
    if (Buffer.byteLength(key, 'utf8') > MAX_KEY_BYTES) return 'too-long';
    for (const seg of key.split('/')) {
        if (seg === '' || seg === '.' || seg === '..') return 'traversal';
        if (Buffer.byteLength(seg, 'utf8') > MAX_SEGMENT_BYTES) return 'segment-too-long';
        if (WINDOWS_RESERVED.test(seg)) return 'reserved-name';
        if (WINDOWS_ILLEGAL.test(seg)) return 'illegal-chars';
        if (/[. ]$/.test(seg)) return 'trailing-dot-or-space';
    }
    return null;
}

const matcherCache = new WeakMap();
function matcherFor(patterns) {
    const list = (Array.isArray(patterns) ? patterns : [patterns]).filter((p) => typeof p === 'string' && p.trim());
    if (!list.length) return null;
    if (Array.isArray(patterns)) {
        const cached = matcherCache.get(patterns);
        if (cached) return cached;
    }
    // "dir/**" also prunes "dir" itself (same convention as the file driver).
    const globs = list.flatMap((p) => (p.endsWith('/**') ? [p, p.slice(0, -3)] : [p]));
    const match = picomatch(globs, { dot: true });
    if (Array.isArray(patterns)) matcherCache.set(patterns, match);
    return match;
}

/** Does `key` match any of the ignore globs (picomatch, dotfiles included)? */
export function isIgnored(key, patterns = MIRROR_IGNORE_DEFAULTS) {
    const k = normalizeKey(key);
    if (!k) return false;
    const match = matcherFor(patterns);
    if (!match) return false;
    if (match(k)) return true;
    // A pattern naming a directory covers everything under it.
    const parts = k.split('/');
    for (let i = 1; i < parts.length; i += 1) {
        if (match(parts.slice(0, i).join('/'))) return true;
    }
    return false;
}

/**
 * Selective sync: `prefixes` is a list of key prefixes (`Docs`, `Photos/2024/`)
 * or picomatch globs. Empty/absent = everything. A prefix covers itself and
 * the subtree under it.
 */
export function matchesPrefixes(key, prefixes) {
    const list = (Array.isArray(prefixes) ? prefixes : [prefixes]).filter((p) => typeof p === 'string' && p.trim());
    if (!list.length) return true;
    const k = normalizeKey(key);
    for (const raw of list) {
        const p = normalizeKey(raw);
        if (!p) return true;
        if (/[*?[\]{}!]/.test(p)) {
            // A glob covers the keys it matches and everything under a
            // directory it matches (`Photos/20*` → `Photos/2024/a.jpg`).
            const match = picomatch(p, { dot: true });
            if (match(k)) return true;
            const parts = k.split('/');
            for (let i = 1; i < parts.length; i += 1) if (match(parts.slice(0, i).join('/'))) return true;
            continue;
        }
        if (k === p || k.startsWith(`${p}/`)) return true;
    }
    return false;
}

const pad = (n) => String(n).padStart(2, '0');

/**
 * Dropbox-style conflict copy name:
 * `<stem> (conflict from <device> <YYYY-MM-DD HHmm>).<ext>` (UTC), in the
 * same directory as `key`. Matches canvas-fuse's `conflict_copy_key`.
 */
export function conflictKey(key, deviceName, date = new Date()) {
    const k = normalizeKey(key);
    const slash = k.lastIndexOf('/');
    const dir = slash >= 0 ? k.slice(0, slash) : null;
    const name = slash >= 0 ? k.slice(slash + 1) : k;
    const dot = name.lastIndexOf('.');
    const stem = dot > 0 ? name.slice(0, dot) : name;
    const ext = dot > 0 ? name.slice(dot + 1) : null;
    const at = date instanceof Date ? date : new Date(date);
    const stamp = `${at.getUTCFullYear()}-${pad(at.getUTCMonth() + 1)}-${pad(at.getUTCDate())} ${pad(at.getUTCHours())}${pad(at.getUTCMinutes())}`;
    const device = String(deviceName || 'device').replace(/[/\\]/g, '_');
    const leaf = ext ? `${stem} (conflict from ${device} ${stamp}).${ext}` : `${stem} (conflict from ${device} ${stamp})`;
    return dir ? `${dir}/${leaf}` : leaf;
}

export default { normalizeKey, validateKey, isIgnored, matchesPrefixes, conflictKey, MIRROR_IGNORE_DEFAULTS };
