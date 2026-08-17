import fs from 'fs';
import path from 'path';
import Debug from 'debug';

const debug = Debug('stored:mount');

/**
 * Filesystem types served over the network. A backend rooted on one of these is
 * `remote: true` — the bytes are reachable through a POSIX path, but every
 * assumption that makes local disk cheap (inotify, free stat, fast full-file
 * hashing) is off.
 *
 * `fuse.*` is matched by prefix: sshfs, rclone, s3fs and friends are all remote
 * regardless of the transport they wrap.
 */
const REMOTE_FSTYPES = new Set([
    'cifs', 'smbfs', 'smb3',
    'nfs', 'nfs4',
    'afpfs', 'afp',
    'ncpfs', 'coda',
    '9p', 'ceph', 'glusterfs', 'lustre', 'beegfs', 'ocfs2', 'gfs2',
    'davfs', 'davfs2', 'webdav',
]);

const REMOTE_FSTYPE_PREFIXES = ['fuse.'];

/** `\040`-style octal escapes are how /proc/mounts encodes spaces and tabs. */
function unescapeMountField(value) {
    return String(value).replace(/\\(\d{3})/g, (_, oct) => String.fromCharCode(parseInt(oct, 8)));
}

export function isRemoteFstype(fstype) {
    if (!fstype) return false;
    const type = String(fstype).toLowerCase();
    if (REMOTE_FSTYPES.has(type)) return true;
    return REMOTE_FSTYPE_PREFIXES.some((prefix) => type.startsWith(prefix));
}

/**
 * Read the kernel mount table. Linux only — every other platform returns [] and
 * detection degrades to "assume local unless configured otherwise".
 * @returns {Array<{ source: string, mountpoint: string, fstype: string }>}
 */
function readMountTable() {
    if (process.platform !== 'linux') return [];
    let raw;
    try {
        raw = fs.readFileSync('/proc/mounts', 'utf8');
    } catch (err) {
        debug(`Cannot read /proc/mounts: ${err.message}`);
        return [];
    }
    const rows = [];
    for (const line of raw.split('\n')) {
        if (!line) continue;
        const [source, mountpoint, fstype] = line.split(' ');
        if (!mountpoint || !fstype) continue;
        rows.push({
            source: unescapeMountField(source),
            mountpoint: unescapeMountField(mountpoint),
            fstype: fstype.toLowerCase(),
        });
    }
    return rows;
}

/**
 * Which mount serves `absPath`? The longest mountpoint that is a path-prefix of
 * it wins (a bind/overlay nested inside another mount must beat its parent).
 *
 * Deliberately synchronous and dependency-free: this runs in the FileBackend
 * constructor, where a subprocess (`findmnt`) or an async hop would force every
 * caller of `addBackend()` to become async. Reading /proc/mounts is a single
 * cheap read of a kernel-generated file.
 *
 * @param {string} absPath Absolute, ideally already-realpath'd directory.
 * @returns {{ remote: boolean, transport: string|null, source: string|null, mountpoint: string|null }}
 */
export function detectMountSync(absPath) {
    const unknown = { remote: false, transport: null, source: null, mountpoint: null };
    if (!absPath || typeof absPath !== 'string') return unknown;

    const target = path.resolve(absPath);
    const table = readMountTable();
    if (!table.length) return unknown;

    let best = null;
    for (const row of table) {
        const mp = row.mountpoint;
        const covers = mp === '/' ? true : (target === mp || target.startsWith(mp + path.sep));
        if (!covers) continue;
        if (!best || mp.length > best.mountpoint.length) best = row;
    }
    if (!best) return unknown;

    const remote = isRemoteFstype(best.fstype);
    debug(`${target} → ${best.fstype} on ${best.mountpoint} (${remote ? 'remote' : 'local'})`);
    return { remote, transport: best.fstype, source: best.source, mountpoint: best.mountpoint };
}
