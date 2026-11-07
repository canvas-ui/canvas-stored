import fs from 'node:fs/promises';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { open } from 'lmdb';

const VERIFY_WORKER = fileURLToPath(new URL('./verify-worker.js', import.meta.url));
const VERIFY_TIMEOUT_MS = 30 * 60 * 1000;

/** The LMDB environment behind the index and ledgers (compressed, named sub-dbs). */
export function openKeyValueStore({ path: dbPath, name, compression = true, maxDbs = 64 }) {
    if (!dbPath) throw new Error('LMDB database path not provided');
    return open({ path: dbPath, name, compression, maxDbs, cache: false, strictAsyncOrder: false });
}

/**
 * Check an existing environment in a child process before opening it in this
 * one: a corrupt map can SIGBUS, which try/catch cannot contain. Walks every
 * named database read-only. Does not repair, truncate, replace or create
 * data.mdb. Returns null when there is no database yet.
 */
export async function verifyDatabase(directory) {
    const databasePath = path.resolve(directory);
    const file = path.join(databasePath, 'data.mdb');
    try {
        let stat;
        try { stat = await fs.stat(file); }
        catch (error) { if (error.code === 'ENOENT') return null; throw error; }
        if (!stat.isFile()) throw new Error('Expected data.mdb to be a regular file');
        if (stat.size === 0) throw Object.assign(new Error('Empty data.mdb'), { canQuarantine: true });
        return await runVerifyWorker(databasePath);
    } catch (cause) {
        throw Object.assign(new Error(`Database verification failed for ${file}: ${cause.message}. No repair or replacement was attempted.`, { cause }),
            { code: 'DATABASE_INTEGRITY_FAILED', databasePath: file, canQuarantine: cause.canQuarantine === true, statusCode: 500 });
    }
}

function runVerifyWorker(databasePath) {
    return new Promise((resolve, reject) => {
        const child = spawn(process.execPath, [VERIFY_WORKER, databasePath], {
            stdio: ['ignore', 'pipe', 'pipe'], env: { ...process.env, NODE_OPTIONS: '' },
        });
        let output = '';
        let errors = '';
        child.stdout.on('data', data => { output += data; });
        child.stderr.on('data', data => { errors = (errors + data).slice(-4096); });
        let timedOut = false;
        const timeout = setTimeout(() => { timedOut = true; child.kill('SIGKILL'); }, VERIFY_TIMEOUT_MS);
        child.on('error', error => { clearTimeout(timeout); reject(error); });
        child.on('close', (code, signal) => {
            clearTimeout(timeout);
            if (timedOut) { reject(new Error('Database verification timed out')); return; }
            if (code !== 0) {
                const canQuarantine = ['SIGABRT', 'SIGBUS', 'SIGSEGV'].includes(signal)
                    || /MDB_(CORRUPTED|PAGE_NOTFOUND|INVALID)\b|Unreadable entry in |Incomplete dataset: /.test(errors);
                reject(Object.assign(new Error(`verify worker ${signal || `exit ${code}`}: ${errors.trim()}`), { canQuarantine }));
                return;
            }
            try { resolve(JSON.parse(output)); } catch { reject(new Error('Invalid verify worker response')); }
        });
    });
}
