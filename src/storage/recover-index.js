import fs from 'node:fs/promises';
import path from 'node:path';
import { verifyDatabase } from './lmdb.js';

/** Prepare the Stored index before opening LMDB. Only an unreadable environment
 * is quarantined; infrastructure failures must not discard a usable index.
 * The entire environment is retained, including operational sub-databases.
 */
export async function prepareIndex(directory, { verify = verifyDatabase } = {}) {
    const databasePath = path.resolve(directory);
    try {
        await verify(databasePath);
        return null;
    } catch (error) {
        if (error.code !== 'DATABASE_INTEGRITY_FAILED' || error.canQuarantine !== true) throw error;
        // Renaming a symlink would leave its actual environment in place.
        if (!(await fs.lstat(databasePath)).isDirectory()) throw error;
        const createdAt = new Date().toISOString();
        const quarantinePath = await fs.mkdtemp(`${databasePath}.quarantine-${createdAt.replaceAll(':', '-')}-`);
        const recovery = {
            databasePath,
            quarantinePath,
            preservedIndexPath: path.join(quarantinePath, 'index'),
            createdAt,
            reason: error.cause?.message || error.message,
        };
        // Write the report before moving anything. A rename failure propagates:
        // never open a fresh index unless the original was preserved successfully.
        await fs.writeFile(path.join(quarantinePath, 'recovery.json'), JSON.stringify(recovery, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
        await fs.rename(databasePath, recovery.preservedIndexPath);
        return recovery;
    }
}
