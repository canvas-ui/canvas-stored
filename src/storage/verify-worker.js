// Native faults stay in this disposable process, never in the caller.
import { open } from 'lmdb';

const db = open({ path: process.argv[2], readOnly: true, create: false, maxDbs: 256,
    compression: false, encoding: 'binary', keyEncoding: 'binary' });
try {
    const datasets = {};
    // The main database holds the named-database descriptors; discover them
    // rather than miss a store. Finish the root cursor before openDB, which
    // renews the read transaction.
    const names = Array.from(db.getKeys(), key => key.toString('utf8').replace(/\0$/, ''));
    for (const name of names) {
        const store = db.openDB(name, { create: false, encoding: 'binary', keyEncoding: 'binary' });
        if (!store) throw new Error(`Missing dataset: ${name}`);
        let count = 0;
        for (const { key, value } of store.getRange()) { if (key == null || value == null) throw new Error(`Unreadable entry in ${name}`); count++; }
        if (count !== store.getStats().entryCount) throw new Error(`Incomplete dataset: ${name}`);
        datasets[name] = { count };
    }
    process.stdout.write(JSON.stringify({ datasets }));
} finally {
    await db.close();
}
