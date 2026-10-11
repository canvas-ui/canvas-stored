import path from 'node:path';
import Stored from '../../src/index.js';

const [root, phase, json] = process.argv.slice(2);
const stored = new Stored({ root: path.join(root, 'state'), retention: { days: 30 }, replication: { autoStart: false } });
stored.on('error', error => { throw error; });
stored.addBackend('data', { driver: 'cacache', root: path.join(root, 'data') });
for (const backend of ['a', 'b']) stored.addBackend(backend, { driver: 'file', root: path.join(root, backend), watch: false });
const target = phase === 'place' ? stored.getBackend('a') : stored;
const method = { place: 'createFrom', release: 'deleteByUrl', retire: 'removeObject' }[phase];
const original = target[method].bind(target);
target[method] = async (...args) => {
    await original(...args);
    process.exit(42); // deliberately skip stop(), receipts and cleanup
};
stored.replicas.submit(JSON.parse(json));
await stored.replicas.drain();
process.exit(43); // the intended crash boundary was not reached
