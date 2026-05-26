import Debug from 'debug';
import StorageBackend from '../StorageBackend.js';

const debug = Debug('stored:backend:s3');

/**
 * S3 storage backend — SKELETON.
 *
 * Registered so `stored://s3:<account>/<key>` URLs parse and dispatch, but the
 * CRUD methods are not yet implemented (they inherit `Not implemented` throws
 * from StorageBackend). Wire up an S3 client (`@aws-sdk/client-s3`) in a
 * follow-up: `config` carries { bucket, region, account, prefix?, credentials? }.
 */
export default class S3Backend extends StorageBackend {
    constructor(name, config = {}) {
        super(name, config);
        this.type = 'remote';
        debug(`S3Backend "${name}" registered (skeleton; bucket=${config.bucket ?? '?'})`);
    }
}
