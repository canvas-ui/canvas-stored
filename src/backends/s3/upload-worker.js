import { parentPort, workerData } from 'node:worker_threads';
import { S3Client } from '@aws-sdk/client-s3';
import { uploadFile } from './upload.js';

// The upload thread of one S3 backend: hashing, request signing, TLS and the
// file stream run here, so a transfer never occupies the thread that serves
// the workspace. Jobs run side by side; each answers with its own id.
const client = new S3Client(workerData.client);

parentPort.on('message', async ({ id, job }) => {
    try { parentPort.postMessage({ id, result: await uploadFile(client, job) }); }
    catch (error) {
        parentPort.postMessage({ id, error: { name: error.name, message: error.message, code: error.code,
            retryable: error.retryable === true, $metadata: { httpStatusCode: error.$metadata?.httpStatusCode } } });
    }
});
