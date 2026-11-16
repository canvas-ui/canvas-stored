import http from 'node:http';
import { createHash } from 'node:crypto';

/** A minimal S3 endpoint over real HTTP (single PUT, HEAD, GET, DELETE) for
 * tests that need the AWS SDK and the upload thread rather than a fake client.
 * `checksums` makes it check and report `x-amz-checksum-sha256`. */
export async function startS3Http({ checksums = false } = {}) {
    const objects = new Map(), requests = [];
    const server = http.createServer(async (req, res) => {
        const chunks = []; for await (const chunk of req) chunks.push(chunk);
        const body = Buffer.concat(chunks), key = decodeURIComponent(req.url.split('?')[0]);
        requests.push({ method: req.method, key, headers: req.headers });
        const error = (code, name) => { res.writeHead(code, { 'content-type': 'application/xml' }); res.end(req.method === 'HEAD' ? undefined : `<Error><Code>${name}</Code><Message>${name}</Message></Error>`); };
        if (req.method === 'PUT') {
            const sha = req.headers['x-amz-checksum-sha256'], md5 = req.headers['content-md5'];
            if (checksums && sha && createHash('sha256').update(body).digest('base64') !== sha) return error(400, 'BadDigest');
            if (md5 && createHash('md5').update(body).digest('base64') !== md5) return error(400, 'BadDigest');
            objects.set(key, { body, sha: checksums ? sha : undefined, meta: req.headers['x-amz-meta-sha256'] });
            res.writeHead(200, { etag: '"etag"' }); return res.end();
        }
        const row = objects.get(key);
        if (req.method === 'DELETE') { objects.delete(key); res.writeHead(204); return res.end(); }
        if (!row) return error(404, 'NoSuchKey');
        res.writeHead(200, { 'content-length': row.body.length, etag: '"etag"', 'last-modified': new Date().toUTCString(),
            ...(row.meta ? { 'x-amz-meta-sha256': row.meta } : {}),
            ...(row.sha && req.headers['x-amz-checksum-mode'] ? { 'x-amz-checksum-sha256': row.sha } : {}) });
        res.end(req.method === 'HEAD' ? undefined : row.body);
    });
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    return { objects, requests, endpoint: `http://127.0.0.1:${server.address().port}`,
        stop: () => new Promise(resolve => { server.closeAllConnections?.(); server.close(resolve); }) };
}
