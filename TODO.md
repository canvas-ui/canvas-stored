# StoreD

We'll design a simple CRUD data storage/retrieval middleware that aims to abstract different BLOB storage backends under a simple API. We should support retrieval based on checksums and paths.

! This module only abstracts blob backends - mailboxes/git/etc live in separate connectors

## TODO

- **Batch operations with a configurable batch size.** Scan/index/resync currently
  processes files one at a time (the consumer-side resync loop does a sequential
  per-file upsert, which is O(n) DB round-trips and slow for large or remote
  backends). `scan()` should yield/return work in batches and downstream
  indexing should consume them in batches (e.g. via synapsd `putMany` /
  `putManyDirectoryPaths` — one transaction + one bitmap flush per batch).
  Batch size must be configurable (per backend and/or global, e.g.
  `config.batchSize`, default ~500) to trade off memory vs throughput, and the
  same batching should apply to delete/sync queue draining where backends
  support bulk operations.

## API

### Core (MVP)

- **put(blob, { key?, backends?, metadata? })** → `PutResult`  
  Store a blob. Auto-detects Buffer/Stream/path.  
  Returns the canonical metadata record including sync status for each backend.  
  key is optional; if omitted, StoreD generates one based on checksum.

- **get(idOrChecksum, { stream?, backend? })** → `Buffer | Stream`  
  Retrieve by checksum (`sha256:...`) or key/path.

- **delete(idOrChecksum, { backends? })** → `{ deleted: string[] }`

- **list({ backend?, prefix?, limit? })** → `AsyncIterable<{ key, checksum, size }>`

- **stat(idOrChecksum)** → `Metadata | null`

- **has(idOrChecksum)** → `boolean`  
  Thin wrapper around stat.

### Backend Management

- **addBackend(name, config)** - Register a backend at runtime
- **removeBackend(name)** - Unregister a backend  
- **listBackends()** → `string[]`
- **getBackend(name)** → `BackendInfo`

Every backend must implement:
- put(key, buffer/stream) → { key }
- get(key, { stream? }) → Buffer|Stream
- delete(key) → void
- list({ prefix?, limit? }) → AsyncIterator<Entry>
- stat(key) → { size, modified } | null
- watch?(emitCb) → void     // optional for remote backends

### Cache Management

- **cache.stats()** – size, entries, hits/misses
- **cache.evict(pattern | oldest | oversized)**
- **cache.clear()**

Config: `{ maxSize, location, evictionPolicy }`

### Events

- stored.on('put',           ({ checksum, key, metadata }))  
- stored.on('delete',        ({ checksum, key, backends }))  
- stored.on('sync:start',    ({ checksum, backend }))  
- stored.on('sync:complete', ({ checksum, backend }))  
- stored.on('sync:error',    ({ checksum, backend, error }))  
- stored.on('watch',         ({ type, key, backend })) // add|mod|del

## Architecture

### Metadata Object

```json
{
  "id": "sha256:abc123",              // canonical ID = primary checksum
  "checksums": {
    "sha256": "abc123",
    "xxh3": "optional"
  },
  "size": 12345,
  "mimeType": "image/png",

  "locations": [
    { "backend": "local", "key": "foo.png", "synced": true },
    { "backend": "s3-prod", "key": "blobs/abc123", "synced": false }
  ],

  "created": 1732844100000,
  "modified": 1732844100000,

  "custom": {}  
}
```

### Backend Drivers (MVP)

| Driver | Type | Watch Support |
|--------|------|---------------|
| `fs` | local | chokidar |
| `s3` | remote | polling / S3 events |

Implementation pattern: `class FsBackend extends StorageBackend`

- `fs`
  - User should be able to select local folders as data backends
  - We should support selecting a folder with existing data, and trigger the indexing of such data
- `s3`
  - We should support storing backend configuration including credentials
  - We need to support indexing of s3 buckets as well

Implementation should be flexible enough to easily add additional backends like smb, azure blob storage or supabase or even wrap rclone

### Services

#### ChecksumD

- Mandatory for ingestion only
- Do not checksum remote blobs on-the-fly—only local

#### WatchD

- Only for backends that support it (fs)
- S3 should not pretend to watch; use polling or disable entirely

#### SyncD

- Retries, exponential backoff, observability

**Queue requirements (LMDB-backed):**
- append job
- iterate jobs in order
- mark as completed / failed
- retry strategy
- ability to survive restart
- atomic operations
- counters, timestamps

Not needed: cluster mode, pub/sub, cron expressions, priorities

#### Index (required)

LMDB-backed checksum → metadata lookup. Required for persistence and efficient lookups.

## Config

```json
{
  "cache": { "path": ".cache", "maxSize": "1GB" },

  "backends": {
    "local": {
      "driver": "fs",
      "root": "./data",
      "watch": true
    },
    "s3-prod": {
      "driver": "s3",
      "bucket": "my-bucket",
      "credentials": {}
    }
  },

  "defaultBackends": ["local"],
  "checksums": ["sha256"]
}
```

## Design Decisions

- Keep StoreD stupid
- Treat metadata as canonical "truth"
- If FS fails and S3 exists, do not silently fall back unless explicitly requested
- Keep events few and clean

## Flow (PUT)

### Input

**put(blob, { key?, backends?, metadata? })**  

- blob is Buffer / Stream / file path
- key optional – overrides backend key
- backends optional – overrides default backends
- metadata optional – merged into metadata.custom

### Normalize Blob Input

The main module first converts whatever the user passed into a unified internal form:

```
InternalBlob = {
  size,
  mimeType,
  read: () => Stream,  // function that always returns a fresh readable stream
}
```

This prevents bugs where streams are consumed multiple times.

- Buffer → size known, read() returns a fresh Readable from the buffer
- File path → size from stat(), read() returns fs.createReadStream
- Incoming network Stream → size unknown at first, buffered/chunked and piped into checksum calculation

### ChecksumD: Compute Checksums

Compute required checksums **before** writing anywhere.

```
const stream = blob.read()
const checksums = await checksumD.compute(stream)
```

- Primary ID = id = sha256:abcdef...  
- Side checksums allowed: xxh3, sha1, etc.  
- ! Rewind the blob afterwards by calling blob.read() again. Never reuse the checksum stream.

### Decide the Final Key

The key is:
 - if user provided: key
 - else: <prefix>/sha256/<hash> (keeps everything nicely partitioned)
   - Example: `blobs/sha256/ab/cd/abcd1234..`

Using directory fan-out avoids filesystem performance issues.

### Create or Update Metadata (in-memory copy)

### Write to Local Cache First

- Local cache is your "universal staging area"  
  `await cache.put(id, blob)`

- Guarantees:
  - No remote backend can cause ingest slowdown.
  - Checksumming and indexing operate on local copies.
  - Remote failures won't corrupt or block ingestion.
- Emit `stored.emit('put', { checksum: id, key, metadata })`

### Insert/Update Index

Index maps: id → metadata  
  `index.put(id, meta)`  

On update:
```
meta.modified = Date.now()
index.put(id, meta)
```

Considered the canonical truth.

### Add to Sync Queue (SyncD)

- Determine Target Backends:  
  `targets = options.backends || config.defaultBackends`

- For each backend:

```
syncD.enqueue({
  id,
  key,
  backend,
  attempts: 0,
})
```
locations entry is only updated after sync completes.

### SyncD (async)

```
for job in queue:
    emit('sync:start', { id, backend })
    try:
        const stream = cache.get(id, { stream: true })
        await backend.put(job.key, stream)
        markLocationSynced(id, backend, key)
        emit('sync:complete', { id, backend })
    catch(e):
        handleError(e)
        if job.attempts < retryLimit:
            reschedule(job)
        else:
            emit('sync:error', { id, backend, error })
```

Key design point:  
Sync failures never bubble up to the main put() call.  
They are entirely async, event-driven.

### Updating Metadata After Sync Completion

markLocationSynced(id, backend, key):
```
meta = index.get(id)

update meta.locations:
   if backend entry exists → update synced=true
   else → push new { backend, key, synced=true }

meta.modified = now
index.put(id, meta)
```

### Return the Put Result Immediately

put() returns before any remote sync. Returns the full Metadata object (see Architecture > Metadata Object).

## Flow (GET)

### Input

**get(idOrChecksum, { stream?, backend? })**

- idOrChecksum: checksum (`sha256:...`) or key/path
- stream: if true, return Stream instead of Buffer
- backend: specific backend to fetch from (optional)

### Lookup

1. Check cache first:
   ```
   cached = cache.get(idOrChecksum, { stream })
   if (cached) return cached
   ```

2. Query Index for metadata:
   ```
   meta = index.get(idOrChecksum)
   if (!meta) throw NotFoundError
   ```

3. Find available location:
   ```
   location = meta.locations.find(loc => loc.synced)
   if (!location) throw NotFoundError
   ```

### Fetch from Backend

```
backend = getBackend(location.backend)
data = await backend.get(location.key, { stream })
```

### Cache Remote Fetches

If fetched from a remote backend, cache locally for future access:
```
if (backend.type === 'remote') {
  cache.put(idOrChecksum, data)
}
```

### Return

Return Buffer or Stream based on options.stream flag.

## Flow (DELETE)

### Input

**delete(idOrChecksum, { backends? })**

- idOrChecksum: checksum (`sha256:...`) or key/path
- backends: specific backends to delete from (optional, defaults to all locations)

### Lookup

```
meta = index.get(idOrChecksum)
if (!meta) throw NotFoundError
```

### Determine Targets

```
targets = options.backends 
  ? meta.locations.filter(loc => options.backends.includes(loc.backend))
  : meta.locations
```

### Remove from Cache

```
cache.delete(idOrChecksum)
```

### Queue Deletion from Backends

For each target location:
```
syncD.enqueue({
  type: 'delete',
  id: idOrChecksum,
  key: location.key,
  backend: location.backend,
})
```

### Update Index

If deleting from all backends:
```
index.delete(idOrChecksum)
```

If deleting from specific backends only:
```
meta.locations = meta.locations.filter(loc => !targets.includes(loc))
meta.modified = Date.now()
index.put(idOrChecksum, meta)
```

### Emit Event

```
emit('delete', { checksum: idOrChecksum, key, backends: targets.map(t => t.backend) })
```

### Return

```
{ deleted: targets.map(t => t.backend) }
```
