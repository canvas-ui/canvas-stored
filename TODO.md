# StoreD

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

### Cache Management

- **cache.stats()** – size, entries, hits/misses
- **cache.evict(pattern | oldest | oversized)**

Config: `{ maxSize, location, evictionPolicy }`

## Architecture

### Backend Drivers (MVP)

- `s3`
  - We should support storing backend configuration including credentials
  - We need to support indexing of s3 buckets as well

Implementation should be flexible enough to easily add additional backends like smb, azure blob storage or supabase or even wrap rclone

### Services

#### EmbedD

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
