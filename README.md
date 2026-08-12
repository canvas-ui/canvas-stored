<p align="center">
  <img src="https://raw.githubusercontent.com/canvas-ai/.github/main/banners/canvas-banner_1200x480.jpg" alt="Canvas" width="100%" />
</p>

# StoreD

Cache-first blob storage with a content-addressable local cache, LMDB metadata index and pluggable backends. Local backends write immediately remote backends sync through a worker-thread queue.

Used with canvas-server/synapsd to build virtual context trees over indexed data: checksum-based identity, multi-location replication, and `stored://` URLs as the canonical fetch form.

```
put(blob)  → stream hash → local backends (hardlink/rename) + remote (cacache → SyncQueue / worker_threads)
get(id)    → cacache → synced backend location → cache on read (buffers only)
```

Paths and streams are ingested without buffering the whole blob in memory (safe for 10GB+ files). Local-only puts never touch the cache; the cache + sync queue are used only when a remote backend is targeted.

## Quick start

### Simple example
```js
import Stored from './src/index.js';

const stored = new Stored({ root: './.stored' });
stored.addBackend('fs:data', { driver: 'file', root: stored.dataPath });
```

### Backend configuration
```js
const stored = new Stored({
  root: './.stored',                    // → ./.stored/{index,cache,data}
  checksums: ['sha256'],                // sha256 is the default and the id/cache algorithm
  primaryChecksum: 'sha256',
});

stored.addBackend('fs:home', {
  driver: 'file',
  root: './home',                       // any path - not tied to .stored/data
  watch: true,
  ignored: /node_modules/,
});

stored.on('object:add', ({ kind, id, key }) => { /* index in synapsd */ });
stored.on('synced', ({ id, results }) => { /* remote write finished */ });

// `backends` is required - there is no implicit fan-out.
const res = await stored.put(Buffer.from('hello'), { key: 'docs/hello.txt', backends: ['fs:home'] });
const data = await stored.get(res.id);
await stored.getByUrl('stored://fs:home/docs/hello.txt');

await stored.stop();
```

---

## Configuration options

All paths hang off a single **`root`** (default `./.stored`):

```
.stored/
  index/     LMDB metadata + path index
  cache/     cacache (content-addressable)
  data/      default root for an optional local file backend
```

| Option | Default | Description |
|--------|---------|-------------|
| `root` | `./.stored` | Base directory for index, cache, and data |
| `index.path` | `<root>/index` | Override index location |
| `cache.path` | `<root>/cache` | Override cacache location |
| `data.path` | `<root>/data` | Override local data directory |
| `checksums` | `['sha256']` | Algorithms computed on `put` / `scan`. sha256 is mandatory (id + cache); extra algorithms are optional |
| `primaryChecksum` | `'sha256'` | Algorithm used for content `id` (`<algo>:<hex>`) |

The constructor extends **eventemitter2** with `wildcard: true` and `delimiter: ':'`, so listeners can bind patterns like `object:*` or `scan:*`.

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `root` | `string` | Resolved store root |
| `paths` | `{ root, index, cache, data }` | All resolved paths |
| `dataPath` | `string` | `<root>/data` (convenience) |
| `cache` | `Cache` | cacache wrapper |
| `index` | `Index` | LMDB metadata store |
| `backends` | `BackendManager` | Registered backend instances |

The `cache` wrapper exposes cacache maintenance helpers directly: `stats()` (entry count + total size, streamed so large stores are never held in memory), `clear()`, `verify()`, and `list()` / `listStream()`.

---

## Metadata

`put()`, `scan()`, and file watchers populate the index. `stat()` / `has()` / `get()` accept either a content **id** or a path key **`{backend}:{key}`**.

```js
{
  id: 'sha256:abc...',           // primaryChecksum:hex
  checksums: { sha256: '...', md5: '...' },
  size: 1234,
  mimeType: 'text/plain',
  created: 1710000000000,
  modified: 1710000000000,
  custom: { /* from put(..., { metadata }) */ },
  locations: [
    {
      backend: 'fs:home',
      driver: 'file',
      key: 'docs/hello.txt',
      synced: true,              // false until remote SyncQueue completes
      source: {                  // provenance descriptor for synapsd / UI
        provider: 'fs',
        account: 'home',
        container: 'home',         // basename of root, or bucket/share/folder from config
        path: 'docs/hello.txt',
      },
    },
  ],
}
```

Auto-generated keys (when `put` has no `key`) use a sharded hash path: `ab/cd/<full-hex>`.

---

## `stored://` URLs

Canonical fetch/delete form: `stored://<backend>/<key>`. Backend names may contain colons (`fs:data:email`); only the **first** `/` after the scheme separates backend from key.

| Method | Description |
|--------|-------------|
| `getByUrl(url)` | `backend.get(key)` - bypasses the content index. Returns `Buffer \| null` (never throws on bad input). |
| `getStreamByUrl(url)` | Same as `getByUrl` but returns a `Readable \| null`. |
| `deleteByUrl(url)` | Deletes bytes on the backend only (does not update the LMDB index). Returns `{ ok: boolean, reason?: string }` where `reason` is `malformed-url`, `unknown-backend`, or `read-only-backend`. |

---

## API

### Backend management

| Method | Returns | Description |
|--------|---------|-------------|
| `addBackend(name, config)` | `StorageBackend` | Register driver; wire events; start `watch()` if `config.watch` |
| `removeBackend(name)` | `Promise<boolean>` | `stop()` backend and unregister |
| `listBackends()` | `string[]` | Registered backend names |
| `getBackend(name)` | `StorageBackend \| undefined` | Instance by name |

**`addBackend` config** (common):

| Field | Description |
|-------|-------------|
| `driver` | `'file'` \| `'cacache'` \| `'s3'` \| `'http'` |
| `watch` | Start backend watcher when true (file: chokidar) |
| `root` | File backend root directory (required for `file`) |
| `ignored` | chokidar ignore pattern (file backend) |
| `provider`, `account`, `container`, `bucket`, `share`, `folder` | Override `location.source` fields (else derived from backend name + config) |

Throws if the name exists or the driver is unknown.

### Core

#### `put(blob, options?) → Promise<{ ok, id, key, size, mimeType, checksums, locations, … }>`

Accepts `Buffer`, `string` (content), filesystem path (`string` path readable as file), or `Readable` stream. Paths and streams are hashed and written in a single streaming pass - the whole blob is never held in memory.

| Option | Default | Description |
|--------|---------|-------------|
| `key` | auto from checksum | Storage key on backends |
| `backends` | **required** | Target backend names. Empty/missing → `{ ok:false, reason:'no-targets' }` |
| `metadata` | `{}` | Stored as `custom` on the index entry |
| `mimeType` | sniffed | Override the detected mime type |

Flow: stream → hash (+ head peek for mime) → local backends (hardlink/rename from a same-fs temp) + remote placeholders (streamed into cacache) → index → enqueue remote sync. Returns `{ ok:false, reason }` on failure (`no-targets`, `unknown-backend`).

Emits `put` with `{ id, key, metadata }`.

#### `get(idOrKey) → Promise<Buffer \| null>` / `getStream(idOrKey) → Promise<Readable \| null>`

Lookup order: path index (`backend:key`) or id → cacache → first **synced** location on a backend. On a backend buffer hit, bytes are written back to cache asynchronously.

#### `delete(idOrKey, options?) → Promise<{ ok, deleted, kept } \| { ok:false, reason:'not-found' }>`

Removes from cache and backends. The index entry is removed only when no locations remain.

| Option | Description |
|--------|-------------|
| `backends` | Only delete locations on these backends |
| `urls` | Only delete the listed `stored://<backend>/<key>` locations (precise multi-name targeting) |

Emits `delete` with `{ id, backends }` (names deleted).

#### `stat(idOrKey) → Promise<Metadata | null>`

#### `has(idOrKey) → Promise<boolean>`

#### `locations(idOrKey) → Promise<Location[]>`

Where the content actually lives: `{ url, nativeUrl, backend, key, driver, synced, size, source }` per location.

- `url` - canonical `stored://<backend>/<key>`, the fetch form (`getByUrl`). Single source of truth for the grammar.
- `nativeUrl` - the real protocol URL for provenance/UI: `https://…`, `s3://bucket/…`, `imap://account/…`, `file://…` (local), or `null` when the backend has none.

Returns `[]` when unknown. Backend drivers render `nativeUrl(key)`; remote backends are the interesting case (a blob can live under several names across backends).

#### `list() → AsyncIterable<Metadata>`

Yields all index metadata entries.

#### `listBackend(name, options?) → AsyncIterable`

Yields `{ key, size, … }` from that backend’s native `list()`. `options` (`prefix`, `recursive`) are passed through.

#### `scan(backendName?, options?) → Promise<{ ok, backend, count, files }>`

Index existing objects from one backend or all. `options` is forwarded verbatim to each backend's `scan()` (per-driver knobs). File backends skip re-hashing files whose size+mtime are unchanged. Updates locations, prunes index entries for keys removed on disk; `files` are per-file scan rows `{ key, size, checksums, mimeType, backend, … }`.

### Lifecycle

#### `stop() → Promise<void>`

Stops sync queue, all backend watchers, and closes the LMDB index.

---

## Backends

Registered in `BackendManager` via `DRIVERS`:

| Driver | Type | Status | Notes |
|--------|------|--------|-------|
| `file` | `local` | complete | CRUD, chokidar `watch`, `scan` |
| `cacache` | `local` | complete | Content-addressable blob store (cacache); CRUD, no `watch`/`scan` |
| `http` | `remote` | partial | Read-only `get` / `stat` via `fetch`; `config.baseUrl`, `headers` |
| `s3` | `remote` | skeleton | Registered for URL parsing; CRUD not implemented |

Each backend implements `StorageBackend`: `put`, `get`, `delete`, `stat`, `list`; optional `commit` (local, for streaming put), `watch`, `scan`, `stop`.

> StoreD abstracts **blob** backends only. Non-blob connectors (mail/IMAP, git, …) are not drivers here - they live in separate consumer services and use StoreD only to persist the blobs they produce. The generic `object:*` / `backend:state` event surface still lets such a connector emit through a host that wraps StoreD, but the IMAP protocol itself is no longer bundled.

| Capability | Meaning |
|------------|---------|
| `capabilities.delete === false` | `deleteByUrl` returns `read-only-backend` (e.g. HTTP) |
| `type === 'local'` | Written synchronously on `put` |
| `type === 'remote'` | Written via `SyncQueue` after cache write |

### `file` driver config

```js
{ driver: 'file', root: '/path/to/root', watch: true, ignored: /pattern/, algorithms: ['sha256'] }
```

### `cacache` driver config

```js
{ driver: 'cacache', root: '/path/to/blobstore', algorithms: ['sha256'] }
```

Content-addressable local blob store backed by [cacache](https://www.npmjs.com/package/cacache): bytes are sha-keyed, deduped, and integrity-checked. Same key→value CRUD surface as `file` (`put`/`commit`/`get`/`delete`/`stat`/`list`), so it is a drop-in `type: 'local'` write target - bytes land synchronously on `put`, no `SyncQueue`. `config.root` is the only required field ("the data route"); `nativeUrl` is `null` (the store is internal - `stored://<backend>/<key>` is the sole address) and the store is **not** watched/scanned (a managed write target, not an external source). canvas-server wires it as the opt-in `workspace:data` backend (disabled by default).

### `http` driver config

```js
{ driver: 'http', baseUrl: 'https://cdn.example/', account: 'cdn', headers: {} }
```

---

## Events

Stored re-emits backend events and adds its own. File watcher events are duplicated as **`file:*`** and **`object:*`** (`kind: 'file'`) so consumers can bind either `file:add` or `object:add`.

### File / object (from file backend + Stored index updates)

| Event | Payload (typical) |
|-------|-------------------|
| `file:add` | `{ backend, key, path, checksums, mimeType, size, id, locations }` |
| `file:change` | Same shape on modify (emits `object:unlink` for old content, then `object:add` for new) |
| `file:unlink` | `{ backend, key, path?, id?, checksums?, locations? }` |
| `object:add` | `{ kind: 'file', … }` (or `{ kind, backend, key, … }` from a non-file connector emitting through stored) |
| `object:change` | Forwarded from backends that emit it |
| `object:unlink` | Forwarded or synthesized on file delete |

Wildcard: `object:*`, `file:*`, `scan:*`.

### API / sync / scan

| Event | Payload |
|-------|---------|
| `put` | `{ id, key, metadata }` |
| `delete` | `{ id, backends }` |
| `synced` | `{ id, results }` - `results[]` has `{ backend, success, … }`; successful rows set `location.synced = true` |
| `scan:start` | `{ backend }` |
| `scan:complete` | `{ backend, count }` |
| `backend:state` | Backend-specific cursor/state `{ backend, … }` (forwarded for any connector that emits it) |
| `error` | From backends or sync queue |

---

## Workspace integration

canvas-server owns a `Stored` instance per workspace at `{WORKSPACE_ROOT}/.stored` (index + cacache). Workspace blobs still live under `data/`, `home/`, etc. as separate file backends. `WorkspaceStoredIndex` listens on `object:*`, builds synapsd documents with `stored://` locations, and resolves blobs via `getByUrl`. See `STORAGE-URL-SCHEME.md` at the repo root for the full URL grammar.

## Licence

Copyright (C) 2025-2026 Jozef Melich. Canvas StoreD is dual-licensed:

- **[AGPL-3.0-or-later](LICENSE)**, free for everyone. Run it, modify it, build
  on it. If you distribute a modified version, or expose one to users over a
  network, they are entitled to your changes (AGPL section 13).
- **[Commercial licence](COMMERCIAL.md)**, the same code without the copyleft
  obligations, for hosted products and proprietary distribution. Issued by
  Augmentd s.r.o., lic@augmentd.eu.

Same software either way. There is no cut-down community edition. See
[NOTICE](NOTICE) for the full position, and [CONTRIBUTING.md](CONTRIBUTING.md)
before opening a pull request.
