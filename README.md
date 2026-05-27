# StoreD

Cache-first blob storage with a content-addressable local cache, LMDB metadata index, and pluggable backends. Local backends write immediately; remote backends sync through a worker-thread queue.

Used with canvas-server/synapsd to build virtual context trees over indexed data: checksum-based identity, multi-location replication, and `stored://` URLs as the canonical fetch form.

```
put(blob)  → cacache → local backends (immediate) → remote backends (SyncQueue / worker_threads)
get(id)    → cacache → synced backend location → cache on read (buffers only)
```

## Quick start

### Simple example
```js
import Stored from './src/index.js';

const stored = new Stored({ root: './.stored' });

// manual data backend
stored.addBackend('fs:data', { driver: 'file', root: stored.dataPath });

// or auto-register
new Stored({ root: './.stored', data: { backend: 'fs:data', watch: true } });
```

### Backend configuration
```js
const stored = new Stored({
  root: './.stored',                    // → ./.stored/{index,cache,data}
  checksums: ['sha256'],
  primaryChecksum: 'sha256',
  defaultBackends: ['fs:home'],
  // data: { backend: 'fs:data', watch: true },  // optional local file backend at root/data
});

stored.addBackend('fs:home', {
  driver: 'file',
  root: './home',                       // any path — not tied to .stored/data
  watch: true,
  ignored: /node_modules/,
});

// Or use the built-in data dir:
// stored.addBackend('fs:data', { driver: 'file', root: stored.dataPath });

stored.on('object:add', ({ kind, id, key }) => { /* index in synapsd */ });
stored.on('synced', ({ id, results }) => { /* remote write finished */ });

const meta = await stored.put(Buffer.from('hello'), { key: 'docs/hello.txt' });
const data = await stored.get(meta.id);
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
| `data.backend` | — | If set (e.g. `'fs:data'`), registers a `file` backend at `data.path` |
| `data.watch` | `false` | Passed to the auto-registered data backend |
| `checksums` | `['sha256']` | Algorithms computed on `put` / `scan` |
| `primaryChecksum` | `'sha256'` | Algorithm used for content `id` (`<algo>:<hex>`) |
| `defaultBackends` | `[]` | Backend names for `put()` when `options.backends` is omitted; if empty, all registered backends are targeted |

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
| `getByUrl(url, options?)` | `backend.get(key, options)` — bypasses the content index. Throws if `url` is not `stored://…`. Returns `null` for unknown backend or malformed URL. |
| `deleteByUrl(url)` | Deletes bytes on the backend only (does not update the LMDB index). Returns `{ deleted: boolean, reason?: string }` where `reason` is `malformed-url`, `unknown-backend`, or `read-only-backend`. |

`options` for `getByUrl`: `{ stream: true }` returns a stream when the backend supports it.

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
| `driver` | `'file'` \| `'s3'` \| `'http'` \| `'imap'` |
| `watch` | Start backend watcher when true (file: chokidar; imap: poll + scan) |
| `root` | File backend root directory (required for `file`) |
| `ignored` | chokidar ignore pattern (file backend) |
| `provider`, `account`, `container`, `bucket`, `share`, `folder` | Override `location.source` fields (else derived from backend name + config) |

Throws if the name exists or the driver is unknown.

### Core

#### `put(blob, options?) → Promise<Metadata>`

Accepts `Buffer`, `string`, filesystem path (`string` path readable as file), or `Readable` stream.

| Option | Default | Description |
|--------|---------|-------------|
| `key` | auto from checksum | Storage key on backends |
| `backends` | `defaultBackends`, or all registered if that is empty | Target backend names |
| `metadata` | `{}` | Stored as `custom` on the index entry |

Flow: normalize blob → checksum + mime → cache → local backends (synced) + remote placeholders → index → enqueue remote sync.

Emits `put` with `{ id, key, metadata }`.

#### `get(idOrKey, options?) → Promise<Buffer \| Stream \| null>`

Lookup order: path index (`backend:key`) or id → cacache → first **synced** location on a backend.

| Option | Description |
|--------|-------------|
| `stream: true` | Return a read stream (cache or backend) |

On backend hit, buffers are written back to cache asynchronously.

#### `delete(idOrKey, options?) → Promise<{ deleted: string[] }>`

Removes from cache and backends. Updates or removes the index entry.

| Option | Description |
|--------|-------------|
| `backends` | If set, only delete from these backends; index entry kept until all locations are gone |

Emits `delete` with `{ id, backends }` (names deleted).

#### `stat(idOrKey) → Metadata | null`

#### `has(idOrKey) → boolean`

#### `list(options?) → AsyncIterable`

| Option | Behavior |
|--------|----------|
| *(none)* | Yield all index metadata entries |
| `backend` | Yield `{ key, size, … }` from that backend’s `list()` |
| `prefix` | Passed to backend `list()` when listing a backend |

#### `scan(backendName?) → Promise<ScanResult[]>`

Index existing objects from one backend or all. Updates locations, prunes index entries for keys removed on disk, returns per-file scan rows `{ key, size, checksums, mimeType, backend, … }`.

### Lifecycle

#### `stop() → Promise<void>`

Stops sync queue, all backend watchers, and closes the LMDB index.

---

## Backends

Registered in `BackendManager` via `DRIVERS`:

| Driver | Type | Status | Notes |
|--------|------|--------|-------|
| `file` | `local` | complete | CRUD, chokidar `watch`, `scan` |
| `http` | `remote` | partial | Read-only `get` / `stat` via `fetch`; `config.baseUrl`, `headers` |
| `s3` | `remote` | skeleton | Registered for URL parsing; CRUD not implemented |
| `imap` | `remote` | functional | Poll + `scan`; keys `<folder>;UID=<n>`; emits `object:add` (`kind: 'message'`) |

Each backend implements `StorageBackend`: `put`, `get`, `delete`, `stat`, `list`; optional `watch`, `scan`, `stop`.

| Capability | Meaning |
|------------|---------|
| `capabilities.delete === false` | `deleteByUrl` returns `read-only-backend` (e.g. HTTP) |
| `type === 'local'` | Written synchronously on `put` |
| `type === 'remote'` | Written via `SyncQueue` after cache write |

### `file` driver config

```js
{ driver: 'file', root: '/path/to/root', watch: true, ignored: /pattern/, algorithms: ['sha256'] }
```

### `http` driver config

```js
{ driver: 'http', baseUrl: 'https://cdn.example/', account: 'cdn', headers: {} }
```

### `imap` driver config

```js
{
  driver: 'imap',
  host, port: 993, tls: true, allowSelfSigned: false,
  user, password,
  folder: 'INBOX',
  account: 'user@example.com',
  pollInterval: 60000,
  initialSyncDays: 180,
  lastUid: 0,
}
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
| `object:add` | `{ kind: 'file', … }` or from IMAP: `{ kind: 'message', backend, key, … }` |
| `object:change` | Forwarded from backends that emit it |
| `object:unlink` | Forwarded or synthesized on file delete |

Wildcard: `object:*`, `file:*`, `scan:*`.

### API / sync / scan

| Event | Payload |
|-------|---------|
| `put` | `{ id, key, metadata }` |
| `delete` | `{ id, backends }` |
| `synced` | `{ id, results }` — `results[]` has `{ backend, success, … }`; successful rows set `location.synced = true` |
| `scan:start` | `{ backend }` |
| `scan:complete` | `{ backend, count }` |
| `backend:state` | Backend-specific (e.g. IMAP `{ backend, lastUid }`) |
| `error` | From backends or sync queue |

---

## Workspace integration

canvas-server owns a `Stored` instance per workspace at `{WORKSPACE_ROOT}/.stored` (index + cacache). Workspace blobs still live under `data/`, `home/`, etc. as separate file backends. `WorkspaceStoredIndex` listens on `object:*`, builds synapsd documents with `stored://` locations, and resolves blobs via `getByUrl`. See `STORAGE-URL-SCHEME.md` at the repo root for the full URL grammar.
