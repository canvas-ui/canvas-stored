# StoreD

## Roadmap

Ordered by what unblocks what. P0/P1 are the MVP gap; everything below is
already-known work kept for context.

---

## P0 — `object:move` + location events ✅ DONE

Shipped in stored (`copy()`/`move()` emit them, `delete()` emits
`location:remove`) and bound in canvas-server
(`WorkspaceStoredIndex.#applyLocationChange`). Verified end-to-end against the
dev server: a move between backends keeps the document id, swaps `locations[]`
in place and leaves no `feature/orphaned` marker.

One deviation from the plan below: **no in-flight-move set is needed in
canvas-server.** Stored suppresses watcher events for the keys it writes and
deletes during a transfer (`#suppressedKeys`, `MOVE_SUPPRESS_MS`), so the
source-path unlink never reaches the server at all. `#purgeOrphanedPaths` only
runs on resync scans, which read live state — and since the destination
location is recorded *before* the source is touched, a resync racing a move can
only ever converge on the post-move truth.

**Why first:** every other item here (copy/move, pull-through cache, backend
migration) mutates `locations[]` without changing content identity. Today the
only vocabulary stored has for that is `object:add` / `object:unlink`, and a
consumer that sees an unlink drops the location — in canvas-server,
`WorkspaceStoredIndex.#purgeOrphanedPaths` / `#reconcileRemovedLocations` will
strip it and orphan the doc (`feature/orphaned`), losing its tree positions and
document id. The event surface has to exist before the operations that need it.

Intra-backend renames are already handled (inode pairing in `#handleFileEvent`
holds an unlink and swallows it when a same-inode add lands inside the window).
What is missing is the **cross-backend** case, where there is no shared inode
and no watcher pairing to lean on.

### New events

| Event | Payload | Meaning |
|-------|---------|---------|
| `object:move` | `{ kind, id, checksums, from: { backend, key, url }, to: { backend, key, url }, locations }` | Same content id, one location replaced by another. Consumers **update in place** — never unlink+add. |
| `object:location:add` | `{ kind, id, location, locations }` | A copy landed: content gained a location, kept all existing ones. |
| `object:location:remove` | `{ kind, id, location, locations, reason }` | One location dropped, content still lives elsewhere. `reason`: `moved` \| `deleted` \| `evicted` \| `backend-removed`. |

Rules:
- All three go through `#emitObject()` so the `file:*` / `object:*` duality and
  the `kind` tag stay consistent with the existing surface.
- `locations` on the payload is always the **post-mutation** full array (same
  shape as `locations()`), so a consumer never has to re-derive it.
- `object:move` is emitted **only** after the destination write is durable —
  for a `type:'remote'` target that means after the `synced` event, not after
  the enqueue. See the two-phase note in P1.
- `object:location:remove` with `reason:'evicted'` must never be treated as
  data loss by consumers — it is the cache/eviction path, not a delete.

### canvas-server side

- `WorkspaceStoredIndex` binds the three new events and patches `doc.locations`
  in place (`#put` with just `{ id, locations }`, as `#reconcileRemovedLocations`
  already does) — no unlink, no re-add, no tree-position churn.
- ~~`#purgeOrphanedPaths` must not race a move: in-flight move set~~ — not
  needed, see the note at the top of this section.
- Where the upsert bails because the target has no mirrored backends-tree path
  (the managed blob store is opaque by design), locations are patched directly
  via `#patchDocumentLocations` — a document must never keep pointing at a
  location the object no longer has.

---

## P0 — `remote` flag on the file driver ✅ DONE

Detection (`src/utils/mount.js`, `/proc/mounts`, Linux), the `remote`/`transport`
axis on `StorageBackend`/`FileBackend`, the watch refusal, local-before-remote
read preference and the `locations()`/`source` surfacing all landed, with
`remote`/`transport` plumbed through canvas-server's backend descriptor for the
UI badge. Still open from this section: **lazy/path-first indexing for large
remote mounts** — full-file hashing over the wire is what will dominate the
128GB PoC, and `shape()` + the size+mtime skip predicate are the levers.

**Why now:** blocks the 128GB NAS PoC. An OS-mounted NFS/CIFS share is a
POSIX path to the `file` driver and gets treated exactly like local disk —
which is wrong in ways that fail quietly.

### Decision: OS mounts, no SMB/CIFS driver

Remote shares are consumed as **OS-level mounts** (kernel cifs/nfs, or
`rclone mount` for the rest), not via a protocol driver in stored. Rationale:

- The `file` driver already carries the whole remote-mount vocabulary —
  `createRoot:false`, `verifyRoot()` + fsid `{dev,ino}` snapshot,
  `complete:false` partial scans, `errors.dirs` orphan-not-delete, `onFile`
  streaming, the shared ignore matcher. A userspace SMB driver re-implements
  all of it and loses the parts that matter.
- **No stable inode over SMB.** `#knownByInode()` rename-matching and the
  size+mtime skip-hash both key off `stat.dev/ino`; without them every scan of
  a large share re-hashes over the wire.
- **No `getRange` parity.** Ranged reads (HTTP Range, canvas-fuse) come free
  from `createReadStream(start, end)` on a mount.
- **Kernel caching + readahead** beats any JS SMB client on the actual
  workload (hash many files sequentially).
- Docker/VPS is a deployment concern: a compose volume with
  `driver_opts: { type: cifs, o: "username=…,vers=3.0" }` needs no privileged
  container.

**Revisit trigger:** per-workspace credentials. Mounts are host-global and
root-provisioned, so multi-tenant hosted Canvas cannot let a user self-service
their own share. When that fires, build the **`s3` driver first** — it forces
credential storage, the `type:'remote'` sync path and polling-instead-of-watch,
all of which an `smb` driver would then reuse.

### Config

```js
{ driver: 'file', root: '/mnt/nas/photos', remote: true, transport: 'cifs' }
```

- Auto-detect as the **default**, user-overridable. Implemented as a synchronous
  `/proc/mounts` read (longest matching mountpoint wins) rather than a `findmnt`
  subprocess, so it can run in the FileBackend constructor without forcing every
  `addBackend()` caller to become async. `cifs|smb3|nfs|nfs4|fuse.*|afpfs|…`
  count as remote; non-Linux platforms degrade to "local unless configured".
- Persist the detection on the config alongside the fsid snapshot, on the same
  hook that already writes fsid after the first successful scan.
- **Do not overload `backend.type`.** `type: 'local'|'remote'` currently means
  *synchronous write vs SyncQueue*, and a CIFS mount is still a synchronous
  POSIX write target. `remote`/`transport` is a separate axis.
- Surface on `capabilities`, on the location `source` descriptor, and in
  `locations()` output so canvas-server/UI can badge it.

### Behaviour that must change when `remote: true`

- **`watch` defaults to `false`.** inotify does not see other clients' writes
  on cifs/nfs — a watched share goes silently stale, which is worse than not
  watching. Scheduled resync instead; chokidar `usePolling` with a long
  interval only as an explicit opt-in.
- **Liveness already correct — keep it.** An unmounted share either vanishes or
  exposes the underlying local dir with a different `dev`; `verifyRoot()`
  catches both and returns `complete:false` with **zero rows**. The remote path
  must never bypass it: absent mount ≠ empty mount.
- **Read preference.** `#read()` takes `locations.find(l => l.synced)` — first
  match wins regardless of where it is, which can route a read across the LAN
  when a local copy exists. Order must be: cache → local location → remote
  location.
- **Hashing is the bottleneck, not enumeration.** For the 128GB PoC the first
  index pass is dominated by full-file checksums over the wire. `shape()`
  (readdir-only) already exists — evaluate path-first indexing with lazy
  hash-on-first-read for remote backends, and make sure the size+mtime
  `knownChecksums` skip predicate is wired on every remote resync.

---

## P1 — copy / move object between backends ✅ DONE (stored + server + UI)

`stored.copy()` / `stored.move()` shipped with the same-filesystem `rename(2)`
fast path, hash verification, add-first ordering and the pending-sync path for
remote targets. canvas-server exposes it as
`POST /workspaces/:ws/backends/:driver/:address/objects/transfer`
(`{ key, to, targetKey?, mode: 'copy'|'move' }`) via
`Workspace.transferBackendObject`; both endpoints must be enabled backends
(a transient registration would be pruned afterwards and undo the transfer).

Storage policies landed too, as the hook-rule action `store` (canvas-server):
declarative move/copy of a document's bytes between backends with
`{{YYYY}}/{{MM}}/{{ext}}`-style key templating (EXIF capture time first) and
`onConflict` handling — the "images always live in workspace:home/Upload/Images"
use case this roadmap predicted would be Workspace-hook material, plus a
`store` row in the web rule builder. stored gained `onConflict`
(`error`|`rename`|`overwrite`, per-target keys) and destination-key
normalization to support it.

The web UI ships as `BackendActionCard` (apps/web) — the backend twin of
`LinkToCard`: one card with Copy to / Move to / Delete from modes over a
checkbox list of storage backends (radio in move mode, since a move has one
destination), reachable from the selection toolbar's "Backends…" button and a
BACKENDS group in the document context menu. It replaces the old
delete-from-all-backends item, which the code comment had already flagged as
awaiting per-backend selection. Batch API:
`POST /workspaces/:ws/documents/transfer` `{ documentIds, to[], mode, keepDocument? }`
— addressed by document id (what a selection holds), with each document's own
source location resolved server-side so external mounts (device `file://`
locations) work without the client knowing the address grammar.

**Bug this surfaced, now fixed:** `deleteByUrl` used to leave stored's index
claiming a location whose bytes it had just deleted, so a later `copy()` skipped
the target as "already there" and reported success for a no-op. `deleteByUrl`
now drops the location (emitting `object:location:remove`), `#planTransfer`
stats a recorded target before skipping it, and a genuine no-op is reported as
`state: 'unchanged'` rather than `complete` all the way up to the toast.

The missing primitive. **Not** backend-to-backend sync — that stays out of MVP.
MIME-routing rules ("images → `workspace:home/Upload/Images`", "pdfs →
`…/Papers`") are Workspace-hook material: `put()` already takes `backends: []`,
and that array *is* the extension point. A hook picks it; stored stays dumb.

`stored.copy(idOrKey, { to: [backends], key? })` / `stored.move(...)`:

- **Copy** = stream the source location → the existing `#commit` path targeted
  at the new backends → append location(s) to the **same** index entry. Hash
  while streaming and compare against `meta.checksums`: the stream is already
  being paid for, and it catches source rot.
- **Move** = copy + drop the source location. Ordering is load-bearing: never
  drop the old location until the new one is committed **and**, for a
  `type:'remote'` target, until `synced` fires. A failed S3 sync must not
  orphan the doc. Expose the in-between state rather than blocking.
- **Same-filesystem shortcut.** When source and target `stat().dev` match,
  `fs.rename` instead of stream-copy — instant vs. minutes on a large file.
- Emits `object:move` / `object:location:*` from P0, never unlink+add.
- Guards: target `capabilities.write` (refuse `http`), source
  `capabilities.read`, target liveness via `verifyRoot()` before starting.

**3-part delivery:**
1. stored — `copy()` / `move()` + events.
2. canvas-server — route alongside `routes/workspaces/backends.js`, with the
   capability/liveness gates and the in-flight-move suppression from P0.
3. web UI — "Copy to… / Move to…" against the backends tree, progress on the
   channel resync already uses.

Note: this also delivers "make available offline" (copy to `workspace:data`)
without any eviction policy to get wrong — which is why it lands **before** the
pull-through cache below.

---

## P2 — cache policy / pull-through cache for remote objects

cacache has no size cap and no LRU; both are ours to build.

- Store-level `cache: { maxSize, maxObjectSize, policy: 'lru' }`, per-backend
  override.
- cacache's index `time` is **insert** time — real LRU needs an atime we track.
  LMDB is already open; a `cache:atime` map touched on hit is a few lines.
- Evict on a high/low watermark (cross `maxSize` → evict oldest down to ~80%),
  debounced after cache writes, plus a periodic sweep. `listStream()` already
  yields `{ size, time }` without materializing the index.
- **Only populate from remote reads.** A local-backend hit must not consume
  cache budget — same principle as local-only puts skipping the cache today.
- **Cap object size.** Ranged reads on a remote mount pass straight through
  `getRange`; only whole-object reads populate. Otherwise one 4K video eats the
  budget.
- **Stream reads currently never populate the cache** (`#read` write-back is
  buffer-only). For remote that is backwards — the big files are exactly the
  ones worth caching. `PassThrough` tee into `cache.putStream` on remote stream
  hits.
- Eviction emits `object:location:remove` with `reason:'evicted'` (P0), never a
  delete.

### API

- **cache.stats()** – size, entries, hits/misses
- **cache.evict(pattern | oldest | oversized)**

Config: `{ maxSize, location, evictionPolicy }`

---

## P2 — Batch operations with a configurable batch size

Scan/index/resync currently processes files one at a time (the consumer-side
resync loop does a sequential per-file upsert, which is O(n) DB round-trips and
slow for large or remote backends). `scan()` should yield/return work in
batches and downstream indexing should consume them in batches (e.g. via
synapsd `putMany` / `putManyDirectoryPaths` — one transaction + one bitmap
flush per batch). Batch size must be configurable (per backend and/or global,
e.g. `config.batchSize`, default ~500) to trade off memory vs throughput, and
the same batching should apply to delete/sync queue draining where backends
support bulk operations.

Directly relevant to the 128GB NAS PoC — pairs with the P0 remote work.

---

## Architecture (unchanged)

### Backend Drivers

- `s3` (next real driver — see the revisit trigger under the `remote` flag)
  - We should support storing backend configuration including credentials
  - We need to support indexing of s3 buckets as well

Implementation should be flexible enough to easily add additional backends like
azure blob storage or supabase or even wrap rclone. `smb`/`webdav` stay
reserved scheme names with no driver — remote shares are OS mounts.

### Services

#### WatchD

- Only for backends that support it (fs)
- S3 should not pretend to watch; use polling or disable entirely
- Remote fs mounts (`remote: true`) likewise — inotify is not reliable there

#### SyncD ✅ DONE (1.6.0 — `src/sync/JobQueue.js`, `Mirror.js`, `Ledger.js`)

- Retries, exponential backoff (`min(60s·2^n, 1h)`), observability (`counters()`, `status()`, `job:*` events) ✅

**Queue requirements (LMDB-backed):** all landed in `JobQueue`
- append job ✅ (`append`, dedupe per `<kind>|<key>` replaces a pending job)
- iterate jobs in order ✅ (`pending(now)`, `all()`)
- mark as completed / failed ✅ (`complete`, `fail`, `retry`, `cancel`)
- retry strategy ✅ (backoff; `permanent` parks the job)
- ability to survive restart ✅ (`recover()`: running→pending on open)
- atomic operations ✅ (LMDB transactions)
- counters, timestamps ✅

Sync policies ✅: the device mirror (`Mirror`) implements the protocol's
three-way table with `deletes: propagate|keep`, `conflictMode: prompt|rename`,
`prefixes`/`ignore`. The in-process `SyncQueue` (cache → remote `commit`) stays
for gdrive-style targets; `canvas` targets stream directly (`putStream`).
Still open: the hub-side mirror to gdrive/NAS (08-31 §5) reusing the same
engine with a non-canvas remote.
Not needed: cluster mode, pub/sub, cron expressions, priorities
