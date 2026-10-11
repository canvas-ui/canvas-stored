# StoreD

## Roadmap

The current storage-policy gaps below include Stored 1.11.0,
canvas-server 2.21.0 and canvas-web 2.20.0 (2026-10-12). They supersede the
historical delivery notes further down. Existing unrelated backlog is retained.

## Storage policies — remaining gaps and deployment limits

The implemented boundary is: Stored owns physical bytes, exact replica
ownership, staging, the durable journal, verification, retries and recoverable
removal. Workspace owns policy configuration, document identity and virtual-tree
projection. Future work must preserve that boundary; the items mentioning
Workspace, FUSE or Web require changes or validation in those repos.

See [the physical replication contract](src/sync/REPLICAS.md) and
[the workspace policy contract](../canvas-server/src/core/workspace/services/storage-policies/README.md).

### Current supported behavior

- The pilot mutation path is the Canvas API, including canvas-fuse and Canvas
  UI/API clients. For predictable mirrored renames, moves and deletes, use those
  paths rather than modifying the server's backing directories through Samba
  or SSH. Using canvas-fuse is sufficient to express intent, but does not remove
  the unsupported-operation limits below.
- Recoverable mirror policies currently require writable **file backends with
  retention enabled**. Targets are asynchronous: a partial failure can leave
  some destinations updated and others pending. Staging remains until all
  required exact destinations verify; this is not an atomic transaction across
  backends. Conflicts stop the affected operation rather than undoing another
  writer's changes.
- **Google Drive is not a write-only driver.** It can read, enumerate/index
  binary files and poll changes. Automated `kind: "filing"` policies can create
  copies there, with `onConflict: "error"` or `"rename"`. Existing filing
  copies survive unfiling. Changed content at the same key errors or creates a
  new name according to that setting; it is not updated as a mirror.
- Google Drive **mirror policies, automated overwrite/removal and proactive
  empty-folder mirroring are rejected**. Uploads can create their needed parent
  folders. Existing low-level/manual Drive operations are separate from the
  new replica executor's recovery guarantees. Reading/indexing Drive changes
  does not enable bidirectional policy mirroring.
- Mirrored file edits and renames preserve the document ID. Legacy successor
  behavior for unmanaged files is unchanged. Notes-as-files and layer metadata
  remain explicitly deferred.
- **S3-compatible backends now support reads, indexing, polling and create-only
  filing policies.** Uploads use conditional creation, with an endpoint probe
  before user writes. S3 mirror policies, overwrite, folder mutation and automated
  retirement remain rejected. The same exact-target verification/release barrier
  protects staged bytes. Real driver and workspace scenarios passed against
  RustFS; this is not certification of every S3-compatible implementation.

### Pilot qualification and recovery work

- [ ] **Exercise actual canvas-fuse mounts against the updated server.** Cover
  editor atomic-save patterns, open handles, folder and file moves, deletion,
  offline/reconnect, lost responses, server restarts and conflicts. Workspace
  API scenario tests passed; an actual kernel FUSE mount was not exercised in
  this change. Verify unsupported operations surface useful errors to clients.
- [ ] **Complete deployment and durability qualification.** Build/run the
  production image (including `util-linux`/`flock`), test with representative
  ZFS datasets, quotas/disk-full conditions and interrupted writes. Abrupt
  process-exit tests cover placement, release and retirement; they do not prove
  power-loss durability or SMB/NFS mount behavior. Rehearse restoration of both
  journal/index state and bytes from snapshots/backups.
- [ ] **Browser-verify the policy editor and recovery UI.** Build/typecheck
  passed; browser workflows, stale edits, capability restrictions, failed
  transfers and retries were not exercised interactively.
- [ ] **Provide explicit resolution for blocked state.** Retry reuses the
  accepted immutable plan. New content can supersede old work, but a policy
  change alone does not rewrite unfinished work. Add a safe, reviewable
  cancel/replan or recovery workflow that accounts for partial ownership and
  pinned bytes; never discard a journal row or pin to make an error disappear.
- [ ] **Handle drift and observation gaps through deliberate repair.** Audits
  detect drift but do not blindly restore/delete changed replicas. An unpaired
  disappearance, expired change-feed cursor, missing document row, conflicting
  byte edit or ambiguous mapping blocks automatic propagation. Provide a
  rescan/rebaseline workflow with explicit preservation of independent changes.
  Currently a planning/observation error can block the whole workspace pass,
  even though the physical executor isolates blocked owners. Isolate unrelated
  planning failures only where a complete, safe plan can still be established.
- [ ] **Resolve content-identity collisions without losing curation.** If an
  edited mirrored document's new checksum already belongs to another document,
  projection blocks rather than merging IDs. Define and test an explicit
  resolution preserving both documents' relations and placements.

### Namespace operations and Samba

- [ ] **Infer raw filesystem folder moves before permitting unrestricted Samba
  writes.** Directory rename/move outside the API is not represented as one
  complete intent today. Preserve empty descendants, nested document identities
  and exact ownership; survive watcher reordering, missed events and restart.
  A confidently paired same-inode file rename can already propagate, but that
  is not a guarantee for arbitrary Samba/SSH moves, cross-filesystem moves or
  copy-then-delete sequences. Unpaired removal must never cascade by guesswork.
- [ ] **Define supported root and flat-mapping moves.** API folder moves within
  an unambiguous recursive mirror are supported. Renaming the mirror root,
  crossing that root or reconstructing a move from a flat target is refused or
  blocks projection. Policy paths are literal paths, not stable folder IDs;
  implement an explicit coordinated policy/placement migration before exposing
  root moves as transparent operations.
- [ ] **Qualify network filesystem semantics separately.** Exporting local
  workspace files over Samba allows writes outside Canvas's operation queue.
  Consuming an OS-mounted CIFS/NFS share as a file backend is a different case:
  root liveness, remote change detection, inode stability, exclusive create,
  rename, fsync and recoverable quarantine need deployment-specific tests.
  `driver: "file"` capability admission alone is not that qualification.

### Stored process boundary and backend parity

- [ ] **Extract the worker into standalone syncd.** The journal and executor
  are Stored-owned but currently run in the owning process. Add an IPC adapter,
  lifecycle supervision, durable status delivery and restart tests. Keep a
  single mutation authority: do not start a second executor beside server
  writes against the same state. Virtual-tree semantics stay in Workspace.
- [ ] **Keep the locking support boundary explicit.** Linux uses an OS-held
  lock and requires `flock` on the local state volume. Non-Linux PID-lease
  fallback and distributed/network-state locking are not pilot-qualified.
  Cluster/multi-writer execution remains outside scope.
- [ ] **Bring Google Drive up to the replica recovery contract before enabling
  mirrors.** Add conditional updates against recorded remote versions,
  recoverable overwrite and retirement, durable remote-ID/path ownership,
  folder-operation receipts and replay-safe conflict handling. Verify concurrent
  external writers, duplicate names, expired credentials, partial uploads,
  lost acknowledgements, trash/restore and remote rename/move propagation.
  Existing driver/transport tests use fixtures or local mock endpoints; live
  multi-backend Drive policy scenarios were not validated in this change.
- [ ] **Qualify each S3 provider and deployment.** Run the opt-in live suite
  against Amazon S3, MinIO, Garage and Ceph RGW, plus the pilot's RustFS version.
  Test path/virtual-host addressing, TLS, scoped IAM, temporary credentials,
  versioned/locked buckets, lost responses, outages and concurrent external
  writers. The current compatibility probe refuses endpoints that ignore
  conditional creation; never replace it with HEAD followed by an unconditional
  write. Current live coverage is RustFS only.
- [ ] **Add S3 recovery before mirror capabilities.** Implement version-aware,
  conditional replacement and recoverable retirement, replay receipts and
  retention/restore semantics across the selected providers. Prefixes are not
  folders; define folder-intent projection separately. Keep mirrors and
  automated overwrite/removal disabled until these guarantees are tested.
- [ ] **Add resumable S3 multipart uploads.** Current streaming single PUTs are
  capped at 5 GiB. Persist multipart IDs/parts and recover uncertain completion;
  clean abandoned parts without touching active work. Include retry behavior
  after a committed write whose response was lost.
- [ ] **Bound S3 scan cost and probe debris.** Initial scans download content;
  poll scans enumerate and HEAD objects, and the checksum memo is process-local.
  Add safe durable observation caching and an explicit rescan strategy for
  missed changes. A crash during `.canvas-probe/` verification may leave a hidden
  random probe; provide scoped lifecycle cleanup (including versions). Never
  sweep user prefixes to clean these up. A probe cleanup permission failure must
  remain visible and prevent writable connection admission.
- [ ] **Extend S3 authentication deliberately.** Explicit access/secret keys
  and session tokens work today. Add scoped role/STS refresh without granting
  workspace-selected endpoints access to ambient server IAM credentials.
- [ ] **Keep native Google document export separate.** Google Docs/Sheets/Slides
  have no binary byte representation in the current driver and are skipped.
  Explicit export/import formats remain future work, as do DB-note projections.

### Space, history and throughput

- [ ] **Finish crash-spool cleanup.** Cache GC and completed replica/ingress-pin
  cleanup now exist, but abandoned plain upload spools and temporary publication
  files are separate. Implement the detailed staging-cleanup item below,
  including the cache `.tmp` fallback, configured temp directories and protection
  for active uploads, retained versions, Trash and journal-referenced files.
- [ ] **Compact journal metadata safely.** Completed receipts are retained for
  idempotency; operation/ownership history has no compaction policy. Bound
  metadata while preserving retries, pending references, replay cursors and
  recovery evidence. Blob GC does not solve receipt growth.
- [ ] **Add capacity accounting and admission controls.** Pending/blocked
  operations intentionally pin bytes and can fill a workspace during a long
  outage. Report pinned/staged/retained space, expose actionable low-space
  status, and apply quotas/backpressure without evicting the last verified or
  recovery copy. General cache size/LRU policy remains in the backlog below.
- [ ] **Measure and bound large-workspace reconciliation.** The current pass
  gathers all documents/plans; physical mutations serialize through one Stored
  executor. A slow backend transfer can delay unrelated work. Benchmark large
  trees and slow targets, then introduce bounded scans and safe concurrency
  without weakening endpoint ownership, ordering or the all-target barrier.
- [ ] **Budget audit IO.** Completed replicas are fully read on initial audit
  and at most once per 15 minutes per completed operation; placement and staging
  release always verify bytes. Make background audit scheduling/bandwidth
  configurable and measure remote read cost. Keep release verification strict.
- [ ] **Complete operational visibility at scale.** Workspace status exposes
  at most 1,000 active operations and the Web view does not paginate them.
  Add complete pagination, per-policy/backend summaries, queue age/byte counts
  and actionable failure notifications. The tree badge currently indicates
  policy membership, not a verified "in sync" state.

### Verification baseline

The S3/Git-separation increment passed 249 Stored tests (including the live
RustFS test), 347 workspace/hook tests, focused connector/configuration checks,
and the Web production build/typecheck. The workspace live S3 test covers add,
keyed upload, rescan, filing, cache release, unfiling and read-only admission;
the configuration test also verifies secret-vault storage and token clearing.
The S3 settings form was built and linted but not browser-verified. The fixture
used host port 32768; host port 9000 was reserved and left untouched.

The preceding storage-policy implementation passed 239 Stored tests and 370 workspace/indexing/hook
tests, plus the Web production build/typecheck. Coverage includes abrupt process
exit/replay, partial failures, exact-target completion, shared ownership,
independent edits, preserved document IDs, API folder operations, unknown
children and cache cleanup. This is a regression baseline, not proof of all
edge cases or approval for unrestricted Samba/remote-backend mirroring.

## Earlier deliveries and continuing backlog

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

The earlier storage action landed as the hook-rule action `store` (canvas-server):
declarative move/copy of a document's bytes between backends with
`{{YYYY}}/{{MM}}/{{ext}}`-style key templating (EXIF capture time first) and
`onConflict` handling — the "images always live in workspace:home/Upload/Images"
use case this roadmap predicted would be Workspace-hook material, plus a
`store` row in the web rule builder. stored gained `onConflict`
(`error`|`rename`|`overwrite`, per-target keys) and destination-key
normalization to support it.

The first-class storage policies in 1.10.0 no longer compile into independent
hook copy/delete actions. Workspace resolves the full target set and submits
it to Stored's replica executor; see the current contract and gaps above.

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

The original delivery covered individual transfers. The MVP scope now also
includes policy-driven replication across supported backends. MIME-routing
decisions ("images → `workspace:home/Upload/Images`", "pdfs → `…/Papers`")
belong to Workspace; Stored receives physical destinations and owns their
execution without interpreting MIME rules or virtual paths.

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

## P1 — staging cleanup for crashed uploads (priority)

Found 2026-10-07 while pushing a 1.1 GB video through canvas-fuse. An
object `PUT` (`Stored.writeObject`, `src/index.js`) spools the body
into a plain temp file, `<backend root>/.stored-tmp/<ts>-<uuid>` (not
cacache), hashes it in one pass, then publishes it through the backend.
The new replica workflow also pins immutable bytes in cacache; those pins
have their own lifecycle and do not replace plain spool cleanup.
Every handled failure path (client disconnect, sha256
mismatch, `If-Match` failure, disk/driver error) removes the temp in a
`finally`, so a failed upload leaves nothing behind. **A process crash,
kill or power loss mid-upload does**, and nothing ever sweeps
`.stored-tmp/`: `sweepRetained` handles retained versions and Trash, not plain
spools. `Cache.verify()` now runs periodically after cache-key removal and on
shutdown, but does not collect these separate temp files. Stale spools still
have no age/size bound.

- [ ] **Sweep `.stored-tmp/` on backend open and on the `sweepRetained`
      timer.** Delete only recognized abandoned spool files older than a grace
      window (default 24 h, `config.tempMaxAgeMs`). Mtime alone cannot prove
      abandonment: protect slow/paused active uploads and journal-referenced
      files, retained versions and Trash. Include the driver's temporary
      publication siblings and the cache `.tmp` fallback. Log a count, emit
      nothing — this is housekeeping, not an object event.
- [ ] Honour `backend.tempDir` overrides (the sweep must look where the
      spool actually goes) and skip the dir entirely when it is not ours
      (not under the backend root and not configured).
- [ ] Check preconditions **before** spooling where possible: `If-Match` /
      `If-None-Match: *` can be evaluated against the current stat up front
      and reject obvious conflicts early, then recheck against actual bytes
      at publication. The final check must remain to catch concurrent edits.
      Today a doomed 1 GB PUT writes all of it to disk before the 412.

### Optional — resumable object uploads

`?sha256=<ref>` on the objects route only dedupes against bytes already
indexed in `workspace:data` (`statBlobByChecksum`); a `workspace:home` PUT
never populates it and a failed one leaves nothing a retry can reuse — the
client resends everything. If large pushes over WAN keep failing midway:

- keep the spool on failure when the client announced `X-Canvas-Sha256`,
  keyed by that digest (`.stored-tmp/partial/<sha256>`), subject to the same
  sweep;
- accept `Content-Range: bytes <from>-` / `X-Canvas-Resume-From` on PUT and
  append to the matching partial, verifying the digest at the end (mirror of
  what canvas-fuse already does for downloads with `.canvas-part` + `Range`);
- `HEAD objects/<key>?sha256=` answers `X-Canvas-Have: <bytes>` so the client
  knows where to continue.

Only worth it once the cleanup above exists, otherwise partials are one
more thing that never gets collected.

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

## Other architecture backlog

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

#### Device sync primitives ✅ DONE (1.6.0 — `src/sync/JobQueue.js`, `Mirror.js`, `Ledger.js`)

This completion refers to the device mirror library, not a standalone syncd
process. The separate-process gap remains open above.

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
`prefixes`/`ignore`. As of 1.10.0, `SyncQueue` is a compatibility adapter over
the durable `ReplicaService` for remote copy transfers; `canvas` targets stream
directly (`putStream`). Hub-side file replication now uses that replica service.
Google Drive destructive mirror support and NAS deployment qualification remain
open as described above; they are not implied by the device mirror's completion.
Not needed: cluster mode, pub/sub, cron expressions, priorities
