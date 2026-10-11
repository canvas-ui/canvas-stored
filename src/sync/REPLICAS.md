# Physical replica executor

`stored.replicas` owns the durable physical workflow. Callers supply a complete,
ordered destination set for an opaque owner; no workspace or virtual-tree types
are imported here. The current worker is in process. On Linux, an OS-held
exclusive lock refuses a second executor and releases on process death, including
container restarts that reuse PIDs. `util-linux` (`flock`) is required; the server
image installs it. The lock file must stay on the workspace's local state volume.
Other platforms use a conservative PID lease and are outside this pilot contract.

```js
stored.replicas.submit({
  id: 'unique-request-id',
  owner: 'opaque-logical-file-id',
  contentId: 'sha256:<64 lowercase hex digits>',
  source: { backend: 'data', key: 'existing-staging-key' },
  replicas: [
    { backend: 'home', key: 'Accounting/2026/invoice.pdf' },
    { backend: 'backup', key: 'Accounting/2026/invoice.pdf' },
  ],
  release: [{ backend: 'data', key: 'existing-staging-key' }],
});
```

Identical request IDs return the original receipt. Reusing an ID with different
input fails. A plan is immutable. `supersede(id, nextPlan)` accepts a newer byte
version of the same owner, preserving verified partial ownership and keeping old
pins until the successor completes. It never resolves filename conflicts by
guessing. Each outcome persists its selected name and
expected byte version before IO. Placement is create-only by default; managed
mirror updates can replace their recorded old version. Explicit overwrite is
accepted only with recovery support. A changed destination blocks the operation.

All mutations share one reentrant executor with the public Stored API. Unfinished
operations serialize by owner; one blocked owner does not stop unrelated work.
Transient failures retry with backoff, permanent conflicts wait for `retry(id)`.
`list({ active: true })` lists pending/blocked operations without completed history
hiding current work. `owner(id)` exposes verified exact-key ownership and content
versions. `audit(id)` verifies actual physical bytes rather than index presence.

The phase order is immutable staging pin, place and verify all destinations,
retire obsolete owned copies, verify all destinations again, release blob
locations, commit ownership and completion, release operation pins. Crash replay
uses the same staged content, names and preconditions. Native file quarantine and
retained versions use independent inodes; mutable destinations never hardlink
immutable cache pins. Deletes/overwrites capture the actual old inode so an
external writer holding it open does not lose its bytes.

Only file backends currently implement automatic recoverable retirement and
overwrite. Remote drivers must supply equivalent conditional-write and recovery
semantics before those capabilities are enabled. Copy-only remote creates use
the same executor through the SyncQueue compatibility adapter. Unsupported
operations fail before destructive execution.

Cache verification reclaims unreferenced content after key removal, serialized
against cache streams. Outstanding operation pins remain until completion.
Managed API edits pin local ingress before publishing the writable file. With
`replication.captureAllWrites`, new API files are pinned too; the producer must
either resolve their complete replica plan or call `finishUnmirroredIngress`
after deciding that the requested primary is their only destination. Ingress
and operation pins are separate, so rapid edits cannot mutate accepted bytes.
Completed receipts are retained for idempotency; metadata history compaction is
not implemented. A backend file changed by an independent process is observed,
never silently interpreted as permission to delete copies elsewhere.
