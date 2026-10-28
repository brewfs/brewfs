# Overlay continuation log

## Iteration 0 — design and acceptance gate (2026-10-28)

### Design

The target is the overlay read path, with the first hypothesis aimed at the
metadata work done for every 4 KiB request.  The current native resolver keys a
read plan by the exact byte range.  Random 4 KiB reads therefore almost never
reuse a plan, even when they address the same inode and chunk.  The first
architecture change is a bounded, immutable extent snapshot cache keyed by
`(workspace, head epoch, inode, chunk, inode data version)`.  A snapshot covers
one complete chunk; each request is clipped in memory.  A successful bounded
backend query populates it, and inode/view invalidation removes it.  If a
backend does not implement the bounded query capability, the old exact-range
path remains the compatibility path and no unbounded snapshot is retained.

The existing per-key upper-row cache change remains in place.  It reduces
unrelated refill loss, but it does not weaken the cancellation, uncertain
commit, lease, or ACL fences.

### Bottleneck evidence

The supplied C3 measurement is 67.4 MiB/s for 4 KiB random reads with a 160 ms
p99.  Code inspection identifies three repeated costs in the native path:

* `src/workspace_overlay/meta_layer/mod.rs:4107-4185` keyed the resolver cache
  by exact `range_start/range_end`.
* `src/workspace_overlay/stores/kv_store/extent_reads.rs:41-141` scanned both
  layer prefixes and read extent generations around every query.
* `src/workspace_overlay/stores/database.rs:2679-2720` issued a range query for
  every request as well.

This iteration does not claim that object-store or FUSE time has been
eliminated.  The handler is only 8–12% of the reported request time, so the
cache is deliberately instrumented and bounded so a later compose run can
separate metadata savings from data and teardown costs.

### Performance判据

Use the same host and workload for at least three runs and report
min/median/max.  Acceptance requires both 4 KiB random-read throughput of at
least 134.8 MiB/s and p99 of at most 80 ms, or (as the alternate gate) 100k-file
`stat` wall time of at most 600 µs.  A result that moves work into close,
flush, or drain is rejected.  The supplied C3/C4 numbers are the baseline;
they are not re-run here unless the compose environment is available.

### Generation answer

`generation` names several independent fences in this repository.

* **Upper-row cache global generation** is bumped by
  `UpperRowCache::invalidate_all` at `src/workspace_overlay/meta_layer/row_cache.rs:234-238`.
  Its call sites are `mod.rs:316-322` (explicit freshness invalidation),
  `mod.rs:334-335` and `mod.rs:352-354` (unknown or ACL-shaped writes),
  `mod.rs:378-381` (lease authority lost), and `mod.rs:708-714`
  (view replacement).  The trigger is a view change, an affected-row set that
  cannot be proven complete, or loss of local lease authority.  Without this
  bump, an in-flight refill could insert rows read under the previous view or
  after the lease ceased to be authoritative; a clear alone has a race.  This
  bump remains **necessary** for global invalidation.  Exact row writes use the
  per-key generation introduced in this work, so unrelated rows no longer pay
  the global cost.
* **Upper-row cache per-key generation** is bumped by
  `row_cache.rs:241-248` for every key in `invalidate_keys`.  Without it, a
  read racing a confirmed or conditionally rejected mutation could reinsert a
  pre-commit inode, dentry, xattr, permission, path, or alias row after the
  immediate eviction.  This is **necessary** for precise invalidation.  The
  cache is still permanently disabled by `CacheCommitGuard` after cancellation
  or an uncertain backend result (`row_cache.rs:119-145`); that red line is not
  replaced by a version check.
* **Namespace cookie epoch** is bumped by
  `src/workspace_overlay/meta_layer/namespace_epoch.rs:39-40`, from
  `NamespaceWriteGuard::drop` at `:80-86`, after a committed namespace write
  or an uncertain/cancelled write.  A definite rejected CAS (`unchanged`) does
  not bump.  Without the committed bump, a readdir cookie or path snapshot
  could be reused across a namespace change; without the uncertain bump, a
  backend write that outlives cancellation could be mistaken for a no-op.  It
  is therefore **partly necessary**: rejected CAS may retain the epoch, while
  committed and uncertain writes must advance it and uncertain writes must
  permanently disable the local proof.
* **Read-slice generation** is bumped at
  `src/vfs/io/reader.rs:1278` when an affected slice is invalidated.  It fences
  a concurrent block fetch from publishing bytes for the old slice state; no
  bump would permit stale page-cache data after a write/truncate.  This is
  **necessary** and independent of metadata row generations.
* **Inode data epoch** is bumped by `src/vfs/inode.rs:176-178`, called from
  `src/vfs/fs/mod.rs:4214,4451,4822,4880,7052` and
  `src/vfs/io/writer.rs:6381` for truncate/overwrite/unlink data transitions.
  It rejects stale writeback/read reservations after a data shape change.
  Without it, a pre-truncate or pre-overwrite request could commit against the
  new inode state.  This is **necessary**.  The new extent snapshot also keys
  on the persisted inode data version, so it does not substitute for this
  in-memory epoch.

The conclusion is **B: partially necessary** only in the narrow sense that a
definite rejected namespace CAS need not bump.  Global row-cache bumps remain
required for unprovable/global invalidations; exact writes use per-key bumps;
uncertain writes keep the disabled flag.  In particular,
`permission_cache_ttl()` at `src/fuse/mod.rs:3885-3897` still returns
`Duration::ZERO` whenever writable POSIX ACLs are enabled, and only
`fenced_metadata_read` can attach a positive TTL.  Cancellation and uncertain
commit paths continue to disable local cache authority permanently.

### Packed-v3 object-side answer

The code establishes the layout but the current workspace has no mountable C4
volume or retained object-server/ESSD read log.  Ordinary content objects use
`{prefix}/objects/{kind as u8}/{sha256}` (`wire005/producer.rs:450-458`);
authenticated index pages use `{prefix}/indexes/{kind as u8}/{sha256}`
(`wire005/index_builder.rs:153-159`).  PM11 stores seven independent roots:
Groups, Inodes, Containers, Frames, ColdAttributes, ReverseNames, and
LargePlacements.  Group metadata and frame payloads are in GroupContainer
objects; external placement uses an inode+big-endian file-offset selector key
(`wire005/placement.rs:135-137`) and LargeData objects.

An exact per-`stat` object count/byte count and per-4 KiB `read` object
count/byte count are **未测**.  The missing conditions are a mountable packed
fixture, its object endpoint, and server-side request or block-device
`read_bytes` logs.  The historical 100k cold-read FUSE aggregate cannot be
used to infer a single-request amplification.  Likewise, the historical
3,319-container/57,552,185,440-byte fixture is not a substitute for a
request-level trace.  Those measurements will be added only when the packed
volume and logs are available.

### Risk and rollback

The snapshot is bounded by the existing `MAX_UPPER_EXTENT_ROWS` backend limit
and a weighted Moka capacity.  A chunk with too many rows, an unsupported
bounded backend, or any cache miss follows the existing exact-range resolver.
The change can be rolled back by removing the snapshot lookup/fill while
leaving the cache invalidation tests and fences intact.

## Iteration 1 — extent snapshot cache

Status: implementation in progress.  The result section will contain the
required three-run compose measurements and correctness gate output; if the
environment cannot run them, the missing condition and an explicit “未达判据”
result will be recorded.
