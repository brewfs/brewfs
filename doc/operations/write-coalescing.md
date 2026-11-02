# Flat-v1 small-write object coalescing

This is an experimental format extension for flat-v1 volumes using Redis or
SQL metadata and upload-before-commit. Its final release performance and broad
regression acceptance remain incomplete; see the
[experiment report](../performance/write-coalesce-2026-11-02.md).

The current code **enables new coalesced writes by default**:

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `BREWFS_WRITE_COALESCE_BYTES` | `65536` | Target uncompressed bytes per shared object; `0` disables new groups. Maximum accepted value: 1 MiB. |
| `BREWFS_WRITE_COALESCE_MS` | `0` | Use a 100 ms queue deadline; a positive value replaces it, up to 5000 ms. |

Unset, empty or unparsable values fall back to these defaults. Use the numeric
value `0` for `BREWFS_WRITE_COALESCE_BYTES` to disable new groups.

The 64 KiB target aims to combine sixteen 4 KiB slices per PUT. It is a candidate
choice supported by the earlier v1 experiment, not a completed final-release
acceptance or an optimum established across workloads. Small tail groups can
be smaller. Each group has at most 32 members, each at most 64 KiB and one
configured block. Large sequential slices keep the ordinary uploader.

## Upgrade and rollback

New readers understand old `chunks` and `chunks-v2` objects. Old binaries do
not understand `chunks-coalesced-v1`. The volume still identifies as flat-v1;
there is no new format-version negotiation that fences an old mount out.

Before a rolling upgrade, explicitly set `BREWFS_WRITE_COALESCE_BYTES=0` in
every new mount's environment. Upgrade **all** clients that can read or write
the volume, including background maintenance processes. Keep new grouping
disabled until the required acceptance checks and rollout decision are complete.
Then explicitly use `BREWFS_WRITE_COALESCE_BYTES=65536` when starting mounts
that should create shared objects. For an offline upgrade, stop all old mounts
before starting any mount that can create shared objects.

Setting the value back to `0` stops new grouping and keeps existing groups
readable with the new binary. It does **not** convert existing data back to the
old object format or make downgrading safe. Once any shared object is published,
continue using compatible readers. An old-binary rollback requires restoring
a pre-upgrade volume backup or exporting through a compatible reader and
importing into a separate volume without coalescing. Do not attach an old mount
to a volume containing shared objects.

## Durability, visibility and bounds

The queue holds in-memory frozen slices; it is not a durable staging journal.
Successful fsync/flush waits for the object PUT and metadata publication.
Normal FUSE close uses FLUSH, and the writable-handle close path also waits for
dirty data. A stateless RELEASE with `flush=false` is not itself a new durability
barrier. Backend durability remains dependent on the object and metadata
servers; mount-process SIGKILL tests do not establish server power-loss safety.

The queue deadline starts an upload task. It does not discard data, bypass
metadata publication or bound the total duration of backend IO. Explicit
flushes wait for completion and propagate failure. The 8 MiB semaphore bounds
additional encoding and assembly admission; exhaustion waits for permits,
without falling back to ordinary writes. Existing writer buffers and caches
have their separate budgets. The 5000 ms queue setting can consume the entire
five-second VFS close budget before upload time, so it is unsuitable for callers
that require that close deadline.

Processes on the same mount use the inode's shared writer overlay for pending
reads. A different mount cannot read another mount's uncommitted memory.
After publication, visibility still depends on existing metadata version/cache
validation and kernel caching; this extension adds no distributed invalidation
protocol. Do not infer immediate cross-mount read-after-write coherence from
the single-mount immediate-read tests.

Shared slice IDs occupy `[2^52, 2^53)`. Redis and SQL ordinary allocators return
an internal error if they reach this range. The writer records a sticky
writeback failure and flush/fsync reports **EIO**, not ENOSPC or success. A
buffered write may have returned before that asynchronous failure; applications
must check flush/fsync/close errors. Seeds at or above `2^47` bypass grouping
while ordinary IDs remain available. Other metadata backends and native/packed
publication paths do not opt into shared-object creation.
