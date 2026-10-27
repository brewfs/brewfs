# Read-path follow-up after #350

PR #350 removes the reopen invalidation that #363 identified: a buffered
read-only reopen can retain the kernel data pages when the FUSE OPEN reply has
`FOPEN_KEEP_CACHE` (`0x2`). The follow-up below is required before treating the
remaining sequential-read gap as solved.

## Remaining performance question

The task's overlay-arm measurements put `fio-seqread` / `fio-bigread` at
`0.48–0.64x` of native and `0.187–0.450x` of JuiceFS. Those ranges are context
from the earlier run; post-#350 values are **not-measured** here. KEEP_CACHE can
remove READs caused only by close/open, but it cannot explain a single-open
read path that still spends time in FUSE dispatch, range planning, backend
fetch, decode, or copy. The current committed benchmark evidence therefore
does not claim a post-#350 speedup for either fio workload.

## Write-path safety

The opt-in applies to ordinary VFS OPEN/CREATE replies and intentionally leaves
the entity adapter on its direct-I/O path. With the default (unset)
`BREWFS_FUSE_WRITEBACK`, the mount uses `write_back(false)`, so a write request
goes through BrewFS's ordinary write path rather than kernel writeback caching.
The `BREWFS_FUSE_WRITEBACK=1` override is a separate mode and is
**not-measured** here. The VFS invalidates its userspace read cache after
writes; truncate and related size-changing operations return zero attribute
TTLs and retain their existing invalidation behavior. These code paths mean a
same-mount write is expected to reach BrewFS and invalidate dependent pages,
but this branch has no kernel-level write/reopen measurement proving that
sequence.

KEEP_CACHE weakens the unconditional invalidation that used to happen at every
reopen. An external mount or API that changes bytes while preserving size and
mtime can leave pages stale; `FUSE_AUTO_INVAL_DATA` only helps when revalidated
attributes expose a change. This is why the option remains opt-in and why the
write-path claim is limited to same-mount operations. The existing probe covers
same-mount overwrite, append, truncate, mmap, rename, and permission cases,
but a mounted run is **not-measured** in this checkout.

The metadata cache TTL is independent of KEEP_CACHE. FUSE replies carry one
`attr_valid`/attribute TTL for the whole reply, so there is no per-field TTL
split. #357's generation-checked fenced metadata path and its single
`permission_cache_ttl()` policy remain intact; #350 changes only
`ReplyOpen.flags` / `ReplyCreated.flags`.

## Next measurement and falsifier

On one frozen binary, run `fio-seqread` and `fio-bigread` with
`fuse.keep_cache=false` and `true`, each in cold, warm, and close/reopen phases.
Capture the OPEN flags and FUSE READ count with the probe (or a kprobe), bytes
read and checksum, file size/mtime before and after each operation, and page
residency. Repeat the sequence with a same-mount overwrite followed by a
reopen, then truncate and reopen. Record the native, overlay, and JuiceFS
throughputs using the same fio job files.

The plan is falsified if a `true` reopen still issues thousands of READs, if a
same-mount write or truncate returns stale bytes/checksums, or if reopen READs
fall to zero while both fio ratios remain in the `0.48–0.64x` native and
`0.187–0.450x` JuiceFS ranges. In the last case the named bottleneck is in the
single-open read path and the next change must target dispatch/fetch/copy
instrumentation rather than cache retention.
