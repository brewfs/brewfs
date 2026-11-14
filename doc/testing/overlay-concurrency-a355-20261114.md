# Workspace Overlay concurrency fixes on A355

Base: `97db0eb1f6b4575774284c089d5fd7831faf5c6e`, on top of main
`03c8cc55d61473533c00a8afe13b4ef1fcd9b725`. Existing A355 only; no paid
resources and no SQLite tests. The earlier failed experiments remain intact.

## Atomic replacement between LOOKUP and OPEN

The captured sequence was LOOKUP(target -> inode 12), rename(next inode 13
over target), OPEN(inode 12) -> ENOENT. The pathname was never absent; its
previous inode had become obsolete before OPEN registered a live handle.

On Linux the FUSE OPEN adapter now returns ESTALE for that missing nodeid.
Linux retries pathname resolution with LOOKUP_REVAL, which makes FUSE issue a
fresh lookup and invalidate an entry whose nodeid changed. This follows
[namei.c](https://github.com/torvalds/linux/blob/v6.8/fs/namei.c) and
[FUSE dir.c](https://github.com/torvalds/linux/blob/v6.8/fs/fuse/dir.c).

The metadata layer still rejects opening a deleted inode. It does not recover
old attributes from cache or add an unbounded tombstone/open-reference cache.
Genuine missing names still return ENOENT from LOOKUP; permission and lease
checks run again on the resolved inode. The deterministic regression verifies
the obsolete inode is not reopened and a new inode with mode 000 rejects an
unprivileged OPEN.

## Read during local metadata publication

`canonical_dirty_snapshot` previously returned generic Workspace Busy whenever
a noncommitted slice had `meta_write_started`. A successful metadata transaction
can be in progress at that instant. The read request surfaced Busy as EIO.

An actively publishing slice now returns a private, typed pending-publication
error. The read drops its incomplete snapshot and request fence, waits for the
slice notification, and obtains a fresh fence and canonical snapshot. Writer
locks are released before waiting. A 20 ms recheck handles completion just
before notification registration and claim release without notification. A
10 second deadline bounds this wait and starts only on the first pending
publication; normal hot reads allocate no waiter or timer.

Failed slices and slices carrying a recorded error do not enter that retry.
The existing three-attempt limit for actual read-view changes remains separate.
Admission, corruption, lease and backend errors are not reclassified as a
local publication. Each successful read still checks its final fence, waits
for committed uploads when needed and cannot extend EOF from committed residue.
No writeback acknowledgment or old-writer rejection logic changed.

## Reproduction and evidence

`tools/perf/check_overlay_hot.sh /absolute/new-check-output` runs the existing
KV, cache, permission, lease/renewal and merged-writeback checks, plus the new
writer snapshot tests. The snapshot test fixture now uses the real KV Workspace
type instead of constructing SQLite. Broad compilation, Clippy and shell
checks are included. This selected test gate does not claim the complete
all-backend workspace suite, which includes SQLite.
The imported Windows shell files initially failed `bash -n` because of CRLF.
The shell checks passed in a separate LF-normalized copy; both the original
failure and the normalization receipt are archived. Rust source was unchanged.

Real FUSE tests, matching release builds, source/binary hashes and raw timing
data are kept in the experiment repository under
`experiments/overlay-fixes-a355-20261114/`. That directory includes repeatable
replacement, verified random read/write, fio and paired hot-read runners.
The old failed fio run cannot serve as a throughput denominator.

The optimized candidate passed six replacement cells (600 replacements,
9,600 concurrent reads) and six verified mixed-I/O cells (59,129 reads,
25,262 writes), including final fsync and remount. Replacement logs contain
23 stale-node OPEN responses successfully revalidated by Linux; mixed-I/O
logs contain 11,263 local-publication waits. These are application-success
checks, not merely the absence of a race in the input. All six separate
16 MiB prepared-file fio randrw cells also completed without I/O errors.

The retained mixed-I/O correctness file is 64 KiB, fitting the existing
harness's one-FUSE-READ-per-file assertion. An earlier 16 MiB debug run
verified its data but failed that assertion because its full-file reads
split into multiple callbacks; it is preserved as an incomplete run. Fio
retains the separate 16 MiB working set. Neither debug timings nor the
incomplete run are used for the release performance comparison.

All 27 matched hot-read cells passed with zero timed object requests/bytes.
With the same 64 MiB row cache, median batch times before/after were
0.933320/0.941798 s (2 files, 257 rounds), 9.050151/9.027474 s (32 files,
257 rounds), and 42.194417/41.753027 s (10,000 files, 3 rounds). Changes were
+0.91%, -0.25%, -1.05%; this is a correctness fix without a claimed stable
hot-read speedup. Both large-file-count arms executed 10,000 upper extent
queries and 50,641 Redis commands per cell. The remaining Overlay/lower
overhead and final writeback drain are not eliminated by these changes.
