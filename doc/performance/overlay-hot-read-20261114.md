# Hot Overlay reads: permission snapshots and directory fences

Base: `03c8cc55d61473533c00a8afe13b4ef1fcd9b725`. The experiment uses an
immutable Packed lower and a writable Workspace upper, Redis, and RustFS on
the existing A355 host. No new cloud resources or SQLite tests are used in
the November 14 iteration.

## Changes

- Permission-cache hits retain the existing immutable `PermissionSnapshot`
  instead of copying its vectors and records. The existing shared byte budget,
  lease authority, TTL, mutation invalidation and generation tickets remain.
  Values retained by an operation remain charged to the cache budget.
- Read-only file open/close polls its owned operation once before spawning a
  task. A ready operation avoids the task handoff. A pending operation moves
  the same future, request scope and mutation owner to a task before yielding
  to its caller. Cancellation cannot abandon an admitted operation; panic
  poisons the gate. Write opens retain the original driver path.
- Directory preparation obtains workspace, head, lease, base and server time
  in one consistent KV snapshot. Previously it loaded the head and then
  read it again while validating authority and both layers. The result is
  returned only after authority and base equality checks. Fixed-layer and
  final namespace validation remain at the caller. Other stores retain the
  original two-step default implementation.
- Standalone Packed can opt into the existing payload-retention cache with
  `BREWFS_PACKED_RETAINED_PAYLOAD_BYTES`. This is present in both comparison
  builds to match their data-cache budgets; it is not a candidate-only speedup.

The default upper row-cache budget remains 64 MiB. No consistency or lease
settings have been relaxed. The existing merged metadata writeback and its
acknowledgement handling are unchanged.

## Existing mechanisms checked

`packed_lower.rs` already caches empty extent results by workspace, lease,
head/base versions, inode, chunk and exact range. `packed_enumeration.rs`
already caches empty upper directory results and validates its namespace
before publishing a directory handle. Both use the existing bounded row
cache. This change does not introduce a second negative cache.

A new regression runs the KV authority renewal path and verifies that it
does not change the file-layer record, replace a cached permission snapshot,
or trigger another empty-directory query. Thus the heartbeat-related cache
invalidation suspected in the JuiceFS reference was not reproduced in this
BrewFS path. This is a controlled KV regression, not a claim that heartbeats
generate no Redis traffic.

## Validation and reproduction

Run `bash tools/perf/check_overlay_hot.sh /absolute/new-results` with the
repository toolchain. It selects KV and database-independent correctness
tests, performs default and measured-feature builds/checks/strict Clippy,
checks both FUSE runtimes, and runs the shell report checks. It deliberately
does not execute the all-backend workspace test suite because it includes
SQLite fixtures. A successful selective check is not all-backend CI approval.

The experiment repository contains the build, matched comparison, concurrent
mutation, pagination, Redis audit and mixed-I/O scripts:

- [November 13 decomposition and comparison](https://github.com/Ivanbeethoven/ax-multi-agent-experiments/tree/codex/a355-overlay-tax-evidence-20261113/experiments/overlay-open-a355-20261113)
- [November 14 small and 10,000-file comparisons](https://github.com/Ivanbeethoven/ax-multi-agent-experiments/tree/codex/a355-overlay-tax-evidence-20261113/experiments/overlay-scale-a355-20261114)

Both builds use optimized release, identical fixtures and actual kernel
attribute/entry TTL=0. The earlier direct-Packed comparison accidentally
returned a 1-second TTL despite a zero JSON setting; correcting that
comparison is separate from optimizing the code.

Results and known failures are recorded in the experiment repository with
per-call measurements, command lines, source patches and binary hashes.
The hot-read comparison does not certify unrelated POSIX or mixed-I/O paths.

## Measured result and acceptance status

The November 14 candidate completed 54 matched read cells. Median batch wall
times (baseline Overlay -> candidate Overlay -> direct lower), in milliseconds:

| Input / upper row cache | Baseline | Candidate | Direct lower | Candidate / baseline - 1 | Candidate / lower - 1 |
|---|---:|---:|---:|---:|---:|
| 2 files / 64 MiB / 257 rounds | 989.184 | 963.925 | 646.872 | -2.55% | +49.01% |
| 32 files / 64 MiB / 257 rounds | 9332.770 | 9154.190 | 7475.006 | -1.91% | +22.46% |
| 10,000 files / 16 MiB / 3 rounds | 62686.925 | 61530.110 | 26387.810 | -1.85% | +133.18% |
| 10,000 files / 64 MiB / 3 rounds | 42122.125 | 40281.624 | 26631.886 | -4.37% | +51.25% |

These are diagnostic measurements, not an accepted performance claim. The
10,000-file/64-MiB paired changes were -4.37%, +4.29%, -18.34%. Every Overlay
cell in that group made 10,000 extent queries in total; the third candidate
distributed those queries differently across rounds. Its warmup exceeded the
unchanged 60-second row TTL. This variation cannot be presented as a stable
gain or attributed to fewer total queries. The 16-MiB group was capacity
limited and made 30,000 extent queries in both versions.

The small-input independent single-pass Redis audit changed from 10 client
commands plus 6 Lua commands to 8 plus 0 (two directory preparations per
pass). It ran after timing and counter snapshots. This does not count
network round trips. Both versions already made zero upper extent/directory
queries in the timed small-input workload through existing negative caches.
All 54 timed read cells made zero object requests and read zero object bytes.

Selected KV/cache/writeback/driver tests and six real mounted upper-mutation,
concurrent chmod/read, fsync and fresh-remount checks passed. **Full acceptance
is blocked**: concurrent rename-replace/open returned ENOENT on both baseline
and candidate; prepared-file fio randrw returned EIO on both, with separate
debug logs reporting `workspace is busy`. The conflict source remains
unresolved, and there is no valid mixed-I/O throughput comparison. No errors
were ignored or fence checks relaxed. Keep this branch as a review candidate;
do not merge or label the broad performance gate passed.
