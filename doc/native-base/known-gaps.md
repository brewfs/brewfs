# Native base: recorded KnownGaps (REGRESS-005)

REGRESS-005 asks for the buffered / direct / mmap shapes to be **recorded as
KnownGaps instead of being reported as PASS**.  This file is that record: the
native-base work never claims these shapes pass, and nothing in
`acceptance-matrix.json` marks them PASS.

## Scope of the record

The rows below are the FUSE-facing access shapes that are *known not to be
proven* by the current acceptance work.  The native-base matrix covers the
component-level read/write/commit path; a mount-level PASS for any of these
shapes would need the xfstests/LTP harnesses named in the row, not a focused
unit test.

| Shape | Manifestation | Why it is not a PASS | Recorded at |
|---|---|---|---|
| buffered (FUSE page cache) | After `truncate`/extend, buffered `mmap` can expose stale pre-truncate page-cache data. `generic/075` is excluded from the default xfstests set for this reason. | The mmap shape cannot be run under direct I/O (`ENODEV`), and the writeback cache still reproduces it. Re-enable only after repeated `generic/075 generic/014` passes with no D-state task and <5% regression. | `AGENTS.md` (Known POSIX And FUSE Limitations), `doc/testing/xfstests-redis-rustfs-fix-plan.md` |
| direct (O_DIRECT) | The tiny-overlap direct-I/O diagnostic itself passes, but the normal buffered FUSE profile still has a split-write/page-cache coherency race; full direct I/O is not a substitute because the mmap shape returns `ENODEV`. `iogen01` therefore stays in the default LTP skip list. | The DIAGNOSTIC is not the workload: promoting it to a PASS would claim a shape that the buffered profile does not deliver. | `AGENTS.md` (Known POSIX And FUSE Limitations) |
| mmap | Post-reply `FUSE_NOTIFY_INVAL_INODE` ordering experiments can leave `fsx` permanently blocked in `request_wait_answer`, and a stuck FUSE process cannot be killed until the kernel request returns. | Adding invalidation ordering without checking teardown and D-state tasks is a hazard, not a fix; the experiments are recorded as rejected. | `AGENTS.md` (Known POSIX And FUSE Limitations, Artifact Hygiene) |

## What would close them

- A repeated, clean mount-level run of `generic/075 generic/014` with no
  D-state task and a bounded performance delta, run through
  `docker/compose-xfstests/`, plus the LTP `iogen01` un-skip in the same
  harness.
- A teardown check that proves no new hung task or background buffer task
  (REGRESS-006) before any invalidation-ordering change is considered.

Until then these rows stay KnownGaps: they are recorded here, they are not
listed as PASS anywhere, and no native-base evidence log claims them.
