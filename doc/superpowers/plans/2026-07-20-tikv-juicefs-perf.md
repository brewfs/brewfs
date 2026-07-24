# TiKV Metadata Performance Comparison, 2026-07-20

## Commands

```bash
PERF_LOG_TO_CONSOLE=false PERF_FIO_SIZE=512m PERF_FIO_RUNTIME=20 \
  bash docker/compose-xfstests/run_tikv_perf.sh --s3 \
  --tools "fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw fio-bigread fio-bigwrite metaperf dirstress dirperf looptest"

JUICEFS_META_BACKEND=tikv PERF_LOG_TO_CONSOLE=false PERF_FIO_SIZE=512m PERF_FIO_RUNTIME=20 \
  bash docker/compose-xfstests/run_juicefs_perf.sh \
  --tools "fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw fio-bigread fio-bigwrite metaperf dirstress dirperf looptest"
```

Both runs used TiKV v8.5.0, RustFS S3, a 512 MiB fio working set, 20 second fio
runtime, buffered io_uring, and the same tool matrix.

## Results

### BrewFS + TiKV

Artifact: `docker/compose-xfstests/artifacts/perf-run-1784556842-18340`.

- `fio-seqread`, `fio-seqwrite`, `fio-randread`, `fio-bigread`, and
  `fio-bigwrite` completed.
- `fio-randwrite` failed after 8 seconds with FUSE `Input/output error`.
- `fio-randrw` failed after 7 seconds with FUSE `Input/output error`.
- `metaperf` produced no output after more than five minutes and was interrupted;
  subsequent directory and loop tests did not run.

The partial fio JSON written by failed jobs is not a valid throughput result and
must not be used in a comparison table.

### JuiceFS + TiKV

Artifact: `docker/compose-xfstests/artifacts/juicefs-perf-run-1784557322-508`.

- All fio, metadata, directory, and loop tests completed successfully.
- `metaperf` completed in 272 seconds.
- Metadata throughput was: create 109 ops/s, open 2,192 ops/s, stat 605,816
  ops/s, readdir 8,086 ops/s, and rename 264 ops/s.

## Rejected Candidate

Expanded BrewFS TiKV retry classification for region, leader, and lock-resolution
errors was tested in this run. It did not prevent either random-write EIO or the
metadata stall, so the change was reverted. The remaining failure is in the
BrewFS TiKV metadata/writeback path, not TiKV availability or the JuiceFS TiKV
compose setup.

## Accepted Contention Fix, 2026-07-21

The failure was reproduced with warning logging in
`perf-run-1784602922-4507`. Concurrent chunk commits for one file all acquired
the same inode with `get_for_update`. Concurrent creates likewise acquired the
same parent inode. The TiKV transaction helper exhausted its ten retries under
these hot-key workloads and returned `MaxRetriesExceeded`; the writeback path
classified that as permanent and exposed FUSE `EIO`.

The accepted fix adds 256 shared transaction gates to the TiKV store. File
writes are gated by inode and the high-contention directory mutations are gated
by parent inode before entering the existing TiKV transaction. This only
serializes conflicting local work; TiKV still provides the transaction's
atomicity and cross-client conflict detection. The writeback retry classifier
also treats `MaxRetriesExceeded` as retryable within its existing bounded retry
budget instead of immediately poisoning the slice.

An ignored live-TiKV test runs 64 concurrent writes to one inode followed by 64
concurrent creates in one parent. It completed in 1.68 seconds. The mandatory
workspace test gate passed with 547 library tests and 611 binary tests, with no
failures.

### Accepted Buffered Matrix

BrewFS artifact:
`docker/compose-xfstests/artifacts/perf-run-1784605652-16698`.

JuiceFS reference artifact:
`docker/compose-xfstests/artifacts/juicefs-perf-run-1784557322-508`.

| Workload | BrewFS + TiKV | JuiceFS + TiKV | Result |
| --- | ---: | ---: | --- |
| seqread | 1121.5 MiB/s | 1150.1 MiB/s | BrewFS -2.5% |
| seqwrite | 211.8 MiB/s | 198.9 MiB/s | BrewFS +6.5% |
| randread | 1329.2 MiB/s | 362.7 MiB/s | BrewFS +266.5% |
| randwrite | 184.6 MiB/s | 182.8 MiB/s | BrewFS +1.0% |
| randrw read | 385.4 MiB/s | 30.4 MiB/s | BrewFS +1169.7% |
| randrw write | 177.7 MiB/s | 15.4 MiB/s | BrewFS +1057.2% |
| bigread | 1575.4 MiB/s | 186.7 MiB/s | BrewFS +743.8% |
| bigwrite | 185.5 MiB/s | 159.7 MiB/s | BrewFS +16.2% |

All ten BrewFS tools completed. The artifact contains no
`MaxRetriesExceeded`, FUSE `EIO`, drain timeout, or final dirty-byte tail. The
random-write effective wall bandwidth was 171.6 MiB/s and random mixed-write
effective wall bandwidth was 162.0 MiB/s, so the result does not hide the gain
in close or drain time.

The matching buffered `looptest` completed 200 iterations in 6 seconds in
`docker/compose-xfstests/artifacts/perf-run-1784607356-15724`.

Metadata correctness is fixed, but metadata performance is not at JuiceFS
parity:

| Operation | BrewFS + TiKV | JuiceFS + TiKV |
| --- | ---: | ---: |
| create | 84.9 ops/s | 109 ops/s |
| open | 1210.9 ops/s | 2192 ops/s |
| stat | 683103.2 ops/s | 605816 ops/s |
| readdir | 19794.8 ops/s | 8086 ops/s |
| rename | 16.0 ops/s | 264 ops/s |
| metaperf wall time | 950 s | 272 s |
| dirstress wall time | 8 s | 2 s |
| dirperf wall time | 318 s | 86 s |

The especially low rename rate and long setup/cleanup wall time remain a TiKV
metadata optimization gap. They are not a reason to reject this correctness
fix, but they must not be presented as performance parity.

### Direct-I/O Guard

Artifact: `docker/compose-xfstests/artifacts/perf-run-1784607169-15972`.

Direct seqwrite, randwrite, and randrw all completed. Throughput was 212.1
MiB/s, 201.7 MiB/s, and 441.1/202.0 MiB/s read/write respectively. The final
snapshot had zero dirty, live-dirty, pending, and remote-inflight bytes and no
critical error signatures.

### TiKV Readiness Harness Fix

Two attempted full runs (`perf-run-1784605501-3630` and
`perf-run-1784605597-11564`) failed before mounting because the runner started
the `--no-deps` perf container before the one-shot `tikv-ready` service had
completed; BrewFS received `cluster is not bootstrapped` from PD. The BrewFS
and JuiceFS TiKV readiness checks now require three consecutive `Up` samples,
and both runners explicitly wait for `tikv-ready` before starting the perf
container. This changes setup only and is outside measured tool wall time.

## Tuned Balanced Matrix, 2026-07-22

The comparison was rerun after screening both clients independently. The
accepted artifacts are:

- BrewFS: `docker/compose-xfstests/artifacts/perf-run-1784683943-7077`
- JuiceFS: `docker/compose-xfstests/artifacts/juicefs-perf-run-1784686113-23030`

Both artifacts use TiKV v8.5.0, RustFS, buffered io_uring, a 512 MiB fio
working set, a 20 second runtime, and the same eleven-tool matrix. All eleven
tools passed in both artifacts.

The accepted BrewFS profile keeps upload-before-commit durability and uses 16
FUSE workers, `max_background=512`, S3 concurrency 16, upload concurrency 10,
no compression, and a 1 second/65,536-entry metadata open cache. The accepted
JuiceFS profile uses `writeback=false`, an 8 GiB buffer, a 4 GiB cache,
`max-uploads=4`, no compression, and the same 1 second/65,536-entry open cache.
Explicit JuiceFS readahead and prefetch were not retained because the short
matrix regressed seqread from 1099.7 to 1017.8 MiB/s and randread from 1197.3
to 1141.1 MiB/s.

### Data Path

Active throughput is the fio-reported rate. Complete throughput divides the
actual bytes by tool wall time plus any post-write drain. BrewFS uses
upload-before-commit and had no post-write debt. The accepted JuiceFS profile
uses no writeback, so its close/fsync tail is already included in tool wall
time and its post-write drain is zero.

| Workload | BrewFS active | JuiceFS active | BrewFS complete | JuiceFS complete |
| --- | ---: | ---: | ---: | ---: |
| seqread | 1123.7 MiB/s | 1063.6 MiB/s | 1126.6 MiB/s | 1063.8 MiB/s |
| seqwrite | 213.3 MiB/s | 473.4 MiB/s | 177.8 MiB/s | 178.7 MiB/s |
| randread | 1455.3 MiB/s | 907.7 MiB/s | 1386.7 MiB/s | 865.0 MiB/s |
| randwrite | 211.8 MiB/s | 574.3 MiB/s | 176.7 MiB/s | 171.7 MiB/s |
| randrw read | 389.5 MiB/s | 28.4 MiB/s | 354.4 MiB/s | 28.0 MiB/s |
| randrw write | 180.4 MiB/s | 14.2 MiB/s | 164.2 MiB/s | 14.0 MiB/s |
| bigread | 2233.4 MiB/s | 190.0 MiB/s | 1365.3 MiB/s | 186.2 MiB/s |
| bigwrite | 187.2 MiB/s | 123.8 MiB/s | 186.2 MiB/s | 120.5 MiB/s |

The JuiceFS foreground write rates are not comparable by themselves because
seqwrite and randwrite spend 33 and 47 additional seconds in close/flush. Once
that cost is included, seqwrite is within 0.5% and BrewFS randwrite is 2.9%
faster. BrewFS remains substantially faster for mixed I/O, large reads, and
large writes.

### Metadata Path

| Operation | BrewFS + TiKV | JuiceFS + TiKV |
| --- | ---: | ---: |
| create | 82.1 ops/s | 106.1 ops/s |
| open | 5389.2 ops/s | 10447.5 ops/s |
| stat | 688342.8 ops/s | 606376.7 ops/s |
| readdir | 16520.8 ops/s | 7609.2 ops/s |
| rename | 115.7 ops/s | 257.0 ops/s |
| metaperf wall time | 593 s | 305 s |
| dirstress wall time | 8 s | 3 s |
| dirperf wall time | 308 s | 87 s |
| looptest wall time | 5 s | 4 s |

The BrewFS metadata profile improves open from 1210.9 to 5389.2 ops/s, rename
from 16.0 to 115.7 ops/s, and metaperf wall time from 950 to 593 seconds versus
the previous stable artifact. Create remains unchanged, and JuiceFS is still
about 1.9x faster for open, 2.2x faster for rename, and 1.9x faster in total
metaperf wall time. BrewFS remains faster for stat and readdir.

The BrewFS artifact has zero final dirty, live-dirty, pending-upload, and
remote-inflight bytes and no EIO, `MaxRetriesExceeded`, PD leader loss, or
drain timeout. The accepted JuiceFS artifact reports zero timeout warnings;
its two warnings are non-timeout background diagnostics.

### Rejected Tuning Candidates

- BrewFS commit-before-upload profile
  `perf-run-1784608085-31470` was rejected. Randwrite took 212 seconds at the
  wrapper level, PD repeatedly lost its leader, transaction heartbeats reported
  `TxnNotFound`, and the run did not complete. Increasing concurrency would
  amplify the unhealthy writeback debt rather than fix it.
- JuiceFS writeback artifact
  `juicefs-perf-run-1784685097-5867` completed, but reported 24 cache timeout
  warnings. Its seqwrite/randwrite/bigwrite drains were 62/52/47 seconds. Fully
  drained write rates fell to 115.1/136.9/55.6 MiB/s, below the accepted
  non-writeback profile for the complete matrix.
- JuiceFS `max-uploads=20` candidates
  `juicefs-perf-run-1784683317-22356` and
  `juicefs-perf-run-1784683548-24423` produced sustained 30 and 60 second local
  cache write timeouts with and without `cache-large-write`.
- JuiceFS `max-uploads=8` candidate
  `juicefs-perf-run-1784683677-24082` avoided the timeout for seqwrite but
  reproduced it under randwrite. Four uploads is the highest healthy screened
  setting on this host.

The TiKV runner now exposes metadata and writeback profile flags and forwards
the cache, writeback, concurrency, open-cache, and drain variables that were
previously dropped at the compose boundary. This made the screened profiles
explicit in `perf-profile.env` rather than relying on host-only variables.

Before accepting the final artifacts, the local CI gate passed formatting,
shell/report tests, workspace check/build, all runtime feature checks, clippy,
and `git diff --check`. Workspace tests completed with 547 library tests and
611 binary tests passing, with zero failures.

## TiKV Namespace Round-Trip Optimization And Four-Backend Matrix, 2026-07-22

The final TiKV metadata pass used JuiceFS v1.3.1 at commit `e0032b2` as a
behavioral reference. The accepted BrewFS changes keep visible POSIX directory
semantics while reducing transaction round trips:

- inode IDs are allocated from process-local leases of 1,024 IDs;
- create batches the parent and dentry pessimistic reads and returns the inode
  attributes from the committing transaction instead of issuing a post-create
  stat;
- rename batches parent, dentry, and source/destination inode reads;
- read-only open uses a 24-hour relatime fast path, while truncate still uses a
  write transaction;
- hot parent/inode transaction gates remain sharded so conflicting namespace
  operations serialize without globally serializing metadata traffic.

The full local gate passed with 548 library tests and 612 binary tests, plus the
workspace checks/builds, runtime feature checks, clippy, formatting,
shell/report tests, and diff checks. A later repeat saw one unrelated writer
timing test exceed its deadline once; the same test passed immediately when run
again in isolation.

### Final artifacts

| Metadata | Filesystem | Artifact |
|---|---|---|
| Redis | BrewFS | `docker/compose-xfstests/artifacts/perf-run-1784699784-1494` |
| Redis | JuiceFS | `docker/compose-xfstests/artifacts/juicefs-perf-run-1784700215-27086` |
| TiKV | BrewFS | `docker/compose-xfstests/artifacts/perf-run-1784698922-7734` |
| TiKV | JuiceFS | `docker/compose-xfstests/artifacts/juicefs-perf-run-1784700736-11129` |

All four runs passed the same 11-tool matrix: seven core fio scenes plus
`metaperf`, `dirstress`, `dirperf`, and `looptest`. The fio runs used a 512 MiB
working set, 20-second runtime, buffered io_uring, 4 MiB requests, and no
compression.

| Metadata | BrewFS metaperf wall | JuiceFS metaperf wall | BrewFS/JuiceFS data geomean | BrewFS/JuiceFS all-12 metadata geomean |
|---|---:|---:|---:|---:|
| Redis | 209 s | 196 s | 2.24x | 1.72x |
| TiKV | 445 s | 271 s | 2.39x | 1.58x |

For BrewFS TiKV, the accepted result improved metaperf wall time from 593 to
445 seconds. Rename increased from 115.7 to 136.1 ops/s, create from 82.1 to
84.6 ops/s, open remained effectively flat at 5,380.2 ops/s, and readdir
increased from 16,520.8 to 18,450.3 ops/s. Against JuiceFS TiKV, BrewFS remained
behind in create/open/rename but led in stat and readdir; the complete table is
recorded in `doc/introducing-brewfs.md`.

Every BrewFS run finished with dirty, live-dirty, pending-upload, and remote
upload-inflight gauges at zero, so the data-path gains were not deferred into a
post-run drain. JuiceFS TiKV emitted one transient directory-stat consistency
warning during `dirstress`; the tool still passed and the session flushed and
closed normally.
