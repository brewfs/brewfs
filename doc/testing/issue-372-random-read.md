# Issue #372: local random-read investigation

Baseline: `975deacc13131335b60a4c72de4ed781bbfe7154`, measured only on the
provided 8-vCPU / 30-GiB host. The severe overlay throughput gap reproduces;
the reported 400-ms p99 and 19x throughput ratio do **not** reproduce here.

| Arm | MiB/s, min / median / max (3 runs) | p99 ms, min / median / max (3 runs) |
| --- | --- | --- |
| flat-v1 | 85.238 / 86.334 / 87.403 | 30.802 / 31.326 / 31.588 |
| workspace-v1, native lower, before | 12.111 / 12.209 / 12.314 | 63.177 / 64.225 / 64.750 |
| workspace-v1, native lower, rejected candidate A | 77.825 / 78.000 / 79.357 | 31.850 / 31.850 / 32.375 |

The baseline native overlay median is 14.14% of flat-v1 throughput; its p99
is 2.05x flat-v1. These are FUSE measurements using the unchanged compose perf runner,
Redis 7.2, and a local RustFS S3 service. They are not in-memory microbenchmarks.

fio: 4-KiB random reads, `direct=1`, `io_uring`, four jobs at depth 32,
512 MiB per job (2 GiB total), 20 seconds. Compression is disabled, blocks are
4 MiB, chunks 64 MiB, read RAM/SSD budgets 1/4 GiB, write RAM/SSD budgets
512 MiB/1 GiB, and the mount memory budget is 4 GiB. Prefetch is disabled;
FUSE uses 8 workers and max_background 512. The default Tokio worker count
is 1. Both arms strictly drain prefill writes to zero dirty/pending bytes and
read on the same mount. Client/backend caches can be warm; these results are
not physical disk bandwidth claims. No compilers ran during measurement.

An initial overlay attempt failed to remount immediately after prefill with
`workspace is busy`. It produced no read measurement, was excluded, and its
logs were retained. The valid comparison disables prefill remount for both
arms, retaining strict prefill drain. The local single backing filesystem
does not reproduce the issue's four-disk EC 2+2 layout.

The actual native-overlay read path is
`FileReader::read_chunk_span_into_observed -> WorkspaceMetaLayer::read_plan`.
It is distinct from `NativeDataRuntime` / `workspace-native-v2`; that earlier
investigation was abandoned without a behavior change or accepted measurement.
`read_plan` keys its bounded resolver cache by the exact request range.
Each range miss calls `read_extent_deltas_consistent`, which brackets paged
scans of both layers with generation checks. The backend scans each chunk's
entire extent history before filtering it to the requested range. This is
the initial hypothesis; the request-level measurements below confirm the
repeated Redis work. No optimization was accepted.

The retained Redis commandstats already show the amplification: the first
valid overlay run issued 310,197 EVALSHA and 186,079 ZRANGEBYLEX calls for
62,017 fio reads (about five Lua round trips and three extent pages per
read). The first flat run issued 274 EVALSHA calls for 442,051 reads.
ZRANGEBYLEX/GET/TIME calls inside Lua are not additional client round trips.
The counts include small mount/lease/background traffic; the exclusive
wall-time attribution below measures the corresponding API waits.

Local artifacts are in `/work/issue372/`: `baseline/brewfs` and its SHA-256,
`compose.override.yaml`, `run-compose.sh`, `host.txt`, `baseline-summary.json`,
and complete per-run fio JSON, mount logs, stats, and drain records. Valid
runs are `baseline-flat-{1,2,3}` and `baseline-overlay-{1-valid,2,3}`.

Exclusive task-local timing sampled one in sixteen 4-KiB VFS reads. The
3,644 samples in `profile-overlay-1` averaged 1.204928 ms (VFS p99 3.390609 ms):

| Exclusive interval | Mean µs | Share of VFS time |
| --- | ---: | ---: |
| Layer/inode resolution (`chain`, `resolve_inode_delta_state`) | 2.273 | 0.1886% |
| Row cache (`UpperRowCache::get`) | 1.932 | 0.1604% |
| Redis get/get-many/scan API waits | 727.973 | 60.4163% |
| Extent scan/plan CPU and remaining work in `read_plan` | 20.776 | 1.7243% |
| Explicit view/layer locks and reader backpressure | 0.788 | 0.0654% |
| Data executor, including cache/object waits | 435.298 | 36.1264% |
| Other VFS work | 15.888 | 1.3186% |

These are wall-clock API intervals, including await/scheduling, not CPU-only
costs or fio end-to-end percentages. The diagnostic run measured 11.387 MiB/s,
69.730-ms fio p99 and 42.464-ms mean clat, so instrumentation is not free and
its throughput is not an acceptance measurement. Among the slowest 1% of VFS
samples, Redis accounts for 33.197% and data for 65.408%.

Existing dispatch/worker log timestamps correlated with 3,076 samples show
47.413 µs mean receive-to-worker wait. A separate perf trace of io_uring async
enqueue, `io_wq_submit_work`, and completion shows that random reads do not
enter the async work queue. That run's mean fio submission/completion latency
is 1.291/40.080 ms. All 61,778 read completion events matched fio's I/O count.

A follow-up perf run joined kernel submission/completion and fio 3.39's
`put_io_u` uprobe by pid and `io_u`/user_data. All 57,797 reads matched:

| Interval | Mean ms | Share of submission-to-recycle time |
| --- | ---: | ---: |
| Kernel submission to completion | 1.365697 | 3.0897% |
| Kernel completion to fio recycling the I/O | 42.835327 | 96.9103% |

The p99 sample for that total interval is 70.535289 ms: 2.844564 ms before
kernel completion plus 67.690725 ms after it. fio's own clat p99 is
68.681728 ms, a different boundary. Completion-to-recycle time includes fio
synchronously submitting other reads and processing completions; it is not
server-side work still pending on the completed read. Each fio process has at
most one kernel read in flight, despite configured depth 32. In 57,673 of
57,797 cases it submits another 31 reads before recycling the completed I/O.
Those later reads' kernel service totals 42.258084 ms on average, accounting
for 98.6524% of the completion-to-recycle interval. Repeated Redis work thus
limits throughput and accumulates into the client-observed clat through later
synchronous submissions. This is a matched-event measurement, not subtraction
of independent percentiles. The same run's
3,613 VFS samples average 1.217260 ms, with Redis at 740.680 µs / 60.8481%
and data at 432.844 µs / 35.5589%, confirming the service-cost attribution.
The temporary source instrumentation and kernel/userspace probes were removed;
patches, binaries, traces and parsing scripts are retained locally.

The initial broad baseline was aborted during 4-KiB random-write close/drain:
about 820 MB and 12,500 slices remained after several minutes. It is not a
passing guard. Diagnostics were retained and compose volumes reset. The broad
guard will use the repository's default 4-MiB random-write/mixed requests;
random reads remain 4 KiB. The unsuccessful 4-KiB write check remains explicit.

The regression fails against the original behavior: the first read leaves
7 backend scans, and 15 new offsets raise that to 52 (45 extra scans).
The overwrite/hole/version/unlink regression passes. Candidate B caches
whole-chunk plans for reads of at most 4 KiB under the existing identity/version
keys, shares them with Arc under the existing segment-weighted limit, and
clips only intersecting segments on each small read. Larger requests retain
exact-range query/planning and owned plan clones. Fresh inode resolution and
invalidation remain in place. Tests also cover open-unlink through final close,
fragmented-plan weight, and alternating small/large reads across independent
overwrites and truncate/extend.

The three successful candidate A compose runs improve median random-read
throughput by 6.389x and reduce median p99 by 50.408%. EVALSHA counts are
260 / 281 / 279 for 398,484 / 399,379 / 406,329 reads, or
0.00065–0.00070 per read. The five buffer/dirty/live/pending/inflight gauges
are zero both after prefill drain and after each read run. Artifacts are
`candidate-overlay-{1,2-valid,3}`; `candidate-overlay-2` was rejected at
mount with `workspace is busy`, before any fio measurement. Later mounts
waited for the lease window without changing lease settings. An extra
exit-diagnostic run is excluded from the performance table. The release
binary SHA-256 is
`6b97e7c8cdbad872e09e228573aaab955c84107c45a319685ce5d0334eafc3e7`;
features, profile, compiler, and rustflags match the localization build.

Candidate A is rejected. Three independently reset broad compose runs per
binary passed all tools and drained to zero, but mixed read/write read p99
rose from a median 1,937.768 to 2,332.033 ms (+20.35%); write p99 rose from
1,954.546 to 2,332.033 ms (+19.31%). Higher mixed throughput does not satisfy
the latency guard. The first-pair big-write p99 regression did not persist:
its three-run median fell 17.96%. Metadata fallback rename and dirperf readdir
rates also fell 7.16% and 10.13%, respectively, and remain unresolved.
Artifacts are `*-overlay-broad-direct1{,-r2,-r3}`, `broad-comparison*.{md,tsv}`,
and `rejected-all-reads-summary.json` under `/work/issue372/`. The all-request
whole-chunk behavior was reverted with a targeted patch. Candidate B limits
chunk-plan reuse to small reads and retains request-range planning for larger
I/O. Its gates and release build pass. The three fresh focused runs per
binary give the following results for rejected candidate B:

| Metric | Fresh baseline, min / median / max | Candidate B, min / median / max |
| --- | --- | --- |
| Random-read MiB/s | 12.101 / 12.168 / 12.271 | 76.965 / 78.570 / 79.528 |
| Random-read p99 ms | 64.750 / 65.274 / 65.274 | 31.850 / 32.113 / 32.637 |

Median bandwidth is 6.457x baseline and p99 falls 50.803%. Each B run issues
256 EVALSHA calls for 407,202 / 402,301 / 394,079 reads, versus about 5.0015
calls per read in the fresh baseline. All six focused runs have zero fio
errors and zero values for the five dirty/pending/inflight gauges both after
prefill drain and after reading. Artifacts are `*-small-overlay-{1,2,3}`,
`*-small-summary.json`, and `small-focused-validity.json`. B's SHA-256 is
`70c4bac39ca602d4b92f9d20f9e3a92f7cf5d356b1686355ee1458a9112a8f5f`;
release features/profile/compiler/rustflags match the localization build.

B is rejected. All six 20-second broad runs pass their ten tools, with zero
fio errors and all five gauges zero in nine drained snapshots per run. The
pooled original-profile runs raise mixed read/write p99 by 20.690%/25.561%
and metaperf open/read by 11.594% (the stat rate also falls 7.081%). These
metadata and tail-latency regressions fail the guard despite the focused-read
gain.

A fixed three-pair confirmation with 60-second mixed I/O was also completed,
but its different workload does not replace the failed original-profile
guard. Artifacts use `*-small-overlay-mixed60{,-r2,-r3}`. An initial
all-scenes-60s diagnostic was stopped during its first sequential read for
disk headroom and is preserved as `baseline-small-overlay-confirm60`.

The generated amplification rows contain cumulative counters. Per-scene
counter differences give mixed GET sizes of 1 MiB in both arms and median
GETs/GiB read of 222.845 / 222.225. See
`small-scene-get-amplification.json`; the cumulative GET rows include earlier
random-read traffic and cannot be attributed to metadata operations alone.

A cold miss in candidate C resolved the whole chunk only for a reusable small
read. Highly fragmented chunks can cost more CPU per miss and consume more of
the segment-weighted cache budget; the unchanged weight limit bounds retained
plans, not transient resolution work. The measured fixture and broad guard do
not cover every fragmentation shape.

Candidate C is rejected after the complete fixed broad guard. All six arms
passed all ten tools, fio reported zero errors, each run had nine drained
snapshots with all five dirty/pending/inflight gauges at zero, and teardown
completed normally. The mixed read/write scene improved (median read
431.418/362.407 MiB/s and write 198.514/167.984 MiB/s, with p99 reductions of
22.966%/23.963%), but metadata regressed: `dirperf stat` fell from
31,809.647 to 26,645.695 ops/s (-16.234%), `metaperf open` from 1,040.324 to
976.844 ops/s (-6.102%), and `metaperf rename` from 834.268 to 728.604 ops/s
(-12.665%). The six-run metadata phase diagnostic confirms the stat-stage
throughput drop (9,634.212 to 7,430.611 ops/s, -22.873%); FUSE getattr calls
fell from 378 to 204 in the paired stage while per-call time stayed nearly
flat (33.946 to 34.131 microseconds), so that count change does not explain
the regression. Candidate C therefore cannot be accepted. Its broad evidence
is in `/work/issue372/scoped-broad-summary.json` and
`/work/issue372/scoped-guard-evidence.json`; the metadata phase evidence is
`/work/issue372/scoped-metadata-phase-summary.json`.

The C focused result remains recorded for diagnosis only: fresh baseline and
candidate medians are 12.223 and 77.488 MiB/s, and p99 is 64.750 and
32.375 ms. The release binary SHA-256 is
`e49c0f68ef158c2f6f05d72f86206a5c1b10557cf546485faa747d96b8a604b1`.
The C source delta was reverted after rejection; no C behavior is accepted.

The C `-j 1` correctness gates pass with 1,275 default-feature library tests,
the complete workspace library/binary suite, and 2,503 tests in the
workspace-overlay configuration. The new regressions pass; no ignore
attributes were added. Formatting, the perf script self-tests, both FUSE
runtime checks, workspace check/build, and strict default/workspace Clippy
also pass. Native-packed library tests pass with `--test-threads=1`:
2,583 passed, zero failed, 451 pre-existing ignored tests. Two earlier parallel
runs each hit the previously documented SQLite ACL/atime `EBUSY` flake;
the identical test passes in isolation and in the serial full suite. No
test, assertion, ACL policy, or retry bound was changed.

An additional strict native-packed Clippy check fails on the existing
eight-argument `src/cli.rs::mount_native_with_catalog`. The complete CLI
file has the identical Git blob (`a476cdd0ec18631869ae05599e771b1e2e20bebf`)
as baseline `975deac`; no suppression or unrelated refactor was added.
Strict workspace-overlay Clippy also passes for candidate C. Its full gate
logs are under `/work/issue372/gates-scoped/`; candidate A's logs are archived
in `gates-rejected-all-reads/`. The original hardware, packed lower, and
JuiceFS comparison have not been validated here.
