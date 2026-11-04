# Small-file warm latency

Branch: `perf/smallfile-warm-latency`, from main
`f5706712302435206269a253bf861a1a648761d1`. This is the merge base of main and
`f38ae44`; `f38ae44` itself is an experiment documentation commit, not main.
No changes are made to the branches for #402, #406 or #414.

Status: rejected experiment, documentation-only draft. The candidate misses
the absolute warm target and has an unresolved original-runner remount failure.
Its production code and added tests have been reverted; the exact patch and
executables remain in the experiment directory.

Task window: 2026-11-03 20:18:20 through 2026-11-04 01:18:20 UTC.
Attribution is due at 22:18:20 UTC. Measurements, binaries, scripts and logs
are retained under `/mnt/brewfs-data/home/hxy/smallfile-warm/`.

## Attribution before implementation

The historical 0.575641 ms/file median and native 0.27 ms/file reference are
reused, not re-benchmarked. The accepted #409 cohort's third Compose round
contains 8,192 deterministic 4 KiB files and takes 4.773213 seconds, or
0.582668 ms/file. The repository scanner does directory discovery, explicit
`stat`, Python buffered `open`, full `read` including an EOF probe, and
`close`. Its cost therefore includes more than the FUSE OPEN/READ/RELEASE
handlers alone.

`BREWFS_REQUEST_TRACE` is available on main. The historical sample rate is
307. Combining the timeseries with the final snapshot recovers all 374
sampled requests, deduplicated by request ID; using only the final ring
would retain just 256 and bias the attribution toward the scan tail.

| Operation | Requests/file | Sampled requests | Estimated request lifecycle, µs/file |
| --- | ---: | ---: | ---: |
| LOOKUP | 2 | 53 | 89.267 |
| GETATTR | 6 | 159 | 167.036 |
| OPEN | 1 | 26 | 35.906 |
| READ, including EOF | 2 | 56 | 94.820 |
| FLUSH during close | 1 | 27 | 23.830 |
| RELEASE | 1 | 27 | 34.840 |
| IOCTL from buffered open | 1 | 26 | 22.114 |
| Warm readdir, separate historical guard | — | three guard batches | 1.453 amortized |

Readdir's historical median is 11.905976 ms for the directory, only about
0.25% of the supplied 0.575641 ms/file reference. The FUSE rows expand each
opcode's sample mean by its actual request count. They are **not an exclusive
wall-clock partition**: asynchronous RELEASE can overlap later operations,
sampling adds uncertainty, and kernel/client time outside observed FUSE
requests is absent. Readdir comes from the separate matched guard cohort.
The original 0.575641 median must not be artificially forced to equal the
sum of these estimates.

The trace's exclusive segments, weighted by the same opcode counts, are:

| Segment | Estimated µs/file |
| --- | ---: |
| Reply | 224.994 |
| Request queue wait | 80.211 |
| Handler poll + residual | 152.184 |
| Flush wait | 6.906 |
| File lock wait | 3.516 |
| Metadata RPC/commit, object PUT, chunk lock, dirty merge | 0 |

These segments are wall-time observations, not CPU profiling, and are
exclusive within an individual request only. Exact
full-scan handler counters put the two READ handlers at about 42.5 µs/file;
eliminating their entire cost would still be insufficient to save the
175.6 µs/file needed to reach 0.40 ms. Packed readonly bypasses MetaClient:
zero metadata RPC counters do not imply zero metadata object I/O. Actual
packed backend range GET counters remain the authority for GET/file.

The historical analysis is reproducible with
`summarize-historical-trace.py`, `historical-attribution.json`, and
`historical-exclusive.json` in the experiment directory. Source artifacts
are preserved there as `historical-candidate-r3`; their original location is
`/home/hxy/opt-reports/opt-b/pack-compose-artifacts/candidate-20261102T151417Z-17006-r3`.
This table was recorded before any production or test source edit, at
approximately 21:51 UTC, within the attribution deadline.

A current-main syscall-stage probe measures readdir, stat, open, read, close
and Python remainder separately, plus tree-only, open/close and open/read
paths with direct=1/0. It captures HTTP metrics and request traces and
validates every payload. Main and fixture builds completed at 22:29 UTC;
the fresh probe therefore follows the historical attribution deadline.

The direct=1 probe's three rounds average 1.675848 ms/file. Unlike the trace
estimates above, these instrumented wall-clock stages are additive:

| Stage | direct=1 mean µs/file | direct=0 mean µs/file |
| --- | ---: | ---: |
| Readdir amortized | 2.777 | 2.685 |
| Explicit stat | 405.726 | 400.177 |
| Buffered open | 701.790 | 695.911 |
| Full read including EOF | 414.671 | 523.669 |
| Close | 119.893 | 118.824 |
| Remaining Python work | 30.991 | 30.065 |
| Total | 1675.848 | 1771.333 |

The same three-round probe also checks the requested path differences:

| Diagnostic path | main direct=1 ms/file | main direct=0 ms/file |
| --- | ---: | ---: |
| Directory discovery only | 0.002566 | 0.002783 |
| Open/close only | 0.894780 | 0.861102 |
| Open/read/close | 1.298345 | 1.356571 |
| Original stat/open/read/close sequence | 1.675848 | 1.771333 |
| Open/read/close minus open/close | 0.403565 | 0.495468 |

The path deltas compare separate scans and include their variation. The
instrumented stages above time open and close individually; those stages
provide the additive wall-time breakdown.

The first scan incurs nine backend GETs (0.001099/file) and receives
2,159,520 bytes. This current-host probe must not be substituted for the
historical 0.575641 reference: the historical artifact ran kernel
6.8.0-147-generic, while this host runs 6.8.0-139-generic, and sampled reply
times are substantially higher here. The paired main/candidate scans below
compare this same host; the absolute 0.40 ms gate still applies.
The direct=0 probe is 1.771333 ms/file, with most of the difference in the
read stage. Both probes finished at 22:33:59 UTC with all 8,192 payloads
validated. Raw stage measurements, counters and trace summaries are in
`artifacts/attribution-main-d{1,0}` and `attribution.json`.

The compact attribution data and analysis scripts are also retained in
[`doc/performance/evidence/smallfile-warm-20261104`](../../performance/evidence/smallfile-warm-20261104/).
In the fresh first scan, `/metrics` reports 16,384 LOOKUP, 49,154 GETATTR,
8,192 OPEN, 16,384 READ, 8,192 FLUSH, 8,192 RELEASE and 8,192 IOCTL operations
with direct=1. Direct=0 has 65,538 GETATTR and 8,192 READ; the extra GETATTRs
outweigh eliminating the EOF READ in this workload. Metadata RPC counters
stay zero. The HTTP packed object counters independently confirm nine GETs
and 2,159,520 received bytes.
Generic block/page/meta cache hit and miss increments are also zero in both
diagnostic modes; they do not describe the dedicated packed GroupMeta cache.

## Single hypothesis

The Linux Tokio transport currently sends every `readv` and `writev` through
`spawn_blocking`. The hypothesis is that readiness-based request receipt and
small ordinary replies can remove repeated thread-pool handoffs while
preserving permissions, metadata/payload caches, inline layout and GET count.
The candidate uses Linux nonblocking reads and readiness-based ordinary
replies with payloads up to 8 KiB. Notifications (unique=0) and larger replies
retain the blocking pool. The fallback preserves backpressure and buffer/fd
ownership. FreeBSD keeps its blocking transport; macOS is unchanged.
The candidate has not been accepted.

Linux 6.8's `fs/fuse/dev.c` confirms that O_NONBLOCK governs request reads,
whereas notification writes can block during inode invalidation. Any
candidate must keep notification writes off the runtime workers and retain
the existing invalidation ordering. Cancellation, split-buffer ownership,
write backpressure and mounted teardown need explicit verification.

Pre-implementation review also found that privileged mounting opens
`/dev/fuse` before attaching its connection. `fuse_dev_poll` returns
`EPOLLERR` without registering a wait queue before attachment, so the
candidate must lazily register on first I/O, shared by all connection clones.

At 22:35 UTC the baseline test proved that canceling a pending read on
unmount retained its input buffer until another packet arrived (2 passed,
1 failed). The candidate passes all three focused tests and all 72 vendor
tests. The other focused cases exercise cloned readers after idle periods,
split reply headers, Send-but-not-Sync owned buffers, notification fallback
and write backpressure. The matching candidate release build started at
22:35:39 UTC. It keeps the main optimization/features and serializes rustc
processes with flock while allowing four LLVM jobs inside the active process.

## Three paired rounds: target not met

Required: three alternating paired main/candidate rounds, warm <=0.40
ms/file, GET/file <=0.002, cold regression <=5%, no readdir regression,
paired t-test and 95% CI, local workspace/overlay/strict clippy gates and
appropriate Compose mixed read/write and metadata guards. The paired
confidence interval uses df=2 and t-critical 4.3026527299.

The preserved candidate build finished at 23:11:24 UTC. Both executables use
`--release --no-default-features --features
fuse-tokio-runtime,workspace-overlay,observability`, library optimization 3
and CLI `-C opt-level=1 -C debuginfo=0`. No compilation overlaps measurements.
The original Compose scanner runs on twelve fresh mounts, with A/B, B/A,
A/B ordering in each warm/cold phase. Warm scans follow `packed-tree`;
cold scans have no tree prewarm. Cold means a fresh client, not flushed
object-server caches. Payload caches remain disabled, metadata budget is
256 MiB, direct=1, keep-cache=0 and TTL=0 in both arms.
The regenerated deterministic corpus contains 147 fully inline groups,
8,192 inline dentries and 32 MiB of inline payload, with no external frames.
It uses the post-#409 sequential-small-file group boundary policy, matching
the historical candidate's 147-group layout. The older 56-of-256 inline
limit describes the pre-#409 layout, not this corpus. Build provenance is
retained in `manifest-key.source.json`.

| Pair order | Warm main ms/file | Warm candidate ms/file | Cold main ms/file | Cold candidate ms/file | Warm readdir main/candidate ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| A/B | 1.692669 | 0.976618 | 1.729471 | 1.030501 | 21.557 / 14.955 |
| B/A | 1.736681 | 0.995991 | 1.769570 | 1.079966 | 25.378 / 16.219 |
| A/B | 1.692208 | 1.020961 | 1.798957 | 1.006281 | 22.837 / 20.118 |
| Mean | 1.707186 | 0.997857 | 1.765999 | 1.038916 | 23.257 / 17.097 |

Warm paired candidate-minus-main difference is **-0.709329 ms/file**, 95% CI
**[-0.796784, -0.621874]**, t=-34.898, df=2, two-sided p=0.000820.
Mean paired reduction is 41.540% (95% CI 37.488% to 45.592%). The candidate
mean's own 95% CI is [0.942633, 1.053080] ms/file: **every candidate round
misses the 0.40 ms/file target**. These are current-host comparisons; the
historical 0.575641 and native 0.27 values are not mixed into this t-test.

Each warm arm makes exactly nine GETs: **0.001098633/file**, unchanged and
below 0.002. Each cold arm makes 159 GETs (0.019409180/file); the warm GET
budget must not be represented as a cold-cache result. Cold paired latency
improves 41.150%, with percentage-change 95% CI [-47.670%, -34.629%].
Including mount startup, cold main/candidate means are 2.128360/1.389997
ms/file, also improving. No cold regression is observed.

Each readdir value is the median of five post-scan directory measurements
outside the scanner timing. All three candidate values improve, but their paired 95% CI
is [-14.214, +1.894] ms (p=0.08124). Three pairs do not statistically exclude
a small readdir regression. No extra rounds are substituted for the requested
three-round cohort.

All twelve runs read 8,192 files and 32 MiB without scanner errors. Every
file's deterministic content and SHA-256 match across all twelve runs.
The analysis verifies binary hashes, source revision/patch digest, full-read
mode and matching runtime/cache contracts before calculating statistics.
Detailed rows and analysis are in `paired-summary.json`, `summarize-pairs.py`
and `artifacts/{warm,cold}-{main,candidate}-r{1,2,3}` under the experiment root.

## Remaining cost after the transport experiment

Recovering timeseries plus final snapshots yields all **374 samples in each
of the six warm arms**. Three-round weighted request-segment means are:

| Within-request segment | Main µs/file | Candidate µs/file |
| --- | ---: | ---: |
| Reply | 756.000 | 157.611 |
| Request queue wait | 224.928 | 118.066 |
| Handler poll + residual | 284.016 | 241.011 |
| Flush wait | 11.789 | 10.369 |
| File lock wait | 5.904 | 5.465 |

These remain request-level estimates, not an additive wall-time partition.
The experiment removes much of the traced reply and queue time, while the
wall-time scan still crosses about fourteen FUSE requests per file. Trace
starts after request receipt; time outside its coverage cannot automatically
be labeled kernel execution or charged to one handler.

A separate candidate direct=1 diagnostic repeats the additive stage probe
three times on one mount. It averages **0.855562 ms/file**, split as follows:

| Stage | Candidate diagnostic µs/file |
| --- | ---: |
| Readdir amortized | 2.228 |
| Explicit stat | 213.953 |
| Buffered open | 337.190 |
| Full read including EOF | 222.651 |
| Close | 59.070 |
| Remaining Python work | 20.471 |

The diagnostic has a different sampling/repetition regime from acceptance
and does not replace the 0.997857 ms/file Compose result. Stat/open/close
alone total about 610 µs/file, so removing read-side copies alone cannot
reach 400 µs under this measured call sequence. Open/close-only, without
explicit stat or read, averages 0.425453 ms/file. Metadata RPC remains zero;
there is no evidence for a metadata-RPC batching optimization in this path.
Further work needs to distinguish receipt/scheduling and kernel/client
round trips, while preserving the stated TTL/cache contract. A different
scanner or more permissive caching is not used to claim a win.

## Validation and disposition

All candidate local gates completed successfully at 00:22:38 UTC:

| Local gate | Result |
| --- | --- |
| Root and vendor fmt; diff | Pass |
| Four shell syntax checks and three shell tests | Pass |
| Workspace check/build; Tokio and io-uring checks | Pass |
| `cargo test --workspace --lib --bins` | 1,327 passed, 264 ignored, zero failures |
| Strict workspace clippy, all targets, `-D warnings` | Pass |
| Full Tokio/overlay/observability tests, integrations and doctests | 2,805 passed, 502 ignored, zero failures |
| Strict matching overlay clippy, all targets, `-D warnings` | Pass |
| Standalone vendor transport tests | Focused 3/3 and full 72/72 pass |

Ignored tests remain ignored; these counts do not imply that they ran.
`validation-summary.json` records commands' exit codes, test summaries,
log hashes and experiment input identities; `run-gates.sh` retains the exact
commands. Both workspace and overlay clippy used `-- -D warnings`.
No GitHub Actions result is used as acceptance evidence.

## Original broad guard failed; follow-up is diagnostic only

The Compose guards started after all compilation ended. The first main
`fio-randrw` arm completed, including a 13-second strict post-write drain
with zero final pending/dirty/buffer-dirty bytes. The first candidate arm
prefilled and strictly drained its dataset, but the subsequent remount
failed **before timed fio** with `Address already in use (os error 98)`.
`guard-mixed-r1-candidate-d0` has no completion marker and no timed-fio
result. It cannot form a valid original-runner mixed pair.

The runner's `cleanup()` unmounts and sends SIGTERM without waiting for the
old process to exit. The metrics listener binds before mounting, so a
surviving listener is a plausible explanation. Its owner was not captured
in the original failed container, and this is **not proven to be a harmless
harness-only race**. Five idle remounts per arm succeeded both with original
cleanup and with an added process-exit wait (20 remounts total); those idle
probes did not reproduce the failure after a write workload.

A separate diagnostic then ran three alternating A/B, B/A, A/B mixed pairs
with the same preserved binaries and profile, adding an explicit wait for
the previous mount process to exit after cleanup. This is a changed cleanup
protocol and does not replace the failed original guard. Each run records
that change in `diagnostic-contract.json`. The Redis/S3 profile uses 512 MiB
per fio job, four jobs, 4 MiB mixed I/O, 70% reads, depth 1, direct=0 and
20 seconds, with prefill drain/remount and strict post-write drain enabled.

Each cell below is main/candidate. Bandwidth is MiB/s, latency is ms, and
drain is seconds; effective wall bandwidth follows the canonical runner's
wall-time accounting.

| Pair order | Active BW | Effective wall BW | Active + drain BW | Read p99 | Write p99 | Strict drain |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A/B | 521.652 / 506.832 | 522.200 / 485.714 | 282.110 / 327.711 | 122.159 / 135.266 | 119.013 / 104.333 | 17 / 11 |
| B/A | 473.855 / 503.063 | 451.810 / 505.000 | 305.838 / 324.999 | 156.238 / 120.062 | 130.548 / 89.653 | 11 / 11 |
| A/B | 504.645 / 509.101 | 481.143 / 510.400 | 315.533 / 308.856 | 119.013 / 154.141 | 83.362 / 117.965 | 12 / 13 |

Mean paired active bandwidth changes +1.40% (95% CI [-9.84%, +12.64%]);
effective wall bandwidth changes +3.62% ([-20.27%, +27.52%]); active-plus-drain
bandwidth changes +6.77% ([-15.96%, +29.50%]). Read p99 changes +5.70%
([-60.61%, +72.01%]) and write p99 changes -0.72% ([-94.57%, +93.14%]).
These intervals do **not** exclude a material mixed-workload regression.
The third candidate pair's write p99 is 41.5% worse. These diagnostic
observations do not clear the mixed guard.

All six diagnostic arms end with zero dirty, live-dirty, recent-pending
and remote-upload-inflight bytes. Measuring amplification through the final
strict-drain snapshot gives exactly 256 PUT/GiB written in every arm,
4.000004 MiB average PUT size, byte amplification 1.000001 and zero partial
tails. Mean upload batch size ranges from 4.583 to 4.720 MiB. Active-window
PUT deltas alone would omit uploads completed during drain.

The additional process-exit waits after prefill cleanup are
0.154/174.788, 61.175/173.532 and 0.226/81.836 ms (main/candidate). Final
cleanup waits are 164.270/339.263, 357.960/225.499 and 286.374/223.784 ms.
These waits start after cleanup and observer startup; they are not full
teardown durations. They show that the old process can outlive cleanup and
that candidate teardown timing still needs investigation.

The raw failed run, four idle probes and six diagnostic mixed arms are
preserved. `diagnostic-mixed-summary.json` contains paired statistics,
strict-drain amplification, final debt and exit waits. The original broader
pass stopped at the failed candidate remount; remaining core fio scenes,
metaperf/dirstress/dirperf and direct-write guards were not completed.

## Final disposition

The absolute warm target fails; readdir and mixed no-regression claims are
not statistically established, and the original remount failure remains a
validity blocker. This experiment stays draft and cannot become ready or
merge. Production source under `src/` and `vendor/` is identical to the base
after the targeted reversion; final root/vendor fmt and diff checks pass.
Only this report and evidence are submitted. The complete rejected patch is
`candidate-reproduction.patch` (SHA-256
`5fa17b055393253565d79fd0b13da32ca172796e51126502c87915a46a197477`)
under the experiment root. No accepted README performance table is updated.
The owned Compose project was stopped with `down -v --remove-orphans`;
bind-mounted raw artifacts, fixture data and preserved binaries remain.
