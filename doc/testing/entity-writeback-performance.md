# Entity writeback optimization: WSL evidence

Measured on 2026-10-30 against `ad5429aab1d65218ea6ac670f03aef91d3f5c3b8`.
Only the opt-in entity writeback queue changes. Default synchronous upload,
legacy overlays, and the entity synchronous PUT/publication path are unchanged.
These measurements concern newly created upper files, not lower copy-up.

## Method

Ubuntu 24.04 / WSL2 kernel 6.18.33.2, 16 logical CPUs, approximately 24 GiB RAM.
Owned loopback Redis (AOF disabled) and pinned RustFS containers use the same WSL
ext4 device as staging. There is no injected network latency. Builds and temporary
build swap do not overlap performance runs. Both BrewFS binaries are release
builds with `--no-default-features --features fuse-tokio-runtime,workspace-overlay`;
CLI Tokio worker count remains its default of one.

Baseline and candidate, both lowers and both upload modes, plus stock JuiceFS
1.3.1, alternate order across three repetitions. Each cell has fresh metadata,
volume and cache. Compression, clean read caching, prefetch and metadata TTLs are
disabled. Target block size is 4 MiB, maximum FUSE request size 1 MiB, background
request limit 256, and application concurrency is one or 16. Application O_DIRECT
uses aligned buffers for every client; the old binary is fully remeasured with
this workload, rather than splicing earlier absolute timings.

The primary clock starts at the workload and ends after remote drain, unmount,
maintenance transition (BrewFS), and process exit. Post-write debt observation
is included. Download/readback verification happens only after the clock stops.
BrewFS requires zero persistent debt, `remote_ready` and Quiescent. JuiceFS
requires zero staging and inflight uploads plus no remaining raw staging files.
Published BrewFS objects are downloaded and checked against exact size/SHA-256;
JuiceFS gets an additional cache-disabled read-only mount to verify every file.

## Creation workload

`small` creates 1,000 files of 16 KiB; `bulk` creates 32 files of 8 MiB.
Every file is fsynced. Values below are three-run medians in seconds. Columns
have independent medians, so active and tail medians need not add exactly to
the total median.

| Workload | Workers | Lower | Old async total | New async active | New async tail | New async total |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| small | 1 | native | 22.286 | 13.380 | 0.041 | 13.421 |
| small | 1 | packed | 22.170 | 13.132 | 0.038 | 13.170 |
| small | 16 | native | 25.823 | 1.381 | 7.382 | 8.763 |
| small | 16 | packed | 25.654 | 1.300 | 7.713 | 8.994 |
| bulk | 1 | native | 9.789 | 4.129 | 0.094 | 4.226 |
| bulk | 1 | packed | 10.192 | 4.294 | 0.093 | 4.387 |
| bulk | 16 | native | 12.495 | 2.133 | 2.551 | 4.683 |
| bulk | 16 | packed | 12.233 | 1.576 | 2.612 | 3.961 |

Including final drain, total time falls by approximately 40–68% relative to the
previous async implementation. This does not establish that async always beats
synchronous upload: the latter already overlaps uploads with 16 application
workers and avoids the extra local durability/debt work. Default sync stays in
place. The complete synchronous and async rows are retained in the archive.

## Mixed read/write guard

fio 3.36: 70/30 randrw, 4 KiB, psync, 16 jobs, iodepth=1, five seconds,
fsync after each write, fixed random seed, distinct 2 MiB per-job files. Seeding
and its initial drain are outside the timer. CRC verification is enabled, with
remote-only verification after the timed final drain. Since this is a timed
workload, effective throughput uses actual I/O bytes divided by active-plus-drain
time; elapsed time alone is not a comparable amount of work.

| Application direct | Lower | Async effective throughput change | Read P95 old/new ms | Write P95 old/new ms | Sync effective throughput change |
| ---: | --- | ---: | ---: | ---: | ---: |
| 0 | native | +151.7% | 70.78 / 26.87 | 73.92 / 47.97 | +2.6% |
| 0 | packed | +146.6% | 89.65 / 25.03 | 123.21 / 42.21 | -4.9% |
| 1 | native | +139.7% | 73.92 / 26.08 | 78.12 / 45.88 | +2.0% |
| 1 | packed | +141.8% | 70.78 / 26.61 | 78.12 / 52.69 | -1.2% |

All four async groups improve throughput and read/write P95. Sync control
observations are retained, including the buffered packed throughput -4.9% and
write P95 122.16 → 128.45 ms. Entity always uses FUSE direct I/O; JuiceFS's
application-buffered group can use kernel page cache, so the direct=1 group is
the stricter cross-product comparison.

## Independent sync audit

The initial three-run bulk controls had noisy point estimates, including
native/one-worker -6.8% and packed/16-worker -19.0%. These results were not
deleted. Seven additional paired rounds each rebuilt their owned Redis/RustFS
backend, reversed/rotated order, and included both binaries, both lowers, both
worker counts, and JuiceFS sync. No outliers were removed.

| Workers | Lower | Old/new median total s | Ratio of median throughputs | Median paired throughput change |
| ---: | --- | ---: | ---: | ---: |
| 1 | native | 6.451 / 6.321 | +2.05% | +1.92% |
| 1 | packed | 6.186 / 6.202 | -0.25% | -1.40% |
| 16 | native | 1.921 / 1.893 | +1.44% | -0.67% |
| 16 | packed | 1.903 / 1.918 | -0.80% | +0.71% |

The initial large negative estimates did not reproduce in the seven-round audit.
This supports accepting the async improvement without a repeatable material sync
regression in these workloads; it is not a guarantee for other workloads or hosts.

## Cost evidence and JuiceFS boundaries

A separate traced 1,000-file / 16-worker native-async run records:

| Client syscall | Previous async | New async |
| --- | ---: | ---: |
| fsync | 3,000 | 2,019 |
| getdents64 | 2,038 | 2 |
| statx | 482,550 | 3,005 |
| rename | 1,000 | 0 |

The new path still fsyncs each staged file and its directory before registering
debt and publishing. Savings come from atomic capacity reservations instead of
repeated directory scans, concurrent blocking I/O, eight bounded PUTs, combined
authority/debt snapshots, batched completion transactions and batched directory
sync on reclamation. Traced elapsed times are excluded from the main comparison.

JuiceFS `--writeback` also allows per-file fsync to return before global remote
drain, but its stock disk staging writes/closes/renames without per-block file
and directory fsync. Entity retains this stronger local persistence barrier.
See the measured revision's [writeback implementation](https://github.com/juicedata/juicefs/blob/e0032b2/pkg/chunk/cached_store.go#L425-L469)
and [disk staging](https://github.com/juicedata/juicefs/blob/e0032b2/pkg/chunk/disk_cache.go#L460-L510).
Formal strace covers both the JuiceFS supervisor and actual FUSE client; its
1,000-file async trace shows 1,000 renameat calls and no fsync/fdatasync.

JuiceFS uses max-uploads=8 for async and application concurrency for sync,
buffer-size=128 MiB and cache-size=1 B. Zero disk cache would disable writeback;
the minimum positive setting retains no complete clean data blocks. Its staging
is governed by free space rather than Entity's strict 1 GiB cap; the harness
checks pending bytes stay under 1 GiB. The creation workload is at most 256 MiB.
These implementation limits are explicit, not claimed to be identical caches.

With upload-delay=0, stock 1.3.1 scans its delayed queue every minute. A separate
three-run supplement changes only upload-delay to 1 ms, which also shortens the
[scan interval](https://github.com/juicedata/juicefs/blob/e0032b2/pkg/chunk/cached_store.go#L821-L838).
This flag changes two scheduling behaviors: zero delay attempts an immediate
upload when a slot is free, whereas positive delay sends every staged block to
the delayed queue. It is not an isolated timer-only change. In overwrite-heavy
randrw, the 1 ms setting creates more remote objects and can lengthen drain;
both configurations' full results are retained rather than selecting the winner.
For 16-worker small files, total median time changes from approximately 61.3 s
to 7.0 s, versus Entity's 8.8–9.0 s. The default scan tail is not evidence that
Entity has higher upload bandwidth. The supplement is a scheduling sensitivity
test, not a production configuration recommendation. Bulk creates 256 upper
objects in Entity versus 64 in JuiceFS because of different write coalescing,
despite equal target block/FUSE request sizes. Cross-product conclusions must
retain these object-amplification and durability differences.

## Validation and retained artifacts

The production candidate passed the repository CI gate: fmt, seven shell gates,
workspace check/build, Tokio and io-uring feature checks, workspace lib/bin tests
(1,277 passed), strict Clippy, entity/FUSE/config unit tests (137 passed), real
Redis/S3 entity/packed tests (167 passed), and six FUSE/sync/async/crash-recovery
cases. Review restored explicit zero-debt orphan recovery and added its regression
test before these gates and release measurements. A final test-only adjustment
fixes the initial blocked-upload batch size before checking the eight-upload
window; it does not change the measured runtime code.
After that test adjustment, fmt, the 137 entity/FUSE/config tests, strict feature
Clippy and the 1,277 workspace lib/bin tests passed again.

The first mixed suite stopped on a JuiceFS post-drain, post-unmount process-exit
timeout. Its 39 completed rows and failure logs remain archived; the entire
60-cell mixed suite was rerun after matching BrewFS's controlled idle-connection
exit assistance. Assistance is allowed only after remote drain, unmount, and zero
kernel pending requests on that process's own FUSE connection. Its time and use
are recorded. Failed cells are never accepted timings. No runtime teardown
behavior is changed by this queue optimization.
Across the 278 accepted cells, one baseline sync-control cell needed the recorded
idle-connection assistance; candidate cells needed none. All BrewFS upload retry
counters were zero. JuiceFS does not expose an equivalent retry counter here;
its complete metrics are retained without inventing a zero value.

Local evidence is under the task's `validation/entity-async-optimization/`:
`entity-async-optimization.html`, `summary.json`, `summary.csv`,
`accepted-rows.json`, `review.md`, `source.diff`, `source-files.json`, and per-cell
configuration, command, CRC/hash, metrics and log files. The three core harness
files are `entity-upload-perf.py`, `perf_payload.py`, and `juicefs_cell.py`;
`harness-v1/` retains the original main-suite driver. Run definitions are in
`main.json`, `randrw.json`, `diagnostics.json`, `fastdrain*.json`, and
`sync-control-*.json`; runners use the owned Compose setup from
`tools/testing/run_entity_writeback_smoke.sh`. Reports retain each cohort's
engine/source/binary hashes, individual rounds, and failure records.

Release SHA-256:

- Baseline: `bbf93b019fe57933a127153574f9415e12e7c5d5119bda962a6be641eba998f6`
- Candidate: `bae7252cbb235442f08a758d898139c3f8d4fde9fa0e34fabacda1a2aca410ef`
- JuiceFS 1.3.1+2025-12-02.e0032b2: `a9e5fcb0278df96de366f99ded6e347e64fcb2230d97d3ce61816cc54eefdab5`
