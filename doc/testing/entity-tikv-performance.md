# Entity TiKV upper: cloud qualification

Measured on 2026-10-31 UTC against main
`cba857b567fa6e94d50edd1d2e1c47c25c7b2e50`. This change adds an explicitly
selected TiKV backend to the experimental entity shared layer. Both native and
packed lowers retain the same namespace, data and upload implementation.
`upload_before_commit` remains the default; legacy workspace dispatch is unchanged.

The existing Redis paths passed the defined regression checks. The new
packed + TiKV configuration remains substantially slower than JuiceFS + TiKV
in these workloads. This qualification establishes optional backend support;
it does not establish performance parity or a speedup from switching to TiKV.
There was no previous functioning entity TiKV backend to use as a TiKV baseline.

## Workloads and accounting

One Aliyun `ecs.g7.2xlarge` spot VM (8 vCPU, 32 GiB, 100 GB ESSD) hosts both
clients and the same loopback object service, one PD and one TiKV with one
replica. No swap, build, profiler or checkpoint transfer overlaps timed cells.
This is not an HA, multi-host or WAN benchmark. PD and TiKV report **8.5.3**;
the pre-existing local image aliases say 8.5.0, so image contents and reported
binary versions, rather than those aliases, were verified on both hosts.
The object service is `ax-retest/rustfs:a87e8f94`; Redis controls use 7.2.
JuiceFS is 1.3.1, build `e0032b2`.

There are 214 cells: four excluded pilots and 210 formal cells, with five
repetitions of every configuration and alternating paired order. All clients
use eight application workers, aligned O_DIRECT I/O, no compression, 4 MiB
chunk/block geometry, and a maximum 1 MiB FUSE request. TiKV comparisons use
eight runtime workers; both Redis control binaries retain the default of one.
Production binaries were built separately from observer/examples with identical
compiler and resolved dependency/features (489 package/feature entries):

```sh
cargo build --locked --release --no-default-features \
  --features fuse-tokio-runtime,workspace-overlay,rustfs-ec-block-size-hint \
  --bin brewfs
```

The immutable corpus contains 1,000 files of 16 KiB in 64 directories and
16 files of 8 MiB. Each 4 KiB sector has deterministic distinct content.
The main-branch packed producer uses raw codecs, no inline data, size-only
frames and the random-small-file profile (default maximum raw frame 256 KiB).
Read-only BrewFS cells must retain zero upper objects, proving that they read
the actual packed lower. New upper creation and lower COW are separate scenes.

| Scene | Work |
| --- | --- |
| Small creation | Create 1,000 × 16 KiB files; fsync and close every file |
| Bulk creation | Create 16 × 8 MiB files; 1 MiB application writes; fsync and close |
| COW | Overwrite 4 KiB within 100 existing 16 KiB files; fsync and close |
| Small read, no prewarm | Read all 1,000 files in a deterministic shuffled order |
| Half-prewarm random read | Fully read half the files, then 4,096 × 4 KiB reads, exactly balanced between initially warmed and unwarmed halves |
| Mixed read/write | Eight 8 MiB files, fio psync, 4 KiB, 70/30 read/write, 10 seconds, fsync after every write, CRC verification |

Every cell has a fresh mount. Clean payload caching is disabled. Half-prewarm
therefore primarily warms metadata; it is **not** a claim of a 50% payload hit
rate. TiKV, RustFS and server OS caches are not flushed between cells; the
no-prewarm scene is not a cold-server measurement. BrewFS has a 64 MiB immutable
metadata memo, 4 MiB index cache, and authenticated group/frame-directory caches
bounded at 256/128 KiB. JuiceFS metadata TTLs and both clients' FUSE TTLs are zero.
Their internal metadata caches and integrity work are not identical.

JuiceFS uses a 128 MiB buffer, no prefetch/readahead, eight maximum uploads and
16 maximum staging writes, with zero upload delay. Its clean cache is zero for
read/synchronous cells; asynchronous cells use the minimum positive 1B setting
to enable staging without retaining a full clean block. BrewFS has eight
background uploads and a 1 GiB staging limit. Its S3 concurrency setting of eight
limits multipart parts, not all GETs; these objects are below the 16 MiB default
multipart threshold. JuiceFS's pinned version has no maximum-downloads CLI flag.

The primary clock includes foreground work, fsync/close, debt observation,
remote upload completion, unmount, BrewFS maintenance transition and complete
filesystem-process exit. The observer is idle during foreground work; its exit
and subsequent remote readback verification are outside the clock. BrewFS must
have zero persistent upload debt, report remote-ready and become Quiescent.
JuiceFS must have zero pending/inflight staging and no raw staging files.
Every published BrewFS upper object is downloaded and checked by size/SHA-256;
JuiceFS writes get a fresh cache-disabled mount and full logical readback.

Mount/bootstrap, workload setup, prewarm and full client lifecycle are also
recorded in the [numerical evidence](entity-tikv-performance.json). Corpus
packing and the initial JuiceFS corpus import are separate setup operations.
BrewFS fsync in async mode durably stages locally. The pinned JuiceFS staging
path closes and renames without an explicit file/directory fsync; foreground
acknowledgement durability is not claimed identical. See the pinned
[JuiceFS cache implementation](https://github.com/juicedata/juicefs/blob/e0032b2/pkg/chunk/disk_cache.go).

## Cross-product results

Seconds below are independent five-run medians; active and tail columns need
not sum to the total median. Sync means upload-before-commit; async means
commit-before-upload / JuiceFS writeback. All tails include full client exit.

| Scene | Mode | BrewFS active | BrewFS tail | BrewFS total | JuiceFS active | JuiceFS tail | JuiceFS total |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Small creation | Sync | 20.383 | 0.019 | 20.401 | 2.482 | 0.066 | 2.551 |
| Small creation | Async | 25.735 | 0.025 | 25.759 | 1.111 | 1.138 | 2.249 |
| Bulk creation | Sync | 1.845 | 0.016 | 1.862 | 0.308 | 0.065 | 0.373 |
| Bulk creation | Async | 2.597 | 0.021 | 2.643 | 0.219 | 0.157 | 0.381 |
| COW | Sync | 1.230 | 0.016 | 1.246 | 0.237 | 0.066 | 0.303 |
| COW | Async | 1.789 | 0.046 | 1.832 | 0.089 | 0.179 | 0.266 |
| Small read, no prewarm | Sync | 1.630 | 0.020 | 1.653 | 0.689 | 0.055 | 0.745 |
| Small half-prewarm read | Sync | 6.248 | 0.022 | 6.270 | 2.554 | 0.056 | 2.610 |
| Large half-prewarm read | Sync | 6.591 | 0.016 | 6.607 | 2.525 | 0.055 | 2.580 |

The paired median total-time ratios are 8.02× and 11.37× for sync/async small
creation, and 2.40× / 2.54× for small/large half-prewarm reads. The new TiKV async
mode is slower than its synchronous mode here. These loopback conditions do
not establish an async advantage at higher object-service latency.

Half-prewarm reads return 16 MiB of application data. BrewFS transfers median
972.80 / 1,022.03 MiB of successful GET bodies for small/large files, including
packed metadata, versus JuiceFS's 63.88 / 44.43 MiB. This measured amplification
and the 256 KiB authenticated frame geometry identify a concrete read-path
optimization target. The experiment does not isolate the causal share of CPU,
metadata RPCs and object transfers. Small creation uses 1,000 PUTs on both
clients; bulk creation uses 128 on BrewFS versus 32 on JuiceFS. The small-file
write gap therefore cannot be explained by PUT count alone.

Mixed read/write is duration-limited and completes different byte counts, so
use actual bytes divided by active-plus-drain time. A short total is not a win
if the client did substantially less work. Write latency and the following
fsync are separate calls and must be read together.

| Client | Mode | Effective MiB/s | Active s | Tail s | Read P95 ms | Write P95 ms | fsync P95 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| BrewFS | Sync | 0.496 | 10.510 | 0.021 | 9.503 | 375.390 | 1.581 |
| JuiceFS | Sync | 0.755 | 10.416 | 0.320 | 12.517 | 0.247 | 170.918 |
| BrewFS | Async | 0.432 | 10.546 | 0.022 | 9.503 | 291.504 | 1.581 |
| JuiceFS | Async | 1.028 | 10.324 | 49.314 | 10.027 | 0.313 | 9.110 |

JuiceFS's roughly 49-second async tail is included. Its effective throughput
still exceeds BrewFS's for this trace. No upload debt, timeout or teardown
assistance is omitted from these totals.

## Existing Redis-path guards

Both binaries were rebuilt from the same current main; only the candidate adds
this entity integration. Native/packed × sync/async small creation and mixed I/O
have five paired repetitions, as do packed half-prewarm small/large reads.
The predeclared investigation rule is a paired median cost increase above 2%
with at least four of five pairs worse, or an increase in teardown assistance.
Cost includes total time, P95, fsync P95, and mixed-I/O inverse effective
throughput plus separate read/write P95. This rule is not a statistical proof
of zero change.

No group triggers that rule. Small-creation total changes range from -0.81% to
+1.86%; half-prewarm totals change -0.21% / +0.20%. All four mixed-I/O effective
throughput comparisons have slightly favorable paired medians. Every cell
reports remote-ready; none needs ordinary-unmount retry or idle FUSE assistance.

Retain the less favorable observations: native/packed async small-creation
tails increase by medians of approximately 52/50 ms. Packed sync small-creation
fsync P95 has a +6.67% paired median with three of five pairs worse; the
independent medians are 1.317 to 1.370 ms, while total time changes -0.11%.
All five raw pairs and all other metrics are included in the numerical evidence.
Acceptance is limited to no material systematic regression in these tested
existing paths; no across-the-board latency improvement is claimed.

## Correctness and review

Local WSL compilation/correctness completed before cloud measurement:

- Workspace check/build, Tokio/io-uring checks, formatting, seven shell gates,
  strict all-target workspace Clippy, and 1,297 workspace lib/bin tests passed
  (264 tests ignored by that configuration).
- The focused suite passed 148 tests, with 45 external-fixture tests excluded
  from that invocation. The separate live-backend suite passed all 173 entity
  and eight packed tests, with none ignored.
- Six TiKV and six Redis FUSE cases passed, covering both lowers, sync/async
  writes and killed-process upload recovery. Concurrent live tests create
  1,000 files across eight writers while renewing leases in all four
  lower/upload-mode combinations.
- Ordered-lock-prefix and allocator counter-ownership regressions have valid
  red/green evidence. Synthetic acknowledgement loss is injected after actual
  commits and verifies no replay; caller cancellation and shutdown are covered.
- The opt-in SDK missing-primary fix passed 126 tests, with one separate live
  fixture ignored. All 107 SDK source/manifest/standalone-lock inputs and the
  test logs were revalidated by hash. The production dependency lock is unchanged.

Review checked key scope and size limits, one-snapshot predicate validation,
lease fencing after locks, atomic counter reservation, ordered retained locks,
rollback before any pre-commit retry, no replay of unknown commit outcomes,
owned-operation shutdown and default-off SDK behavior. No new actionable
correctness issue remained at publication. This is experimental admission,
not distributed-fault or production HA qualification.

To run the live contracts in a local isolated environment:

```sh
ENTITY_DATA_TIKV=true bash tools/testing/run_entity_data_tests.sh
cargo build --features workspace-overlay --example entity_metadata_probe
ENTITY_SMOKE_META=tikv \
  ENTITY_SMOKE_PROBE="$PWD/target/debug/examples/entity_metadata_probe" \
  bash tools/testing/run_entity_writeback_smoke.sh
```

See [usage and recovery](entity-writeback.md). The full cloud archive retains
the exact harness, plan, deterministic traces, configuration, host/backend
metrics, byte counters and all result/log files. The committed JSON exports
every numerical sample, including excluded pilots, without service credentials.
Candidate source hashes use LF normalization, matching the Linux release input.

| Identity | SHA-256 |
| --- | --- |
| Frozen Rust/Cargo source manifest | `9a0888c4ad3e9f123dfd2dd487a8211b687d334ca4e6ad1d4c4078093ae52a86` |
| Candidate production binary | `abf8d5e1b6e99dedd4bc19cc1c0f2682aa651885b0b8f81751ef9d0af8390e4f` |
| Baseline production binary | `c5df5ae8762ce9b16a52925cf633198720363ff77a94ad1f6752bd9e390a2615` |
| Complete R7b cloud archive | `fb60cd12bf7af14f47c226d68dc7be7dc3c5d2abf24336fb5c41bc274316bc98` |

Earlier rejected runs are not pooled into these measurements: R4 exposed a
lock-prefix issue; R6 exhausted the unlocked allocation counter CAS loop;
R7 stopped after 60 cells when JuiceFS's verification mount returned EBUSY on
ordinary unmount. R7b retains the validated Rust binaries and reruns all 214
cells with a bounded five-second ordinary-EBUSY unmount retry, recording every
attempt and including primary retry time in the tail. No force/lazy unmount
was added, and no R7b cell actually required a retry. Full failure archives
remain preserved. All task-owned cloud containers, volumes, VM and security
group were released after complete archive verification.
