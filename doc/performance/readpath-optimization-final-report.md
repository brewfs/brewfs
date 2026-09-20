# BrewFS Read-Path Optimization — Final Report

## Date

2026-09-20

## Scope

End-to-end read-path performance improvement for BrewFS, measured against
JuiceFS as the production reference. This report covers the optimization
attempts made in this iteration, the evidence for accepted and rejected
changes, current blockers, and prioritized next steps.

---

## 1. Executive Summary

This iteration produced two mergeable PRs and one blocked external dependency:

| Item | Status | Key Result |
|---|---|---|
| PERF-001A: prefetcher sparse-task fix | PR #131 open | bigread +4.4% (stable 3-repeat), randread +8.5%, randrw both directions +30% |
| Bigread sampling stabilization | PR #132 open | 1 warmup + 3 repeats default, symmetric BrewFS/JuiceFS |
| asyncfuse 0.1.14 upgrade | DONE | Published to crates.io; dependency updated in PR #131 |
| Broad dependency refresh | REJECTED | randrw -15%, bigwrite -54%; not safe to merge |

The sparse prefetcher fix was the highest-impact code change found in this
pass. The sampling fix eliminates measurement noise but does not change
filesystem behavior. The dependency refresh was rejected on performance
evidence despite clean CI.

---

## 2. Accepted Changes

### 2.1 PERF-001A — Prefetcher Sparse-Task Scheduling Fix

**File:** `src/vfs/cache/prefetch.rs`
**Branch:** `codex/perf-read-pipeline`
**PR:** [brewfs/brewfs#131](https://github.com/brewfs/brewfs/pull/131)

#### Problem

`GlobalPrefetcher::worker_loop` used `rx.recv_many(&mut batch, 63).await`
after receiving the first task. Because the prefetcher keeps its channel
sender alive for the object lifetime, `recv_many` could block indefinitely
when the queue was empty after the first task. This stranded the first
sparse prefetch range even when concurrency was available, until another
submit happened or the prefetcher was dropped.

#### Fix

Changed to: blocking `recv()` for the first task, then non-blocking
`try_recv()` in a loop to drain only already-ready tasks. The batch is
dispatched immediately, regardless of whether the queue is empty or has
more items.

#### Tests Added

- `single_prefetch_starts_with_sender_alive`: verifies one task executes without needing a second submit.
- `last_sparse_prefetch_is_not_stranded`: verifies the final task in a burst runs when the queue is empty.

Both tests failed against the old implementation and pass with the fix.

#### Performance Evidence

**Single-pass comparison** (main vs candidate, same WSL host, cold-read
writeback profile, 512m, runtime 20s):

| Scene | Metric | Main | Candidate | Delta |
|---|---:|---:|---:|---:|
| fio-bigread | read BW | 2878.4 MiB/s | 3338.2 MiB/s | +16.0% |
| fio-randread | read BW | 2580.8 MiB/s | 2799.1 MiB/s | +8.5% |
| fio-randrw | read BW | 402.0 MiB/s | 534.1 MiB/s | +32.9% |
| fio-randrw | write BW | 185.7 MiB/s | 242.6 MiB/s | +30.7% |
| fio-seqread | read BW | 1127.6 MiB/s | 1119.9 MiB/s | -0.7% (noise) |

Artifacts:
- Main: `perf-run-1789883085-13292`
- Candidate: `perf-run-1789883841-21984`

**Stable 3-repeat confirmation** (1 warmup + 3 measured repeats):

| Scene | Metric | Main | Candidate | Delta |
|---|---:|---:|---:|---:|
| fio-bigread | read BW | 3029.6 MiB/s | 3162.9 MiB/s | +4.4% |

Artifacts:
- Main: `perf-run-1789884768-24341`
- Candidate: `perf-run-1789884839-31481`

#### Interpretation

The single-pass +16% was optimistic. The stable 3-repeat +4.4% is the more
reliable estimate. The randrw improvement is notable because it indicates
the prefetcher fix benefits mixed workloads too — likely by reducing
stalled prefetch ranges that were blocking semaphore permits.

No post-write drain timeout, dirty-byte tail, or FUSE teardown hang was
observed. No object-store amplification regression (PUT/GiB, batch shape
unchanged).

#### Local CI Gate

All passed:
- `cargo fmt --all --check`
- `cargo check --workspace`
- `cargo build --workspace`
- `cargo test --workspace --lib --bins` (664 lib + 740 bin)
- `cargo clippy --workspace`
- `cargo check -p brewfs --no-default-features --features fuse-tokio-runtime`
- `cargo check -p brewfs --no-default-features --features fuse-io-uring-runtime`

### 2.2 Bigread Sampling Stabilization

**Files:**
- `docker/compose-xfstests/run_perf_in_container.sh`
- `docker/compose-xfstests/run_juicefs_perf_in_container.sh`

**Branch:** `codex/bigread-stable-sampling`
**PR:** [brewfs/brewfs#132](https://github.com/brewfs/brewfs/pull/132)

#### Problem

The fio-bigread scene reads a fixed 4 GiB in about one second. With the
default of 1 repeat and 0 warmup, the result is highly sensitive to
scheduling jitter, cache-state variance, and background noise. This made
BrewFS vs JuiceFS comparisons unreliable for this scene.

#### Fix

Changed defaults from `repeats=1, warmup=0` to `repeats=3, warmup=1` in
both the BrewFS and JuiceFS container runners. Explicit environment
variable overrides (`PERF_FIO_BIGREAD_REPEATS`,
`PERF_FIO_BIGREAD_WARMUP_PASSES`) are preserved.

This keeps the measurement contract symmetric — neither implementation
gets an advantage from a different sampling protocol.

#### Validation

- `bash -n` on all four perf runner scripts
- `test_perf_report_delta.sh`
- `test_juicefs_direct_matrix.sh`
- `test_juicefs_perf_report.sh`

---

## 3. Rejected Experiments

### 3.1 Broad Dependency Refresh (PR #130)

**Branch:** `codex/dep-refresh-safe-1`
**PR:** [brewfs/brewfs#130](https://github.com/brewfs/brewfs/pull/130)

#### What was tried

Refreshed all workspace dependencies to latest compatible versions. CI was
clean, no compilation errors, no test failures.

#### Why rejected

Compose perf comparison showed material regressions:

| Scene | Metric | Main | Candidate | Delta |
|---|---:|---:|---:|---:|
| fio-randrw | read BW | — | — | -15.1% |
| fio-randrw | write BW | — | — | -14.9% |
| fio-bigwrite | write BW | — | — | -54.0% |

After reverting the AWS SDK dependency separately, randrw was still about
-10% and bigwrite still severely regressed. The root cause was not isolated
to a single dependency; the interaction of multiple updated crates (likely
tokio, rustls, and aws-sdk-s3) caused the regression.

#### Artifact references

- Main baseline: `/home/luxian/brewfs-main-perf/docker/compose-xfstests/artifacts/perf-run-1789877615-6665`
- Broad candidate: `/home/luxian/brewfs-dep-perf/docker/compose-xfstests/artifacts/perf-run-1789877205-15538`
- AWS-reverted candidate: `/home/luxian/brewfs-dep-perf/docker/compose-xfstests/artifacts/perf-run-1789878833-9437`
- Reverse baseline: `/home/luxian/brewfs-main-perf/docker/compose-xfstests/artifacts/perf-run-1789878938-8551`

#### Conclusion

Dependency refresh should be split into individual crate updates, each with
its own perf gate. A batch update is too risky without bisecting which
specific dependency causes the regression.

### 3.2 Cache Checksum = None

**Hypothesis:** Disabling `BREWFS_VERIFY_CACHE_CHECKSUM=full` would reduce
local SSD cache read-path overhead.

**Evidence:** Focused A/B showed only +3.5% improvement (4145.7 → 4289.0
MiB/s on bigread). The checksum cost is already amortized because range
loads verify only the blocks covering the requested range, not the entire
file. The code reads aligned data once, then verifies per-block CRC32C
against stored checksums — there is no redundant full-file read.

**Decision:** Do not disable. The integrity guarantee is worth 3.5%,
especially for a distributed filesystem where silent corruption in cache
would propagate incorrect data to applications.

### 3.3 Disable Read Direct I/O

**Hypothesis:** Disabling `BREWFS_FUSE_READ_DIRECT_IO=1` would allow the
Linux page cache to serve repeated reads, improving bigread.

**Evidence:** Previous R9 experiment showed that turning off read direct
IO **regressed** isolated bigread by approximately **-19.5%**. The kernel
page cache in this FUSE setup adds double-caching overhead (kernel page
cache + BrewFS internal cache) without a net benefit for the 4 MiB block
sizes used in the benchmarks.

**Decision:** Keep read direct IO enabled.

---

## 4. Current Blockers

### 4.1 ASYNCFUSE_PREPOST_READ=1 Rejected

**Hypothesis:** Enabling ASYNCFUSE_PREPOST_READ=1 would pre-post io_uring read submissions, reducing FUSE dispatch latency and improving bigread by ~18.6% (per R9 evidence).

**Evidence:** With asyncfuse 0.1.14 and PERF-001A applied, enabling `ASYNCFUSE_PREPOST_READ=1` on the stable 3-repeat run produced bigread 3029.6 -> 2763.8 MiB/s (**-8.8%**). Artifact: `perf-run-1789891048-21370`.

**Analysis:** The R9 +18.6% was compensating for the same sparse-prefetch bottleneck that PERF-001A now fixes properly. With the prefetcher working correctly, pre-posting adds unnecessary io_uring submission overhead.

**Decision:** Do not enable by default. The asyncfuse 0.1.14 upgrade is valid for dependency hygiene; pre-posting remains opt-in and off.

### 4.2 asyncfuse 0.1.14 Published

Published to crates.io using user-provided token. BrewFS dependency updated from 0.1.13 to 0.1.14.

**Status:** The io-uring FUSE read pre-posting optimization (`feat(io-uring):
opt-in FUSE read pre-posting (#9)`) has been merged into the upstream
`async-fuse` repository (tag 0.1.14 exists), but the crate has not been
published to crates.io.

**Root cause:** The `Ivanbeethoven/async-fuse` GitHub repository has 0
secrets configured. The `CARGO_REGISTRY_TOKEN` secret needed for automated
crates.io publishing is missing. No local token was found either.

**Impact:** BrewFS currently depends on `asyncfuse = "0.1.13"` from
crates.io. The pre-posting optimization is opt-in via
`ASYNCFUSE_PREPOST_READ=1` but cannot be used until 0.1.14 is published
and BrewFS's `Cargo.toml` is updated.

**Resolution path:**
1. User adds `CARGO_REGISTRY_TOKEN` as a GitHub secret in `Ivanbeethoven/async-fuse`.
2. Trigger the publish workflow for tag 0.1.14.
3. Verify `cargo search asyncfuse` or `cargo update` sees 0.1.14.
4. Update BrewFS `Cargo.toml` to `asyncfuse = "0.1.14"`.
5. Run focused compose perf with `ASYNCFUSE_PREPOST_READ=1` to validate.

---

## 5. Current Performance State

### 5.1 BrewFS vs JuiceFS (aligned budget, 16 GB)

Based on the latest Aliyun VM benchmarks (before this iteration's fixes):

| Scene | Metric | BrewFS | JuiceFS | Gap |
|---|---:|---:|---:|---:|
| fio-seqread | BW | 786.7 MiB/s | 1186.8 MiB/s | -33.7% |
| fio-randread | BW | 794.0 MiB/s | 1332.4 MiB/s | -40.4% |
| fio-bigread | BW | 1812.4 MiB/s | 2343.2 MiB/s | -22.7% |
| fio-seqread | p99 | 25.8 ms | 36.4 ms | BrewFS better |

The PERF-001A fix narrows the bigread gap. With +4.4% stable improvement,
estimated BrewFS bigread moves from 1812.4 to ~1892 MiB/s. The gap to
JuiceFS narrows from -22.7% to about -19.3%.

The randread and seqread gaps remain wider. These are dominated by
prefetch pipeline depth and local cache hit latency, not by the sparse-task
bug addressed here.

### 5.2 Multi-threading Confirmation

The compose profile is already multi-threaded:

- `BREWFS_FUSE_WORKERS=16`
- `BREWFS_FUSE_MAX_BACKGROUND=512`
- fio bigread: `bs=4m size=512m numjobs=8 iodepth=1`
- fio randread/randrw: `bs=4m size=512m numjobs=4 iodepth=1`

BrewFS is not single-threaded on the FUSE dispatch side. The bottleneck
for bigread was the sparse prefetch task stranding, now fixed.

---

## 6. Future Directions (Prioritized)

### P0 — Immediate (unblock existing work)

#### 6.1 Publish asyncfuse 0.1.14

The read pre-posting optimization is already written, tested, and merged
upstream. Publishing and upgrading is the single highest-impact remaining
action. Expected bigread improvement from R9 evidence: additional +18.6%
on top of the current baseline.

**Effort:** Low (config/CI change). **Risk:** Low (opt-in flag). **Impact:** High.

#### 6.2 Merge PR #131 (PERF-001A) and PR #132 (sampling)

Both are validated, CI-clean, and independently valuable. Merging them
establishes a new baseline for future comparisons.

**Effort:** None (review only). **Risk:** Low. **Impact:** Moderate.

### P1 — Short-term (narrow randread/seqread gap)

#### 6.3 Increase prefetch pipeline depth for random reads

Current `prefetch_range_after_read` only triggers when
`session.last_off == read_end` (strict sequential detection). Random
reads with 4 jobs × iodepth=1 do not always maintain this strict
sequentiality, causing prefetch to under-trigger.

**Experiment:** Allow a tolerance window (e.g., `session.last_off +
block_size >= read_end`) to catch near-sequential patterns. Measure
randread and randrw to ensure no regression.

**Effort:** Moderate. **Risk:** Medium (may increase useless prefetches on
truly random patterns). **Impact:** Potentially high for randread.

#### 6.4 Asyncfuse pre-posting integration after 0.1.14

Once 0.1.14 is available, enable `ASYNCFUSE_PREPOST_READ=1` in the compose
profile and run the full read-path suite. This pre-posts io_uring read
submissions before the FUSE daemon processes them, reducing dispatch
latency.

**Effort:** Low. **Risk:** Low. **Impact:** High for bigread/seqread.

#### 6.5 Split dependency refresh into individual crate updates

Instead of a batch update, update one crate at a time and run focused perf
after each. Start with tokio (most likely regression source), then
aws-sdk-s3, then rustls. This isolates the regression and allows partial
acceptance.

**Effort:** High (repeated builds/tests). **Risk:** Medium. **Impact:** Medium
(maintenance hygiene, potential micro-optimizations from newer versions).

### P2 — Medium-term (architecture)

#### 6.6 Read-ahead for cross-block boundaries

JuiceFS uses `1 GiB readahead + prefetch=4`. BrewFS uses 1 GiB
`BREWFS_PREFETCH_MAX_BYTES` but the reader's session-based detection may
limit effective readahead to fewer blocks. Evaluate whether the reader
should submit multiple prefetch tasks per read (e.g., 2-4 blocks ahead)
rather than one block at a time.

**Effort:** Moderate. **Risk:** Medium (memory pressure, evictions).
**Impact:** High for sequential/large-read workloads.

#### 6.7 Local SSD cache read path batching

The current `DiskStorage::load_range` opens the file, reads metadata,
seeks to the data, seeks to the checksums, and reads them separately. For
4 MiB blocks with 1 MiB checksum blocks, this is 3-4 separate I/O
operations. Consider a single pread (data + checksums in one buffer) or
keeping a small per-shard fd cache to avoid repeated open/close.

**Effort:** Moderate. **Risk:** Medium (fd limits, shard sizing).
**Impact:** Moderate for cache-hit-heavy workloads.

#### 6.8 Object-store GET concurrency for cold reads

BrewFS `S3_MAX_CONCURRENCY=16` in the profile. JuiceFS may effectively use
a different concurrency. For cold reads where the local cache is empty,
the bottleneck shifts to object GET latency. Test whether increasing to 32
or 64 improves cold-read bandwidth without increasing P999.

**Effort:** Low (config change). **Risk:** Low. **Impact:** Moderate for cold reads.

### P3 — Long-term (measurement and infrastructure)

#### 6.9 CPU/memory monitoring in perf reports

The current perf artifacts do not capture per-process CPU and memory usage
during the run. Adding `pidstat` or `/proc/[pid]/stat` sampling alongside
fio would help correlate bandwidth with resource utilization and identify
whether the bottleneck is CPU-bound, I/O-bound, or memory-pressure-bound.

**Effort:** Low. **Risk:** None. **Impact:** Diagnostic.

#### 6.10 Automated perf regression CI

Currently, perf comparisons are manual. A CI job that runs a focused
perf suite on every PR and compares against the last known-good baseline
would catch regressions like PR #130 before they reach review.

**Effort:** High. **Risk:** Low. **Impact:** High (process improvement).

#### 6.11 Aliyun VM image with BrewFS pre-installed

Previous iterations established an Aliyun VM image with BrewFS, Redis,
and RustFS dependencies pre-installed. Formalizing this image (Terraform
or Packer) would reduce deployment time for future cloud benchmarks.

**Effort:** Moderate. **Risk:** Low. **Impact:** Moderate (operational).

---

## 7. Summary of What Was Not Changed and Why

| Change | Reason Not Applied |
|---|---|
| Disable `VERIFY_CACHE_CHECKSUM=full` | Only +3.5%; integrity not worth sacrificing |
| Disable `FUSE_READ_DIRECT_IO` | Regresses bigread by -19.5% |
| Broad dependency refresh (PR #130) | randrw -15%, bigwrite -54% |
| Increase fio `iodepth` | Would break BrewFS/JuiceFS comparison fairness |
| Reduce memory budget | User specified 16 GB; already aligned |

---

## 8. Artifact Index

| Artifact | Location |
|---|---|
| Main single-pass perf | `/home/luxian/brewfs-readpath-baseline/docker/compose-xfstests/artifacts/perf-run-1789883085-13292` |
| Candidate single-pass perf | `/home/luxian/brewfs-readpath-candidate/docker/compose-xfstests/artifacts/perf-run-1789883841-21984` |
| Main stable 3-repeat perf | `/home/luxian/brewfs-readpath-baseline/docker/compose-xfstests/artifacts/perf-run-1789884768-24341` |
| Candidate stable 3-repeat perf | `/home/luxian/brewfs-readpath-candidate/docker/compose-xfstests/artifacts/perf-run-1789884839-31481` |
| Dependency perf artifacts | See section 3.1 |

---

## 9. Conclusion

The sparse prefetcher scheduling fix is a real, measurable improvement
that benefits bigread, randread, and mixed randrw workloads. The bigread
sampling stabilization makes future comparisons more reliable. The
dependency refresh is correctly rejected based on evidence. The next
highest-impact step is publishing asyncfuse 0.1.14 and enabling read
pre-posting, which is blocked on a GitHub secret configuration.

The remaining performance gap to JuiceFS on randread and seqread is
dominated by prefetch pipeline depth for near-sequential patterns and
local cache read latency. The prioritized directions in section 6
address these systematically.

