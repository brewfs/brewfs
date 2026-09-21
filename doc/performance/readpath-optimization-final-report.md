# BrewFS Read-Path Optimization — Final Report

## Date

2026-09-21

## Scope

End-to-end read-path performance improvement for BrewFS, measured against
JuiceFS as the production reference. This report covers the optimization
attempts made in this iteration, the evidence for accepted and rejected
changes, current blockers, and prioritized next steps.

> Update: section 10 records the PR #135 investigation completed on
> 2026-09-21. It supersedes earlier claims in this document about page-cache
> behavior, asyncfuse pre-posting, and the remaining read-path bottleneck.

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

---

## 10. PR #135 Read-Path Investigation (2026-09-21)

### 10.1 Request size is a mode effect, not an 819.2 KiB negotiation

The test host runs Linux
`6.18.33.2-microsoft-standard-WSL2`. Two repeatable FUSE request shapes were
observed:

| BrewFS mount mode | Kernel FUSE reads for 4 GiB | Request shape | Average |
|---|---:|---|---:|
| Buffered (`READ_DIRECT_IO=0`, `KEEP_CACHE=1`) | 16,384 | 256 KiB each | 256 KiB |
| Direct read (`READ_DIRECT_IO=1`) | 5 requests per 4 MiB fio I/O | 4 x (1,048,576 - 16) bytes + 64 bytes | 819.2 KiB |

The `819.2 KiB` value is therefore only `4 MiB / 5`; it is not a value
negotiated in FUSE INIT. Direct read avoids the buffered path's 16-way split,
but it also bypasses the Linux page cache. BrewFS then relies on its own block
cache. The buffered A/B proves that the kernel page cache works; the benchmark
remounts and evicts cold pages between bigread repeats, so the first pass cannot
benefit from retained kernel pages.

### 10.2 asyncfuse INIT compatibility finding

asyncfuse 0.1.14 correctly echoes `FUSE_MAX_PAGES` and writes
`max_pages = u16::MAX`; Linux still caps observed direct-read requests near
1 MiB. Raising `max_pages` further is therefore not the solution.

There is a separate ABI correctness issue in asyncfuse:

- its `fuse_init_out` defines `max_read: u32` after `flags2`;
- current Linux `struct fuse_init_out` defines that same slot as part of
  `unused[7]` and has no `max_read` field;
- asyncfuse sets `flags2 = 0x1` and labels it `FUSE_HAS_MAX_READ`, but bit 0 of
  `flags2` is protocol bit 32, which Linux defines as `FUSE_SECURITY_CTX`.

This should be fixed in asyncfuse as a protocol/ABI cleanup. It does not
explain the near-1 MiB Linux cap and should not be used to claim that a 4 MiB
read limit was negotiated.

### 10.3 Accepted PR #135 implementation

The accepted implementation optimizes the physical read below FUSE rather
than pretending that the kernel request split is gone:

- a persistent-slice read within 4 KiB of the 1 MiB threshold promotes the
  complete 4 MiB block into BrewFS's memory cache;
- concurrent ranges for the same block share the existing
  `SingleFlight<BlockKey, Bytes>`, so promotion performs one physical read;
- genuinely small reads remain ranged and do not amplify to 4 MiB;
- persistent-slice file descriptors are reused;
- after a slice is opened, later blocks prefer it over repeated failed probes
  of the ordinary disk cache;
- new counters report physical persistent-slice operations and bytes.

The rejected reader-session prefetch experiment must not be restored. It
reduced bigread to about 2.28 GiB/s and amplified randread to 6,761 physical
reads / 26.4 GiB for a 2 GiB dataset. After reverting it, randread returned to
exactly 512 physical reads / 2 GiB.

### 10.4 A/B evidence

| Experiment | bigread result | Other evidence | Decision |
|---|---:|---|---|
| Full-block promotion, stable focused run (`perf-run-1789973330-21674`) | 4,271 MiB/s median (4,205-4,381; 4.1% spread) | randread 13.04 GiB/s | Keep |
| Regression-fixed focused run (`perf-run-1789975061-15419`) | 3,897 MiB/s median | randread 12.78 GiB/s; 512 reads / 2 GiB | Keep |
| Opened-slice priority enabled | about 4.00 GiB/s | about 64 ordinary cache misses | Keep |
| Opened-slice priority disabled | about 3.72 GiB/s | 1,024 ordinary cache misses | Reject |
| Buffered page cache (`perf-run-1789975826-23215`) | 4,068 MiB/s median; 0.4% spread | 16,384 FUSE reads at 256 KiB; 4 GiB physical | Valid control, not the default |
| Reader-session prefetch | about 2.28 GiB/s | randread 6,761 reads / 26.4 GiB | Reject |

### 10.5 CPU profile and remaining bottleneck

The seqread profile at
`tools/perf/results/20260921-154205` reached about 3.73 GiB/s. Its largest leaf
costs are `__memmove_avx_unaligned_erms` (about 27%),
`__memset_avx2_unaligned_erms` (about 12%), `__pi_memcpy`, and kernel
`fuse_copy_fill` / `fuse_copy_folio`. The dominant application stack is:

```text
ChunksCache::get_range_into_memory
  -> ObjectBlockStore::read_range
  -> DataFetcher::read_at_into_from_slices
  -> FileReader::read_at
  -> VFS::read
  -> asyncfuse reply writev
```

Crypto accounted for 0% in this profile. Redis, RustFS, compression, and
encryption are not the limiting factors in this local cached-read case. The
remaining gap is primarily buffer allocation/zeroing, repeated memory copies,
and the unavoidable FUSE kernel copy. A future optimization should target an
owned-buffer or vectored-reply path before adding more speculative prefetch.

### 10.6 Final local Redis + RustFS matrix

Artifacts:

- BrewFS: `perf-run-1789977274-2588`
- JuiceFS: `juicefs-perf-run-1789977506-26084`

Both sides used the same fio workload parameters and all 11 tools passed.
Read figures are fio foreground throughput; write figures show foreground and
fully-drained throughput separately.

| Workload | BrewFS | JuiceFS | Interpretation |
|---|---:|---:|---|
| fio-bigread | 4,214 MiB/s | 3,984 MiB/s | BrewFS +5.8%; BrewFS spread 11.8%, JuiceFS 0.1% |
| fio-seqread | 3,723 MiB/s | 2,862 MiB/s | BrewFS +30.1% |
| fio-randread | 12,789 MiB/s | 3,230 MiB/s | BrewFS 3.96x; hot-cache semantics dominate |
| fio-bigwrite | 1,006 / 451 MiB/s | 532 / 63 MiB/s | BrewFS drains much faster |
| fio-seqwrite | 694 / 579 MiB/s | 1,085 / 310 MiB/s | JuiceFS foreground faster; BrewFS durable faster |
| fio-randwrite | 517 / 369 MiB/s | 956 / 343 MiB/s | Similar durable result after drain |
| fio-randrw read/write | 743 / 338 MiB/s | 889 / 397 MiB/s | JuiceFS foreground faster; BrewFS drained total 601 vs 408 MiB/s |
| dirstress | pass (1 s) | pass (<1 s) | Functional fallback workload |
| dirperf | pass (1 s) | pass (2 s) | Functional fallback workload |
| metaperf | pass (1 s) | pass (1 s) | Functional fallback workload |
| looptest | pass (2 s) | pass (1 s) | Functional fallback workload |

The full matrix takes several minutes even though individual reads are fast.
The main wall-time cost is strict durability accounting: JuiceFS post-write
drain took 57 s for bigwrite, 50 s for seqwrite, 36 s for randwrite, and 51 s
for randrw. These waits are intentional and prevent background writeback from
inflating the reported durable throughput.

The local randread number should not be extrapolated to cloud cold-read
performance: BrewFS promotes the complete 2 GiB working set into its own memory
cache, while the two filesystems have different cache-layer semantics. Cloud
results with managed OSS/Tair remain the authoritative cold-backend comparison.

### 10.7 Final Aliyun managed-backend matrix

The final candidate was commit `3d0575c` (binary SHA-256
`3fee18c3b4f59fba34bddc7704e45b6166fee6caadf7fa048ef69edcc1e670b6`).
BrewFS and JuiceFS ran on matched temporary ECS instances with PL2 data disks,
managed Tair, and same-region OSS. Both 11-tool legs passed, and all temporary
ECS instances, data disks, buckets, binary runs, and Tair resources were deleted
afterward.

Result Vault runs:

- BrewFS: `run-20260921-163938-673b15dc`
- JuiceFS: `run-20260921-165717-d9261e32`

Local extracted artifacts:

- BrewFS: `docker/compose-xfstests/artifacts/aliyun-analysis/pr135-3d0575c-promotion-full/brewfs/perf-run-1789979359-17831-brewfs-brewfs-pr135-3d0575c-promotion-full`
- JuiceFS: `docker/compose-xfstests/artifacts/aliyun-analysis/pr135-3d0575c-promotion-full/juicefs/perf-run-1789980030-29925-juicefs-juicefs-pr135-3d0575c-promotion-full`

Foreground fio throughput:

| Workload | BrewFS | JuiceFS | BrewFS relative |
|---|---:|---:|---:|
| fio-bigread | 322.6 MiB/s | 328.3 MiB/s | -1.7% |
| fio-seqread | 2,371.8 MiB/s | 1,211.7 MiB/s | +95.7% |
| fio-randread | 4,935.2 MiB/s | 1,337.2 MiB/s | +269.1% |
| fio-bigwrite | 1,039.6 MiB/s | 688.6 MiB/s | +51.0% |
| fio-seqwrite | 318.4 MiB/s | 530.4 MiB/s | -40.0% |
| fio-randwrite | 303.6 MiB/s | 571.0 MiB/s | -46.8% |
| fio-randrw read / write | 332.1 / 148.6 MiB/s | 328.2 / 146.0 MiB/s | +1.2% / +1.8% |

Foreground write bandwidth includes queued writeback. Fully drained throughput,
which includes the time needed to empty the filesystem queue, reverses the
seqwrite and randwrite result:

| Workload | BrewFS | JuiceFS | Drain seconds (BrewFS / JuiceFS) |
|---|---:|---:|---:|
| fio-bigwrite | 205.4 MiB/s | 186.6 MiB/s | 4 / 4 |
| fio-seqwrite | 308.1 MiB/s | 181.9 MiB/s | 2 / 115 |
| fio-randwrite | 293.8 MiB/s | 185.2 MiB/s | 2 / 125 |
| fio-randrw total | 450.7 MiB/s | 190.3 MiB/s | 4 / 90 |

Against the previous stable direct-read BrewFS cloud run
(`perf-run-1789967854-12390`), the block-promotion candidate improved every
target read workload under the same cold-read sampling contract:

| Workload | Before promotion | After promotion | Delta |
|---|---:|---:|---:|
| fio-bigread | 309.1 MiB/s | 322.6 MiB/s | +4.4% |
| fio-seqread | 1,564.7 MiB/s | 2,371.8 MiB/s | +51.6% |
| fio-randread | 3,186.5 MiB/s | 4,935.2 MiB/s | +54.9% |

The physical-read counters show why the long-running workloads improve more
than the one-shot cold bigread:

| Workload | Logical FUSE read | Persistent-slice reads | Physical bytes | Physical / logical |
|---|---:|---:|---:|---:|
| fio-bigread | 1,024 MiB / 1,280 ops | 256 ops | 1,024 MiB | 100.0% |
| fio-seqread | 142,312 MiB | 256 ops | 1,024 MiB | 0.72% |
| fio-randread | 296,124 MiB | 3,601 ops | 14,404 MiB | 4.86% |

For bigread, promotion reduces five near-1 MiB FUSE requests per 4 MiB block to
one 4 MiB persistent-slice operation without reading extra bytes. The remaining
approximately 320 MiB/s ceiling is the cold PL2 data-disk bandwidth: JuiceFS is
at the same ceiling. For seqread and randread, the promoted blocks are reused in
BrewFS memory, so only 0.72% and 4.86% of logical bytes respectively reach the
persistent slice.

The three measured bigread samples were `7111.1 / 321.6 / 322.6 MiB/s` for
BrewFS and `327.7 / 328.3 / 329.7 MiB/s` for JuiceFS. BrewFS's first measured
sample inherited the promotion cache populated by the warmup; the later samples
were remounted and cold. Median selection therefore chose the cold `322.6 MiB/s`
sample. This explains the large reported BrewFS spread and prevents the hot
sample from inflating the comparison.

