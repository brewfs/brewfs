# Fragmented framed-object preparation

The candidate reserves the four-byte persisted header in the buffer used to
assemble a fragmented object. When compression is disabled or not beneficial,
that buffer is the upload object and an immutable `Bytes` slice of its body is
the raw cache entry. A single input buffer keeps the existing encoding path.
Compressible inputs still use the existing compressor and its size decision.

This removes a copy in `ObjectBlockStore` object preparation. It does not remove
VFS staging, native-P1's separate copy, backend/transport copies, or codec-internal
work. No FUSE reply or INIT setting changes.

## Evidence method

Baseline: `d73a611b73abbca194fe58425fed3f2e4d163881`, with test-only counters
at the actual concatenation and frame append sites and a store-level capture
test. The counters are compiled out of production builds. The test uses a
current-thread runtime because object preparation is synchronous and the
counters are thread-local. It resets and reads them around one store write,
before any read or verification work.

The same deterministic xorshift input and fragment boundaries are used before
and after. The capture backend writes through `LocalFsBackend`; each capture
reads the complete persisted `chunks-v2/81/0` object back from disk. This includes
the mandatory header. `tools/perf/compare_framed_objects.py` compares every byte
of each before/after capture, checks capture metadata, and reports the observed
copy counts. SHA-256 identifies the captures; hash equality is not substituted
for the byte comparison.

The cases cover fragmented 4 MiB raw input, disabled cache population,
create-only writes, one contiguous input, a sparse prefix and partial tail,
incompressible LZ4/Zstd fallback, and compressible LZ4/Zstd input. Header writes
are recorded separately from payload copies. Codec-internal and transport
copies are outside these counters.

## Measured results

All nine stored objects are byte-identical: **25,317,639 bytes compared**,
including every frame header. The actual comparison result and both measurement
manifests are in the evidence directory. No throughput gain is inferred from
these copy counters.

| Object case | Payload bytes copied before | Payload bytes copied after | Header bytes written before / after |
| --- | ---: | ---: | ---: |
| Raw 4 MiB, 32 fragments | 8,388,608 | 4,194,304 | 4 / 4 |
| Same, cache disabled | 8,388,608 | 4,194,304 | 4 / 4 |
| Same, create-only PUT | 8,388,608 | 4,194,304 | 4 / 4 |
| Raw 4 MiB, single fragment | 4,194,304 | 4,194,304 | 4 / 4 |
| 131,071-byte tail + 4,097-byte zero prefix | 270,336 | 135,168 | 4 / 4 |
| LZ4 incompressible fallback, 4 MiB | 8,388,608 | 4,194,304 | 4 / 4 |
| Zstd incompressible fallback, 4 MiB | 8,388,608 | 4,194,304 | 4 / 4 |
| LZ4 compressible, 4 MiB | 4,210,768 | 4,210,768 | 4 / 8 |
| Zstd compressible, 4 MiB | 4,194,451 | 4,194,451 | 4 / 8 |

The compressed cases write an additional four bytes for the raw assembly
buffer's reserved header. Their persisted compressed frames remain exactly
16,468 bytes (LZ4) and 151 bytes (Zstd), with the same bytes as the baseline.
For a fragmented raw 4 MiB object, measured payload copies plus header writes
change from 8,388,612 to 4,194,308 bytes.

## Cache ownership and accounting

The buffer is frozen into `Bytes` before either upload or cache publication.
There is no mutation or buffer reuse after that point. The cache receives only
the raw body, so reads and optional disk-cache persistence retain their existing
raw-data contract. Publication still occurs only after a successful upload.

`RecentWriteHotCache` charges logical payload length once per entry. The normal
read cache likewise already accepts raw views returned by the framed decoder.
The recent-write cache's replacement,
eviction, and removal paths use that same length. The four retained header bytes
are allocation overhead, not an extra logical cache entry. This change does not
turn the existing logical-byte cache counters into allocator-resident-byte
counters. The regression test checks shared pointers, inability to acquire a
mutable buffer while the cache owns it, stable bytes across eviction, and full
release of the cache charge and ownership.

## Validation

All Rust builds use `CARGO_INCREMENTAL=0 CARGO_PROFILE_TEST_DEBUG=0
CARGO_PROFILE_DEV_DEBUG=0`, `-j 2`, and a preceding `pgrep -a rustc` check.
Builds run serially on the 27 GiB host. The baseline capture test passed and the
shared-allocation regression failed at its pointer comparison: the raw cache
and framed upload had different backing allocations. The baseline copied
8,388,608 payload bytes and wrote four header bytes for a fragmented 4 MiB raw
object. The single-fragment control copied 4,194,304 payload bytes.

The candidate `chunk::` run passed 188 tests, with 0 failures and 3 ignored.
This filter also matches several `workspace_overlay::...::large_chunk::` tests.
The previously failing sharing test now passes, including charge replacement,
eviction, removal, and ownership release. Empty fragments, zero-length writes,
and magic-like user bytes also pass.
The complete `workspace_overlay::` run passed 1,138 tests, with 0 failures and
161 ignored; ignored external-backend tests are not counted as passes.

The first full workspace test attempt used the inherited shell proxy variables
and failed three local S3 mock-server tests (1,261 passed, 3 failed, 262 ignored).
Using the **same compiled workspace test binary**, the S3 module passed 14 tests
with 0 failures and 1 ignored after removing `HTTP_PROXY`, `HTTPS_PROXY`,
`ALL_PROXY` and their lowercase equivalents. Restoring the proxy environment
reproduced exactly the same three failures (11 passed, 3 failed, 1 ignored).
The full workspace gate was then rerun with those variables unset for the
validation process; no source change or test exclusion was made. The initial
failure and subsequent results are retained in the check history. A diagnostic
single-package Cargo build was canceled in favor of reusing the existing
workspace test binary for this control experiment.

The full workspace rerun passed **1,266 tests, 0 failures, 262 ignored**
(1,264 library tests and 2 binary tests). Workspace check/build and the
`fuse-tokio-runtime` / `fuse-io-uring-runtime` feature checks also passed.
Formatting, four perf-runner shell syntax checks, and all three required
perf-report shell tests passed; their individual exit codes are recorded in
`auxiliary-checks.json`. `rfuse3` is not a workspace member, so its conditional
feature matrix does not apply.

Both strict clippy runs passed: `cargo clippy -j 2 --workspace --all-targets --
-D warnings` and the same command with `--features workspace-overlay` before
`--`. The complete Rust command/exit-code history is in `ci-results.json`,
including the initial proxy-related test failure and the successful rerun.
The final staged diff also passed `git diff --cached --check`.

Before clippy, the build guard detected another checkout compiling against the
shared target directory. It refused to start another build, then resumed after
that compiler exited.

No matched release compose throughput/drain comparison was run within this
75-minute task. This is a measured copy reduction with correctness evidence;
broader performance acceptance remains pending, and the PR is left in draft.
No README performance comparison table was updated.

The exact baseline patch, measurements, and test result are retained in
[`evidence/single-alloc-framed-object/`](evidence/single-alloc-framed-object/).
Full local captures and build logs are preserved under
`/home/luxian/p1-alloc/evidence/`.

## Reproduction

Use separate baseline and candidate checkouts, and run all builds serially.
Apply `evidence/single-alloc-framed-object/baseline-instrumentation.patch` to
baseline commit `d73a611b73abbca194fe58425fed3f2e4d163881`. The patch adds only
test instrumentation and the original store evidence/regression tests; its
shared-allocation test is expected to fail on the baseline.

In each checkout, choose a different absolute capture directory and run:

```bash
pgrep -a rustc  # Wait for any existing compiler/build to finish.
CARGO_INCREMENTAL=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_PROFILE_DEV_DEBUG=0 \
BREWFS_FRAMED_OBJECT_EVIDENCE_DIR=/absolute/path/to/before-or-after \
  cargo test -j 2 --lib --features workspace-overlay \
  chunk::store::framed_write_tests:: -- --nocapture
```

From the candidate checkout, compare the actual captures:

```bash
python3 tools/perf/compare_framed_objects.py \
  /absolute/path/to/before /absolute/path/to/after
```
