# Independent EC write hint and sparse fetch policy

`--read-fetch-policy sparse` selects page/range reads for small uncompressed
misses in the ordinary `ObjectBlockStore`, including when an EC write hint is
configured. It also disables both range-triggered speculative whole-object
prefetch paths. `--read-fetch-policy whole-object` explicitly selects whole
objects, with or without a hint. Omitting the option selects `backend-default`
and preserves the previous decisions, including the EC hint's whole-object
default. The CLI overrides YAML `cache.read_fetch_policy`.
SDK callers can use `.with_read_fetch_policy(ReadFetchPolicy::Sparse)` on the
store; existing `BlockStoreConfig` constructors keep their source compatibility.

```yaml
data:
  backend: s3
  s3:
    rustfs_ec_block_size_hint: 262144
cache:
  compression: none
  read_fetch_policy: sparse
```

The hint still requires the `rustfs-ec-block-size-hint` Cargo feature and a
compatible server. The read policy is available without that feature. Write
encoding, object boundaries, EC hint selection, multipart geometry, FUSE reply
ABI, and INIT maxima are unchanged. Tests compare complete persisted frames
under all three policies for raw, LZ4, and Zstd writes.

## Read behavior and contracts

The existing 64 KiB page cache, range coalescing, block cache admission, and
write-cache behavior remain in place. A small read joins an existing whole
download, including while that download waits for cache publication. Sparse
mode still promotes fully populated pages to the block cache under the existing
promotion conditions; it suppresses the subsequent speculative full GET.

The existing size threshold selects whole-object reads for large requests.
Explicit policies classify these full responses from their own headers, so a
cold complete-object scan retains one GET. Compressed objects still require a
whole download and frame decode; the current mount's write compression is not
used to reinterpret a versioned object's encoding. A cold sparse format cache
needs a four-byte header probe before its payload request. Cached pages remain
usable after the independent format cache evicts their layout.

This policy belongs to `ObjectBlockStore`. It does not change the S3 adapter's
exact range API, packed authenticated frame readers, or Native-P1's verified
whole-object repository. The authenticated paths retain their length and digest
checks. Partial tails are checked for complete requested coverage; a read past
the persisted end remains an error.

The complete-scan guard below requests a complete 4 MiB object at once. Sparse
mode does not infer sequential access from a series of tiny requests: such a
fragmented scan can issue additional range GETs. End-to-end FUSE scan and mixed
read/write performance acceptance remain separate work; this experiment does
not establish their non-regression. No general performance comparison table is
updated from these measurements.

## Measurement method

The dedicated compose fixture uses four EC directories on one local Docker
disk, with RustFS's physical-disk check disabled for this test. This is an HTTP
body experiment, not a replication of the supplied real-hardware EC hint A/B.
The installed image and health response are recorded in `environment.json`.
Persisted geometry is decoded from each object's `V2Obj.EcBSize` in all four
`xl.meta` copies. User metadata is not used as layout evidence.

Both arms use the same objects, configured 262,144-byte write hint, same client,
8 MiB block-cache budget, default 64 KiB page cache, and no disk read cache.
Each sample constructs fresh client data caches; server caches stay warm. The
format-warm cases resolve only the four-byte header before measurement. Those
warmup bytes are excluded from the miss measurement in both arms. The cold
format case includes the probe. No FUSE/kernel cache is involved.

Each of three trials contains 64 isolated misses and a repeat read after each
miss. Trial order is default/sparse, sparse/default, default/sparse. The report
takes the median of each trial statistic. HTTP attempts and consumed response
body bytes come from the SDK connector/body observer; headers and TCP/TLS bytes
are excluded. Repeated-read cache hits and HTTP traffic are recorded separately.
Throughput is logical requested MiB divided by summed miss latency. p95/p99 are
nearest-rank request latencies. The executable is an unoptimized Rust test build
with debug information disabled. Timing is local diagnostic evidence.

The 4 MiB raw payload is a 4,194,308-byte stored object including its unchanged
four-byte frame header. A 65,536-byte known-format fetch would reduce body bytes
by about 64 times; that ratio is not a throughput multiplier.

## Measured results and acceptance status

The [full medians table](evidence/ec-sparse-fetch/results.md) and
[raw trials](evidence/ec-sparse-fetch/results.json) contain all seven cases,
including HTTP body bytes, GET counts, cache hits, logical throughput, and
p95/p99. Each row is the median of three trials of 64 isolated misses.

| 4 KiB miss | Default body bytes / GETs | Sparse body bytes / GETs | Default / sparse MiB/s | Default / sparse p95 ms | Default / sparse p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Known format | 4,194,308 / 1 | 65,536 / 1 | 0.115 / 0.612 | 40.591 / 7.523 | 41.769 / 8.060 |
| Cold format | 4,194,308 / 1 | 65,540 / 2 | 0.109 / 0.324 | 42.302 / 14.440 | 44.078 / 15.230 |

Both arms had zero data-cache hits during these misses and 64/64 cache hits on
the separate immediate rereads. The approximately 64-fold reduction applies
to HTTP body bytes. These local timings do not establish a production
throughput multiplier.

Both arms read the same persisted objects. All 16 audited records (four objects
on four directories) report `V2Obj.EcBSize=1048576`, with `EcM=2` and `EcN=2`,
despite the configured 262,144-byte hint. The
[layout audit](evidence/ec-sparse-fetch/summary.json) includes file hashes, and
the original [xl.meta files](evidence/ec-sparse-fetch/xl-meta/) are preserved.
The fixture reports RustFS 1.0.0; this does not re-derive or replace the supplied
RustFS 1.0.4 versus rebased `a87e8f94` hardware evidence.

Complete-object reads retained one GET and identical body bytes, with median
throughput 114.469 versus 113.436 MiB/s (about 0.9% lower for sparse). LZ4 and
Zstd retained one GET, identical body bytes, and 64/64 reread cache hits.

The partial-tail case exposes an acceptance gap. Sparse fetched 65,519 bytes
instead of 4,194,291 on the miss and returned correct data, but the existing
page-cache admission rule does not retain the incomplete last page. Immediate
rereads therefore changed from 64/64 cache hits to 0/64, adding 64 GETs and
4,193,216 body bytes per trial. Reread latency was not measured, so this run
cannot establish tail performance non-regression. The PR remains draft:
tail caching and fragmented complete-file scans need further evaluation,
along with the broad compose/FUSE mixed-workload gate. These measurements are
diagnostic evidence, not an accepted general performance improvement. The
70-minute experiment timebox ended before these gaps could be resolved.
The dedicated compose services and volume were removed after preserving data.

## Reproduction

Build after checking for an active compiler, using the same environment as the
local CI gate. The cargo output identifies the unit-test executable to pass to
the runner. Install `boto3` and `msgpack` in a local Python environment.

```bash
pgrep -a rustc
export CARGO_TARGET_DIR=/home/luxian/p1-alloc/repo/target
export CARGO_PROFILE_TEST_DEBUG=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0
cargo test -j 2 --lib --features rustfs-ec-block-size-hint --no-run
python docker/compose-xfstests/run_ec_sparse_read.py \
  --test-binary /absolute/path/from/cargo/output \
  --output /absolute/path/to/a/new/evidence-directory
```

The runner preserves raw trials, server identity/logs, and the persisted data
directory before removing its dedicated compose services and volume. Set
`RUSTFS_IMAGE` to select another server image. Its summary rejects unexpected
metadata framing instead of guessing the persisted EC geometry.

## Validation and resource limits

The final default workspace gate passed: 1,269 library tests and 2 binary tests,
zero failures, with 262 ignored tests not counted as passes. Workspace check,
build, Tokio/io_uring checks, strict Clippy on all targets, formatting, and the
three existing perf-report shell tests also passed. The final gate used
`CARGO_INCREMENTAL=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_PROFILE_DEV_DEBUG=0` and
one build job after the memory limit described below.
The final EC-hint library suite passed 1,277 tests, with zero failures and 265
ignored. [Validation records](evidence/ec-sparse-fetch/validation.json) include
commands, exit codes, test counts, and the resource-limited attempts.

The regression test first failed on the original dispatch: the sparse request
still performed one whole-object GET. The final tests exercise partial-page
and aligned requests with backend preferences both enabled and disabled,
retained page-cache hits after format eviction, publication single-flight,
complete scans, compressed frames, short tails, and byte-identical writes.

The first configuration-field approach failed to compile an explicit initializer
inside the protected overlay tree. The final API uses a private store field
and builder, so that tree is unchanged. The broad native/packed unit-test build
later exceeded the 2 GiB swap threshold; its test compiler was stopped and
subsequent builds used one job. That full feature-suite attempt is not counted
as a pass.

The compiled native library was checked with
`tests/ec_sparse_contracts.rs`, linked using `rustc --test`: both tests passed.
One exercises sparse reads followed by an exact packed frame range and digest
tamper rejection through the public APIs. The other exercises Native-P1
create-only publication/readback and rejection of both changed bytes and short
objects. The compiler command and test binary digest are retained in the local
evidence directory. A normal reproduction can use:

```bash
pgrep -a rustc
CARGO_INCREMENTAL=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_PROFILE_DEV_DEBUG=0 \
  cargo test -j 1 --test ec_sparse_contracts \
  --features native-packed-base,rustfs-ec-block-size-hint
```
