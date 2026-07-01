# BrewFS Performance Optimization Roadmap

## Baseline (with fix applied)

| Workload   | Throughput | P99 Latency |
|------------|-----------|-------------|
| SeqRead    | 220 MiB/s | 36ms        |
| SeqWrite   | 145 MiB/s | 194ms       |
| RandRead   | 76 MiB/s  | 952ms       |
| RandWrite  | 139 MiB/s | 793ms       |

## Priority 1: S3 Operation Timeouts (Reliability)

**Problem**: S3 uploads can hang indefinitely (no connect/read/operation timeout),
causing `commit_chunk` to loop forever and FUSE operations to block.

**Fix**:
- Add `TimeoutConfig` to the AWS SDK S3 client:
  - `connect_timeout`: 5s
  - `read_timeout`: 30s  
  - `operation_timeout`: 120s
- Add a max upload duration in `commit_chunk` (mark slice as Failed after 180s)
- This unblocks generic/091 from hanging indefinitely

**Impact**: Prevents indefinite hangs; enables generic/091 to either pass or fail
cleanly.

## Priority 2: Sequential Write Throughput (145 → 250+ MiB/s)

**Bottleneck**: Single-threaded commit path and small slice upload granularity.

**Optimizations**:
- **Parallel block uploads**: Upload multiple 4MB blocks from a frozen slice
  concurrently (currently sequential)
- **Larger slice coalescing**: Merge adjacent small slices before upload to
  reduce HTTP overhead
- **Pipeline commit**: Start metadata commit while upload is still in flight
  for the last block (CommitBeforeUpload mode already exists but unused)
- **Write buffer backpressure tuning**: Current hard limit (2× soft) causes
  stalls; use a graduated backpressure curve

## Priority 3: Random Read Latency (952ms P99 → <200ms)

**Bottleneck**: Each random read goes to S3 (cache misses on cold data).

**Optimizations**:
- **Aggressive prefetch on open**: When a file is opened, prefetch its slice
  metadata and first N blocks in parallel
- **Read-ahead for sequential patterns**: Detect sequential access and prefetch
  next blocks
- **Larger local cache**: Increase block cache capacity (currently limited)
- **Connection pooling**: Ensure idle connections are reused (partially done
  with Hyper pool_max_idle_per_host=64)

## Priority 4: Metadata Batching

**Bottleneck**: Each stat/lookup is a separate Redis RTT.

**Optimizations**:
- **Batch stat on readdir**: When listing a directory, prefetch all child node
  attributes in a single MGET
- **Slice metadata prefetch**: On file open, batch-fetch all chunk slice lists
- **Pipeline Redis commands**: Use Redis pipelining for concurrent independent
  operations

## Priority 5: Parallel Read Scaling

**Bottleneck**: Single DataFetcher per read operation.

**Optimizations**:
- **Concurrent block fetches**: For large reads spanning multiple blocks, fetch
  them in parallel (up to N concurrent S3 GETs)
- **Vectored I/O**: Use splice/sendfile for zero-copy reads where possible
- **Read request merging**: Merge adjacent small reads into single S3 range
  requests

## Negative Result: Writeback-Pending-Gated Range Prefetch

2026-06-10 tested a prototype that skipped range-read-triggered full-block
background prefetch when commit-before-upload pending bytes exceeded a limit.
The intent was to keep read tail benefits from normal background prefetch while
reducing object-store/cache contention during heavy randrw writeback.

Test command shape:

```bash
PERF_FIO_RANDRW_RUNTIME=20 \
PERF_FIO_RANDRW_SIZE=512m \
PERF_FIO_PREFILL_DRAIN_TIMEOUT_SECS=600 \
BREWFS_COMPRESSION=none \
BREWFS_PREFETCH_ENABLED=true \
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile --tools "fio-randrw"
```

Results:

| Variant | Read BW | Write BW | Read P99 | Write P99 | Script Wall | Notes |
|---------|---------|----------|----------|-----------|-------------|-------|
| No pending gate | 719.57 MiB/s | 328.80 MiB/s | 54.264 ms | 120.062 ms | 113s | Baseline for this code state |
| Pending gate 1GiB | 117.78 MiB/s | 53.24 MiB/s | 54.788 ms | 53.215 ms | 141s | Severe throughput regression; active IO stretched to 136.9s |
| Pending gate 512MiB | 751.47 MiB/s | 342.56 MiB/s | 53.740 ms | 40.108 ms | 128s | Throughput ok, but close/flush tail worsened; gate did not trigger in final stats |

Conclusion: pending-byte-gated range background prefetch is not a valid default
optimization. It was rolled back. The next write-path attempt should target the
actual tail driver shown by the reports: high PUT/object count and recent pending
upload drain time. Prefer small-write coalescing, upload object-count accounting,
and tighter writeback drain instrumentation over read-prefetch gating.

## Result: Nonblocking Range Background Prefetch Admission

2026-06-10 implemented a low-risk object/read-cache change: range-triggered
full-block background prefetch now uses a nonblocking permit acquisition. When
all range prefetch permits are busy, the prefetch is dropped and counted instead
of queueing behind existing background GETs. This keeps the existing prefetch
benefit when capacity is available, while preventing background work from
building an unbounded tail under pressure.

Targeted TDD:

- RED: `cargo test -p brewfs test_background_range_prefetch_drops_when_limit_saturated --lib`
  failed because `read_background_prefetch_dropped` stayed at 0 while all 8
  permits were held.
- GREEN: the same test passes after switching the permit acquisition to
  `try_acquire_owned()`.
- Regression checks:
  - `cargo test -p brewfs test_background_range_prefetch_is_serialized --lib`
  - `cargo test -p brewfs test_small_range_read_prefetches_full_block_in_background --lib`
  - `cargo test -p brewfs test_intelligent_read_strategy --lib`
  - `cargo fmt --all --check`
  - `bash -n` for the compose and tools/perf scripts

Compose validation command shape:

```bash
PERF_FIO_RANDRW_RUNTIME=20 \
PERF_FIO_RANDRW_SIZE=512m \
PERF_FIO_PREFILL_DRAIN_TIMEOUT_SECS=600 \
BREWFS_COMPRESSION=none \
BREWFS_PREFETCH_ENABLED=true \
BREWFS_UPLOAD_CONCURRENCY=32 \
bash docker/compose-xfstests/run_redis_perf.sh \
  --writeback-throughput-profile --tools "fio-randrw"
```

Compose artifact: `docker/compose-xfstests/artifacts/perf-run-1781112437-22526`.

| Variant | Read BW | Write BW | Read P99 | Write P99 | Script Wall | Notes |
|---------|---------|----------|----------|-----------|-------------|-------|
| Prior default-32 baseline | 127.11 MiB/s | 57.76 MiB/s | 52.691 ms | 26.083 ms | 122s | `perf-run-1781110789-10279` |
| Nonblocking prefetch permits | 127.11 MiB/s | 57.88 MiB/s | 58.982 ms | 22.413 ms | 125s | No obvious throughput/write-tail regression; dropped=0 in this run |

The compose run did not saturate range prefetch permits
(`brewfs_read_background_prefetch_dropped_total=0`), so it mainly proves the
change is neutral for this randrw profile. It does not prove a large win for the
default workload.

Tools/perf quick validation:

```bash
PERF_FIO_WORKLOADS="randrw" \
PERF_FIO_DIRECT=0 \
PERF_RECORD_FREQ=19 \
BREWFS_COMPRESSION=none \
BREWFS_PREFETCH_ENABLED=true \
BREWFS_UPLOAD_CONCURRENCY=32 \
bash tools/perf/run_perf.sh --no-build --quick --skip-oncpu --skip-offcpu
```

Tools/perf artifact: `tools/perf/results/20260610-173023`.

| Variant | Read BW | Write BW | Read P99 | Read P99.9 | Write P99 | Write P99.9 |
|---------|---------|----------|----------|------------|-----------|-------------|
| Prior quick baseline | 637.5 MiB/s | 289.7 MiB/s | 29.75 ms | 2.20 s | 893.39 ms | 2.40 s |
| Nonblocking prefetch permits | 768.0 MiB/s | 343.9 MiB/s | 28.70 ms | 320.86 ms | 943.72 ms | 2.09 s |

Conclusion: keep this change. It fixes a real admission-control bug and shows
no meaningful compose regression. The next high-impact bottleneck remains write
buffer/backpressure and committed-but-not-uploaded slice drain: tools/perf still
flags write P99 around 944ms and identifies `src/vfs/io/writer.rs` as the next
target.

## Negative Result: LZ4 Raw-Fallback Read Zero-Copy

2026-06-11 tested an object/read-cache candidate from the parallel review:
avoid copying raw fallback payloads on the LZ4 read path. The prototype added a
`decompress_to_bytes(Vec<u8>) -> Bytes` helper and switched full-object read and
range-triggered background prefetch to keep the original S3 payload buffer when
an object has no compression header.

Targeted TDD:

- RED: `cargo test -p brewfs test_raw_data_to_bytes_reuses_vec_without_copy --lib`
  failed because the helper did not exist.
- GREEN: the helper returned `Bytes` with the same pointer for raw data.
- Regression checks passed before perf comparison:
  - `cargo test -p brewfs chunk::compress::tests --lib`
  - `cargo test -p brewfs test_compressed_small_read_uses_full_object --lib`
  - `cargo test -p brewfs test_intelligent_read_strategy --lib`
  - `cargo test -p brewfs test_small_read_piggybacks_in_flight_full_block_read --lib`
  - `cargo fmt --all --check`

The first perf attempt exposed an environment issue: `/mnt/slayerfs/root/.cache/brewfs`
had grown to about 12GiB, leaving too little headroom for tools/perf result
files. That generated `No space left on device` while writing fio JSON. The
cache was removed because it is regenerated BrewFS local cache data.

Valid `tools/perf` quick comparison with `compression=lz4`:

```bash
PERF_FIO_WORKLOADS="randrw" \
PERF_FIO_DIRECT=0 \
PERF_RECORD_FREQ=19 \
BREWFS_COMPRESSION=lz4 \
BREWFS_PREFETCH_ENABLED=true \
BREWFS_UPLOAD_CONCURRENCY=32 \
bash tools/perf/run_perf.sh --quick --skip-oncpu --skip-offcpu
```

| Variant | Artifact | Read BW | Write BW | Read P99 | Write P99 | Write P99.9 |
|---------|----------|---------|----------|----------|-----------|-------------|
| Raw-fallback zero-copy prototype | `tools/perf/results/20260611-051752` | 503.6 MiB/s | 230.6 MiB/s | 29.75 ms | 1.52 s | 5.34 s |
| Baseline mainline | `tools/perf/results/20260611-052102` | 759.0 MiB/s | 343.6 MiB/s | 37.49 ms | 111.67 ms | 2.84 s |

Conclusion: do not merge this prototype. The idea is locally sensible, but this
implementation did not translate into a workload win and coincided with a large
write-tail regression in the A/B sample. Keep focus on the bottleneck reported
by tools/perf: write buffer backpressure, auto-flush, and committed-but-not-
uploaded drain behavior.

## Measurement

Run benchmarks before/after each optimization:
```bash
tools/perf/run_perf.sh --quick --skip-offcpu --no-build
```

For detailed profiling:
```bash
tools/perf/run_perf.sh  # Full run with flamegraph
```
