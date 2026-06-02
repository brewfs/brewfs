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

## Measurement

Run benchmarks before/after each optimization:
```bash
tools/perf/run_perf.sh --quick --skip-offcpu --no-build
```

For detailed profiling:
```bash
tools/perf/run_perf.sh  # Full run with flamegraph
```
