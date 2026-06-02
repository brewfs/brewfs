# BrewFS Detailed Performance Profile

Artifact: `perf-run-1779281478-692`

## Throughput Summary

| Workload | Mode | BS | Jobs | Read BW | Write BW | Read IOPS | Write IOPS |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| fio-randread | randread | 4m | 4 | 109.5 MiB/s | - | 27.4 | - |
| fio-randrw | randrw | 4m | 4 | 74.0 MiB/s | 33.4 MiB/s | 18.5 | 8.3 |
| fio-randwrite | randwrite | 4m | 4 | - | 146.0 MiB/s | - | 36.5 |
| fio-seqread | read | 4m | 1 | 217.4 MiB/s | - | 54.4 | - |
| fio-seqwrite | write | 4m | 1 | - | 160.3 MiB/s | - | 40.1 |

## Latency Summary

| Workload | Read Mean | Read P50 | Read P99 | Write Mean | Write P50 | Write P99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| fio-randread | 145.17 ms | 127.40 ms | 513.80 ms | - | - | - |
| fio-randrw | 208.49 ms | 122.16 ms | 767.56 ms | 13.99 ms | 7.63 ms | 63.70 ms |
| fio-randwrite | - | - | - | 108.94 ms | 51.64 ms | 658.51 ms |
| fio-seqread | 18.02 ms | 17.69 ms | 33.42 ms | - | - | - |
| fio-seqwrite | - | - | - | 24.39 ms | 3.46 ms | 308.28 ms |

## Latency Distribution

### Read Latency Percentiles

| Workload | p1 | p5 | p25 | p50 | p75 | p90 | p95 | p99 | p99.9 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| fio-randread | 26.08 ms | 71.83 ms | - | 127.40 ms | - | 217.06 ms | 287.31 ms | 513.80 ms | 759.17 ms |
| fio-randrw | 25.82 ms | 32.64 ms | - | 122.16 ms | - | 501.22 ms | 616.56 ms | 767.56 ms | 1.02 s |
| fio-seqread | 8.85 ms | 10.42 ms | - | 17.69 ms | - | 20.58 ms | 25.03 ms | 33.42 ms | 102.24 ms |

### Write Latency Percentiles

| Workload | p1 | p5 | p25 | p50 | p75 | p90 | p95 | p99 | p99.9 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| fio-randrw | 2.97 ms | 3.49 ms | - | 7.63 ms | - | 32.37 ms | 35.91 ms | 63.70 ms | 95.94 ms |
| fio-randwrite | 2.24 ms | 4.08 ms | - | 51.64 ms | - | 304.09 ms | 404.75 ms | 658.51 ms | 960.50 ms |
| fio-seqwrite | 1.88 ms | 2.01 ms | - | 3.46 ms | - | 43.25 ms | 86.51 ms | 308.28 ms | 1.84 s |

## Metadata Performance

| Operation | Ops/sec | Latency (µs/op) |
| --- | ---: | ---: |
| create | 176.2 | 5675 |
| open | 1110.6 | 900 |
| stat | 4861.2 | 206 |
| readdir | 21892.1 | 46 |
| rename | 722.8 | 1384 |

## Bottleneck Analysis

### fio-randread (randread, bs=4m, jobs=4)

  - **High read baseline**: p50=127ms. Network RTT to S3 dominates. Consider prefetch tuning or local cache.
  - **Read scaling**: 27 MiB/s/job (4 jobs). May be limited by S3 connection pool or prefetch contention.

### fio-randrw (randrw, bs=4m, jobs=4)

  - **Read tail latency**: p99/p50 = 6.3x (122.16 ms → 767.56 ms). Likely cause: S3 GET retry or cache miss on cold blocks.
  - **High read baseline**: p50=122ms. Network RTT to S3 dominates. Consider prefetch tuning or local cache.
  - **Read scaling**: 19 MiB/s/job (4 jobs). May be limited by S3 connection pool or prefetch contention.

### fio-randwrite (randwrite, bs=4m, jobs=4)

  - **Write P99 > 500ms** (659ms): Consider increasing write buffer capacity or S3 upload concurrency.

### fio-seqread (read, bs=4m, jobs=1)

  - **Read outliers**: p99.9/p99 = 3.1x. Possible GC pause, TCP retransmit, or lock contention.

### fio-seqwrite (write, bs=4m, jobs=1)

  - **Write stall pattern**: p50=3.5ms, p99=308ms. Most writes are buffered (fast), but auto_flush/freeze triggers S3 upload that blocks subsequent writes (write buffer hard limit).


## Optimization Roadmap

### 1. Write Buffer Management

Write P99=659ms in fio-randwrite. Consider: increase write_buffer_hard_limit, use adaptive auto_flush based on upload throughput feedback, or implement S3 upload pipelining to avoid blocking writes during uploads.

### 2. Random Read Prefetch

Random read p50=127ms in fio-randread. Each 4MB block requires a full S3 GET. Consider: smaller block size for random workloads, read-ahead pattern detection, or tiered block cache with SSD backing.

### 3. Parallel Read Scaling

Only 27 MiB/s/job in fio-randread (4 jobs). May be limited by: connection pool size, prefetch contention, or per-inode lock granularity. Consider per-chunk parallelism.


## Comparison (Baseline → Current)

| Workload | Metric | Baseline | Current | Delta |
| --- | --- | ---: | ---: | ---: |
| fio-randread | Read BW | 107.1 MiB/s | 109.5 MiB/s | +2.2% |
| fio-randread | Read P99 | 505.41 ms | 513.80 ms | +1.7% |
| fio-randrw | Read BW | 82.1 MiB/s | 74.0 MiB/s | -9.9% |
| fio-randrw | Write BW | 37.2 MiB/s | 33.4 MiB/s | -10.4% |
| fio-randrw | Read P99 | 750.78 ms | 767.56 ms | +2.2% |
| fio-randrw | Write P99 | 89.65 ms | 63.70 ms | -28.9% |
| fio-randwrite | Write BW | 229.9 MiB/s | 146.0 MiB/s | -36.5% |
| fio-randwrite | Write P99 | 530.58 ms | 658.51 ms | +24.1% |
| fio-seqread | Read BW | 204.7 MiB/s | 217.4 MiB/s | +6.2% |
| fio-seqread | Read P99 | 58.98 ms | 33.42 ms | -43.3% |
| fio-seqwrite | Write BW | 219.2 MiB/s | 160.3 MiB/s | -26.9% |
| fio-seqwrite | Write P99 | 223.35 ms | 308.28 ms | +38.0% |

## Current Run Details

- **fio-randread** (randread, bs=4m, 4j):, Read 109.5 MiB/s
- **fio-randrw** (randrw, bs=4m, 4j):, Read 74.0 MiB/s, Write 33.4 MiB/s
- **fio-randwrite** (randwrite, bs=4m, 4j):, Write 146.0 MiB/s
- **fio-seqread** (read, bs=4m, 1j):, Read 217.4 MiB/s
- **fio-seqwrite** (write, bs=4m, 1j):, Write 160.3 MiB/s


## Next-Step Optimization Plan

Based on the profiling data, these are the prioritized optimizations:

### Priority 1: Write Buffer Back-Pressure (P99 reduction)

**Problem**: Sequential write P99=308ms, random write P99=659ms caused by write buffer hard
limit blocking FUSE writes while waiting for S3 uploads.

**Approach**:
- Implement adaptive auto_flush: when upload throughput drops, increase slice size to reduce
  PUT overhead; when upload is fast, keep current 500ms age limit.
- Increase write_buffer_hard_limit or make it configurable per-workload.
- Pipeline S3 uploads: start next slice upload before previous completes (overlap compression/
  serialization with network).

### Priority 2: Random Read Latency (p50=127ms)

**Problem**: Each 4MB random read requires a full S3 GET (127ms RTT). No benefit from
sequential prefetch for random access patterns.

**Approach**:
- Implement SSD-backed block cache tier (already partially implemented in ChunksCache disk layer).
- Add random access detection in prefetcher — disable sequential read-ahead for random
  patterns, reducing wasted bandwidth.
- Consider smaller block size option (512KB-1MB) for random-read-heavy workloads to reduce
  amplification.

### Priority 3: Multi-Job Read Scaling (27 MiB/s/job)

**Problem**: 4 parallel readers only achieve 27 MiB/s each (109 MiB/s total vs 217 MiB/s
single-job). Contention in S3 connection pool or prefetch scheduling.

**Approach**:
- Increase S3 connection pool size (currently limited by hyper/reqwest defaults).
- Per-inode prefetch isolation — ensure one inode's prefetch doesn't starve another.
- Measure if TCP connection reuse is working correctly (HTTP/1.1 keep-alive vs HTTP/2).

### Priority 4: Metadata Create Latency (5.7ms/op)

**Problem**: File creation takes 5.7ms — acceptable but could be improved for workloads
with many small files.

**Approach**:
- Batch metadata writes where possible (directory entries + inode in single Redis pipeline).
- Add metadata write-behind: return success immediately, commit asynchronously.
