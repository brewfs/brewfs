# Local HTTP read-policy results

Medians of three trials, 64 isolated misses per trial. Every miss had zero
data-cache hits. Repeat hits count a separate immediate reread after each miss.
Throughput and latency cover misses only; units and exclusions are in the main report.

| Case | Policy | HTTP body bytes/miss | GETs/miss | Repeat hits/64 | Logical MiB/s | p95 ms | p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| cold-4k | BackendDefault | 4,194,308 | 1 | 64 | 0.109 | 42.302 | 44.078 |
| cold-4k | Sparse | 65,540 | 2 | 64 | 0.324 | 14.440 | 15.230 |
| complete-object | BackendDefault | 4,194,308 | 1 | 64 | 114.469 | 41.413 | 43.626 |
| complete-object | Sparse | 4,194,308 | 1 | 64 | 113.436 | 42.282 | 44.380 |
| known-4k | BackendDefault | 4,194,308 | 1 | 64 | 0.115 | 40.591 | 41.769 |
| known-4k | Sparse | 65,536 | 1 | 64 | 0.612 | 7.523 | 8.060 |
| known-aligned-64k | BackendDefault | 4,194,308 | 1 | 64 | 1.800 | 40.896 | 43.886 |
| known-aligned-64k | Sparse | 65,536 | 1 | 64 | 10.156 | 7.506 | 7.773 |
| lz4-4k | BackendDefault | 16,468 | 1 | 64 | 0.824 | 5.891 | 8.146 |
| lz4-4k | Sparse | 16,468 | 1 | 64 | 0.834 | 5.992 | 7.382 |
| tail-4k | BackendDefault | 4,194,291 | 1 | 64 | 0.116 | 40.217 | 42.134 |
| tail-4k | Sparse | 65,519 | 1 | 0 | 0.649 | 7.455 | 7.917 |
| zstd-4k | BackendDefault | 151 | 1 | 64 | 0.918 | 5.337 | 5.714 |
| zstd-4k | Sparse | 151 | 1 | 64 | 0.944 | 5.254 | 5.588 |

Partial tail pages retain the existing range-cache admission behavior. Sparse
tail rereads have 0/64 cache hits versus 64/64 for the default, adding 64 GETs
and 4,193,216 HTTP body bytes per trial. Reread latency was not measured; tail
performance non-regression is therefore not established. Exact counts are also
retained in `summary.json`. These are local diagnostic measurements; the PR is
draft pending tail caching, fragmented scans, and broad compose/FUSE acceptance.
