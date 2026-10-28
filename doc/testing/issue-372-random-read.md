# Issue #372: local random-read investigation

Baseline: `975deacc13131335b60a4c72de4ed781bbfe7154`, measured only on the
provided 8-vCPU / 30-GiB host. The severe overlay throughput gap reproduces;
the reported 400-ms p99 and 19x throughput ratio do **not** reproduce here.

| Arm | MiB/s, min / median / max (3 runs) | p99 ms, min / median / max (3 runs) |
| --- | --- | --- |
| flat-v1 | 85.238 / 86.334 / 87.403 | 30.802 / 31.326 / 31.588 |
| workspace-v1, native lower | 12.111 / 12.209 / 12.314 | 63.177 / 64.225 / 64.750 |

The native overlay median is 14.14% of flat-v1 throughput; its p99 is 2.05x
flat-v1. These are FUSE measurements using the unchanged compose perf runner,
Redis 7.2, and a local RustFS S3 service. They are not in-memory microbenchmarks.

fio: 4-KiB random reads, `direct=1`, `io_uring`, four jobs at depth 32,
512 MiB per job (2 GiB total), 20 seconds. Compression is disabled, blocks are
4 MiB, chunks 64 MiB, read RAM/SSD budgets 1/4 GiB, write RAM/SSD budgets
512 MiB/1 GiB, and the mount memory budget is 4 GiB. Prefetch is disabled;
FUSE uses 8 workers and max_background 512. The default Tokio worker count
is 1. Both arms strictly drain prefill writes to zero dirty/pending bytes and
read on the same mount. Client/backend caches can be warm; these results are
not physical disk bandwidth claims. No compilers ran during measurement.

An initial overlay attempt failed to remount immediately after prefill with
`workspace is busy`. It produced no read measurement, was excluded, and its
logs were retained. The valid comparison disables prefill remount for both
arms, retaining strict prefill drain. The local single backing filesystem
does not reproduce the issue's four-disk EC 2+2 layout.

The actual native-overlay read path is
`FileReader::read_chunk_span_into_observed -> WorkspaceMetaLayer::read_plan`.
It is distinct from `NativeDataRuntime` / `workspace-native-v2`; that earlier
investigation was abandoned without a behavior change or accepted measurement.
`read_plan` keys its bounded resolver cache by the exact request range.
Each range miss calls `read_extent_deltas_consistent`, which brackets paged
scans of both layers with generation checks. The backend scans each chunk's
entire extent history before filtering it to the requested range. This is
the current hypothesis; request-level timing is still pending and no
optimization has been accepted.

Local artifacts are in `/work/issue372/`: `baseline/brewfs` and its SHA-256,
`compose.override.yaml`, `run-compose.sh`, `host.txt`, `baseline-summary.json`,
and complete per-run fio JSON, mount logs, stats, and drain records. Valid
runs are `baseline-flat-{1,2,3}` and `baseline-overlay-{1-valid,2,3}`.

Pending: exclusive per-request timing breakdown, regression test demonstrating
repeated scans, one-root-cause change, all local Rust CI gates, and three
matched candidate runs. The original hardware, packed lower, and JuiceFS
comparison have not been validated by this investigation.
