# Tokio worker default: measured scope

The `brewfs` executable now defaults to one Tokio worker, using the same
multithread runtime. An explicit `TOKIO_WORKER_THREADS` remains authoritative.
Use `TOKIO_WORKER_THREADS=8` for workloads that benefit from more executor
threads. FUSE worker count and Tokio's blocking I/O pool are separate settings.

The motivating measurements used an eight-CPU Linux host, 16 FUSE workers,
`max_background=512`, Redis metadata, a verified initialized packed workspace,
and local-fs upper data. Attribute/entry TTLs were zero; writeback cache was
disabled. The packed lower contained a seed used to verify mount identity;
all measured files were writable upper data. These are not S3 or packed-lower
payload results, nor evidence about gateway workloads or every CPU count.

Three independent mounts per setting, alternating 8/1/1/8/8/1, gave medians:

| Workload | Tokio 8 | Tokio 1 | Change |
|---|---:|---:|---:|
| Warm serial stat, ops/s | 656.156 | 753.430 | +14.82% |
| Warm stat, eight processes, ops/s | 1386.979 | 1503.935 | +8.43% |
| Create 256 files, ops/s | 137.290 | 148.253 | +7.99% |
| Fresh stat 256 files, ops/s | 277.129 | 307.448 | +10.94% |
| Unlink 256 files, ops/s | 165.484 | 190.993 | +15.42% |
| Sequential write 32 MiB, MB/s | 115.210 | 118.557 | +2.91% |
| Write 64 × 4 KiB files, files/s | 78.545 | 87.086 | +10.87% |

Sequential write includes fsync and close. Warm-stat workloads use 5,000
operations, with an unmeasured warm-up; the eight processes use separate
directories and report total work over their makespan. The bounded metadata
follow-up has no stat warm-up. Do not combine their rates.

The final bounded confirmation used one fresh mount per setting, order 8/1:

| Workload | Tokio 8 | Tokio 1 | Change |
|---|---:|---:|---:|
| Create 256 files, ops/s | 123.173 | 130.898 | +6.27% |
| Sequential write 32 MiB, MB/s | 96.885 | 101.186 | +4.44% |
| Eight-process create, 256 total, ops/s | 344.608 | 343.040 | -0.46% |
| Eight-process fresh stat, 256 total, ops/s | 996.410 | 762.170 | -23.51% |
| Eight-process unlink, 256 total, ops/s | 625.707 | 388.466 | -37.91% |

These short concurrency samples are a material caveat: the warm-stat gain
does not imply a gain for fresh parallel metadata or unlink. They are single
observations, not new medians or confidence intervals. Keep the explicit
override when evaluating a parallel workload.

Buffered mixed fio (32 MiB, 64 KiB randrw, 50% reads, one job/depth 1, 8 seconds,
end_fsync) returned EIO at **both settings**: 3/3 each in the earlier series
and 1/1 each in the confirmation. Failed throughput is excluded. This default
change does not fix that error or establish mixed-I/O throughput parity.

All measurements used the same retained release, SHA256
`15128b8c87fe0d16e9025ba5902ec45bdacc5095e5865b3129942d6855bdda2c`,
with explicit worker environment settings. That binary predates #339; the
failure counts are not a claim that #339 still fails. No compiler overlapped
the measurements. Confirmation minimum available RAM was 28.322 GiB, swap
unused; both mounts exited cleanly without daemon fence/error logs.

Raw artifacts are retained at `/home/hxy/perf-bench/owner/artifacts/` in
`config-tokio`, `config-tokio-small-r2`, and `config-tokio-land-r1`; their
environment, mount, per-worker timing, fio output and executable hashes are
recorded. `scratch/owner-config-small.py --concurrency --order 8,1` ran the
final confirmation using fresh packed workspaces.

The runtime helper was compiled directly against the cached Tokio dependency
and its two tests passed, including isolated child processes that verify the
actual worker count with no override and with an override of two, and execute
a spawned task. Formatting and diff checks passed. This iteration did not
rebuild the full filesystem or rerun broad CI; the performance comparison
isolates the existing runtime setting on the same executable.
