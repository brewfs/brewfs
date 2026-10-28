# Issue #370: local metadata fan-out experiment

Status: investigation and candidate validation in progress; this is not an
accepted performance result. Baseline: `975deac`, on the task's single 8-vCPU,
30-GiB machine. Redis is local (`127.0.0.1:6380`), data uses LocalFS, volume
format is workspace-v1. No remote benchmark machine was used.

## Local reproduction

The initial 1,000-file, 8-KiB/file fixture wrote 8,192,000 bytes in 1.880098 s
(531.887 files/s). Redis had 3,022 delta keys (1,012 inode, 1,010 dentry,
1,000 extent), with 845,840 bytes of summed `MEMORY USAGE`. The per-file
three-key delta shape reproduces. Including indexes and auxiliary records,
the same volume has 4,051 keys / 1,848,843 bytes; delta-only counts must not
be compared to whole-volume counts.

A second volume populated with the baseline binary has 100,000 files of
85,900 bytes each (8,590,000,000 bytes, approximately 8 GiB), in 1,000
directories. The large-workload amplification is reproduced locally:

| Family | Keys | Summed Redis MEMORY USAGE (bytes) |
| --- | ---: | ---: |
| delta/inode | 101,002 | 28,280,560 |
| delta/dentry | 101,000 | 26,648,000 |
| delta/extent | 100,000 | 29,600,000 |
| packed auxiliary records | 101,008 | 36,313,696 |
| key indexes | 2 | 65,454,296 |
| remaining control/lease/catalog keys | 10 | 2,272 |
| **Total** | **403,022** | **186,298,824** |

This is not the exact original corpus (373,040 keys / 192 MB); it reproduces
the approximately four-key/file and large memory footprint on this machine.
Large-fixture creation took 440.699 s while release compilation was active,
so that creation time is preparation telemetry, not an acceptance baseline.

Initial MONITOR/strace samples (different cache/warmup state from the explicit
uncached profile below):

| Operation | Top-level Redis commands | Socket writes | Sent bytes | Received bytes |
| --- | ---: | ---: | ---: | ---: |
| stat | 13 | 10 | 4,915 | 3,079 |
| open + 4-KiB read + close | 12 | 11 | 6,274 | 3,558 |
| 100-entry directory listing (READDIRPLUS) | 113 | 110 | 29,409 | 58,760 |

Lua-internal commands are not additional network round trips. Socket writes
are observed client write groups, not counts of TCP packets.

## Diagnosis

A 100-entry READDIRPLUS performs 100 serial two-layer inode MGETs for child
attributes. The directory names are already fetched by range scans; the
remaining fan-out is fetching each child's attributes separately.

A second explicit diagnostic profile sets
`BREWFS_WORKSPACE_ROW_CACHE_TTL_MS=0` and `--fuse-cache-ttl-ms 0`. Python times
syscalls within one process, while strace captures complete RESP request and
response payloads. In steady samples:

| Operation | Redis commands / socket writes | Sent bytes | Received bytes |
| --- | ---: | ---: | ---: |
| stat | 11 / 11 | 4,930 | 2,501 |
| open + 4-KiB read + close (warm data/read plan) | 15 / 15 | 7,009 | 3,400 |
| 100-entry READDIRPLUS listing | 126 / 126 | 35,282 | 61,289 |

The first read (cold plan) uses 20 commands / writes, 9,610 sent bytes and
3,801 received bytes. Cache state therefore must accompany these counts.

For the middle directory trace (17,683.708 us total), measured outstanding
socket-request spans break down as follows:

| Segment | Time (us) | Wall share |
| --- | ---: | ---: |
| inode row reads | 8,293.390 | 46.90% |
| directory scans | 2,178.431 | 12.32% |
| permission snapshots | 160.217 | 0.91% |
| dentry lookup | 69.141 | 0.39% |
| outside socket-request spans | 6,982.528 | 39.49% |

These are diagnostic spans from traced execution, including client scheduling
and ptrace overhead, not pure Redis server times or throughput evidence. The
remainder includes FUSE, application, response decoding and tracing overhead;
it is not labelled as handler time. Release compilation was active during
this diagnostic sample. Untraced performance runs must be isolated from builds.

## Candidate and acceptance

The candidate batches child inode rows for a directory page into one MGET,
preserving input order, duplicate inode results, upper tombstones, packed-lower
fallback, row-cache invalidation tickets and local writeback sizes. Other
metadata layers have a single-stat fallback. A failed batch retries the old
per-entry path to preserve omitted-entry error behavior. No key format changes
or migration are intended.

The candidate is not accepted yet. In particular, faster READDIRPLUS does not
by itself establish the issue's >=10% random-read or repeated-open criterion.
Those measurements and all required CI gates must be reported separately.

Validation so far: default `cargo test -p brewfs --lib -j 4` passed with
1,275 passed / 0 failed / 262 pre-existing ignored tests. Both default and
workspace-overlay `cargo clippy --all-targets -- -D warnings` exited 0;
existing vendored dependency warnings remain visible. `cargo fmt --all
--check` passed. No ignores or assertions were removed or added to suppress
failures. Full workspace and feature-enabled tests are still pending.

Local raw evidence and scripts: `/work/issue-370/artifacts/` and
`/work/issue-370/{measure,trace_ops,analyze_trace}.py`. The original baseline
binary is preserved as `/work/issue-370/brewfs-baseline`, SHA-256
`c673a43296b2ff9051d3a2ee26b616026ccd0a4d58b038bad5821c6d0765df4c`.

Draft PR: https://github.com/brewfs/brewfs/pull/374

Unverified: >=3 matched untraced repetitions, full
workspace tests, feature-enabled complete tests, compose/S3 guard runs
(Docker is unavailable on this host), TiKV and packed-lower mounted performance.
