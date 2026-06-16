<div align="center">
  <img src="doc/icon.png" alt="BrewFS icon" width="96" height="96" />
</div>

<h1 align="center">BrewFS</h1>
<p align="center"><strong>High-performance Rust and layer-aware distributed filesystem</strong></p>
<p align="center"><a href="README.md"><b>English</b></a> | <a href="README_CN.md">中文</a></p>

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/language-Rust-orange.svg)](https://www.rust-lang.org/)

BrewFS is a Rust filesystem for container, AI, and object-storage-heavy workloads. It exposes a POSIX-like FUSE interface, stores file data as chunked objects, and keeps namespace and slice metadata in a pluggable transactional metadata backend.

The core design goal is to decouple compute from storage: applications read and write normal paths, while BrewFS handles chunk layout, object IO, caching, metadata transactions, compaction, and garbage collection.

BrewFS is not a JuiceFS fork, but JuiceFS is the main production-grade reference point used for performance comparison and gap analysis. Current tuning work compares metadata caching, read/write cache behavior, writeback semantics, object-store amplification, compaction, and test coverage against JuiceFS so regressions and improvements can be measured against a mature baseline.

## Architecture

Main layers:

- `fuse` and `vfs`: inode-based FUSE integration and POSIX-facing behavior.
- `meta`: metadata client, transaction-capable backends, sessions, control plane, compaction hooks, and GC metadata.
- `chunk`: chunk/block layout, read/write path, cache, compaction, delayed deletion, and block-store GC.
- `cadapter`: object backend abstraction with LocalFS and S3-compatible implementations.
- `fs` and SDK examples: path-based API and local examples that can run without FUSE.

Default data layout:

- Chunk size: 64 MiB
- Block size: 4 MiB
- Object granularity: blocks addressed under slice IDs

## Current Capabilities

Data backends:

- `local-fs`: stores object data in a local directory for development and tests.
- `s3`: supports AWS S3 and S3-compatible services such as RustFS, MinIO, and Ceph RGW.

Metadata backends:

- `sqlx`: SQLite for local/dev and PostgreSQL for server deployments.
- `redis`: low-latency metadata operations with Lua/CAS based chunk updates.
- `etcd`: distributed KV metadata with transaction and watch-oriented semantics.
- `tikv`: transactional TiKV metadata backend with namespace, file data, hardlinks, symlinks, rename exchange, compaction hooks, and delayed/uncommitted slice GC support.

Filesystem and runtime features:

- FUSE mount via `brewfs mount`
- Path/inode operations for create, mkdir, readdir, stat, read, write, truncate, unlink, rmdir, rename, hardlink, and symlink
- Chunked sparse IO with zero-fill for holes
- Read/write cache with memory and SSD budgets
- Optional S3 writeback mode (`commit_before_upload`) with orphan cleanup support
- Slice compaction and two-phase block deletion
- Runtime control plane for `brewfs info` and `brewfs gc`

## Quick Start

Requirements:

- Rust 1.85+ (the crate uses Rust 2024 edition)
- Linux for FUSE mounting
- `fuse3` / `fusermount3` for unprivileged mounts

Run the SDK demo without FUSE:

```bash
cargo run -p brewfs --example sdk_demo -- /tmp/brewfs-sdk-data
```

Build the CLI:

```bash
cargo build -p brewfs --release
```

BrewFS defaults to the `io_uring` FUSE runtime. Build the Tokio FUSE runtime with:

```bash
cargo build -p brewfs --release --no-default-features --features fuse-tokio-runtime
```

Mount with local object storage and SQLite metadata:

```bash
mkdir -p /tmp/brewfs-mnt /tmp/brewfs-data

cargo run -p brewfs -- mount /tmp/brewfs-mnt \
  --data-backend local-fs \
  --data-dir /tmp/brewfs-data \
  --meta-backend sqlx \
  --meta-url sqlite:///tmp/brewfs-meta.db
```

Unmount with `Ctrl+C` in the mount process, or use `fusermount3 -u /tmp/brewfs-mnt` if needed.

## Configuration

BrewFS can be configured with CLI flags, a YAML file, or both. CLI flags override YAML values.

Minimal YAML:

```yaml
mount_point: /tmp/brewfs

data:
  backend: local-fs
  localfs:
    data_dir: ./data

meta:
  backend: sqlx
  sqlx:
    url: "sqlite::memory:"

layout:
  chunk_size: 67108864
  block_size: 4194304
```

S3 plus Redis example:

```yaml
mount_point: /mnt/brewfs

data:
  backend: s3
  s3:
    bucket: brewfs-data
    endpoint: http://127.0.0.1:9000
    region: us-east-1
    force_path_style: true
    disable_payload_checksum: true
    part_size: 16777216
    max_concurrency: 16

meta:
  backend: redis
  redis:
    url: "redis://127.0.0.1:6379/0"

cache:
  root: /var/cache/brewfs
  writeback_mode: upload_before_commit
```

TiKV metadata example:

```yaml
mount_point: /mnt/brewfs

meta:
  backend: tikv
  tikv:
    pd_endpoints:
      - 127.0.0.1:2379
    namespace: tenant-a
```

See [doc/operations/configuration.md](doc/operations/configuration.md) and the files under [examples/](examples/) for the full configuration surface.

## CLI

```bash
brewfs mount [OPTIONS] [MOUNT_POINT]
brewfs info [MOUNT_POINT]
brewfs gc [MOUNT_POINT] [--dry-run]
```

Useful mount options:

- `--config <FILE>`: YAML configuration file.
- `--data-backend <local-fs|s3>`: object data backend.
- `--meta-backend <sqlx|redis|etcd|tikv>`: metadata backend.
- `--chunk-size <BYTES>` and `--block-size <BYTES>`: data layout tuning.
- `--fuse-workers <N>`: `0` or `1` uses low-overhead asyncfuse session dispatch; values above `1` enable the worker pool.
- `--fuse-max-background <N>`: maximum queued and running FUSE requests.
- `--privileged`: use `/dev/fuse` directly instead of `fusermount3`.

## Testing

Fast local checks:

```bash
cargo check -p brewfs
cargo test -p brewfs
```

Focused checks used often during backend work:

```bash
cargo test -p brewfs meta::stores::tikv --lib
cargo test -p brewfs mount_config --bin brewfs
```

Docker-based filesystem tests:

```bash
cd docker
bash compose-xfstests/run_redis_xfstests.sh --cases "generic/001"
bash compose-xfstests/run_redis_xfstests.sh --s3 --cases "generic/001"
```

More test and benchmark entry points:

- [docker/README.md](docker/README.md)
- [doc/testing/docker-compose-test-guide.md](doc/testing/docker-compose-test-guide.md)
- [doc/testing/bench.md](doc/testing/bench.md)
- [doc/testing/fuzz_testing_guide.md](doc/testing/fuzz_testing_guide.md)

## JuiceFS Comparison

BrewFS tracks JuiceFS as a practical benchmark for distributed filesystem semantics and object-storage performance. The comparison is organized around three document sets:

- [JuiceFS internals notes](doc/juicefs/README.md): architecture, read/write paths, cache system, transactions, and slice compaction.
- [BrewFS/JuiceFS gap analysis](doc/gap/README.md): module-by-module gaps and iteration roadmap.
- [Performance roadmap](doc/performance/perf-optimization-roadmap.md): current tuning targets and validation expectations.

Use these notes to understand where BrewFS intentionally differs from JuiceFS, where it is still catching up, and which benchmark scenarios are used to guard against regressions.

### Latest Local Perf Snapshot

The following snapshot was collected on 2026-06-15 with the full Docker perf
runners under `docker/compose-xfstests/`:

```bash
bash docker/compose-xfstests/run_redis_perf.sh --s3 --writeback-throughput-profile
bash docker/compose-xfstests/run_juicefs_perf.sh --writeback-throughput-profile
```

Artifacts:

- BrewFS: `docker/compose-xfstests/artifacts/perf-run-1781568047-20894`
- JuiceFS: `docker/compose-xfstests/artifacts/juicefs-perf-run-1781565450-27081`

All default perf tools passed on both filesystems.

Wall seconds include close/flush tail. Fio bandwidth reports the active IO
window, so both columns are useful when diagnosing writeback behavior. This
snapshot used fio `direct=0`, as recorded in the generated fio JSON. The same
JuiceFS command's terminal output, which is not captured in the artifact
directory, emitted slow S3 PUT, disk-cache write timeout, direct-upload
fallback, read context-cancel, and slow-flush warnings during write-heavy and
mixed phases, so the JuiceFS numbers below are the local same-cycle comparison
rather than an ideal upper bound. In this run, JuiceFS reported many 30s
disk-cache/S3 stalls and roughly 90s flush tails during `seqwrite` and
`randwrite`; use the active fio bandwidth and p99 columns together with wall
time when judging writeback behavior.

| Workload | BrewFS wall | JuiceFS wall | BrewFS fio BW | JuiceFS fio BW | BrewFS/JuiceFS BW | BrewFS p99 | JuiceFS p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `fio-bigwrite` | 1s | 1s | W 1.05 GiB/s | W 3.16 GiB/s | W 0.33x | W 40.6ms | W 17.4ms |
| `fio-bigread` | 3s | 0s | R 536.4 MiB/s | R 2.36 GiB/s | R 0.22x | R 94.9ms | R 42.7ms |
| `fio-seqread` | 61s | 60s | R 1.77 GiB/s | R 2.44 GiB/s | R 0.73x | R 3.3ms | R 1.5ms |
| `fio-seqwrite` | 139s | 161s | W 73.9 MiB/s | W 258.3 MiB/s | W 0.29x | W 15.4ms | W 329.3ms |
| `fio-randread` | 60s | 61s | R 704.2 MiB/s | R 3.22 GiB/s | R 0.21x | R 120.1ms | R 7.0ms |
| `fio-randwrite` | 142s | 153s | W 135.9 MiB/s | W 285.0 MiB/s | W 0.48x | W 37.5ms | W 333.4ms |
| `fio-randrw` | 125s | 61s | R 205.2 / W 93.7 MiB/s | R 161.0 / W 73.6 MiB/s | R 1.27x / W 1.27x | R 320.9ms / W 9.1ms | R 826.3ms / W 10.9ms |

Metadata comparison from `metaperf`:

| Operation | BrewFS | JuiceFS | BrewFS/JuiceFS |
| --- | ---: | ---: | ---: |
| `create` | 1069.2 ops/s | 1325.0 ops/s | 0.81x |
| `open` | 9785.6 ops/s | 23525.4 ops/s | 0.42x |
| `stat` | 1025756.1 ops/s | 1033061.3 ops/s | 0.99x |
| `readdir` | 65464.2 ops/s | 90966.0 ops/s | 0.72x |
| `rename` | 1907.2 ops/s | 2672.8 ops/s | 0.71x |

The full `metaperf` tool wall time was `405s` on BrewFS and `275s` on JuiceFS.
BrewFS total metadata wall remains far above JuiceFS despite matching `stat`
throughput; `open`, `create`, `readdir`, and `rename` remain metadata-cache and
Redis-roundtrip targets.

Additional 2026-06-16 short matrix:

The full default profile can spend a long time in writeback tail on this local
RustFS setup, so the following same-cycle comparison keeps the same profile
shape but shortens fio work to make all core scenarios repeatable in one run.
Both filesystems used fio `direct=0`, `fio-big*` size `64m`,
`fio-seq*`/`fio-rand*`/`fio-randrw` size `128m`, 5s time-based windows for
seq/random workloads, `metaperf -t 8`, S3 writeback profiles, open-cache
`1s/65536`, and cold-read prefill drain/remount/cache-clear.

Artifacts:

- BrewFS: `docker/compose-xfstests/artifacts/perf-run-1781587688-20926`
- JuiceFS: `docker/compose-xfstests/artifacts/juicefs-perf-run-1781588148-5455`

All selected tools passed: `fio-bigwrite fio-bigread fio-seqread fio-seqwrite
fio-randread fio-randwrite fio-randrw metaperf`. The throughput profile now
enables post-write drain accounting by default for both filesystems. BrewFS
waits on `.stats` `pending/dirty/buffer_dirty`; JuiceFS waits on its
staging/uploading counters after `sync`. The `tool+drain` columns below add
`perf-summary.tsv` tool wall time to `post-write-drain.tsv` so writeback tail
cost is visible without contaminating the next workload.

| Workload | BrewFS tool+drain | JuiceFS tool+drain | BrewFS post-drain | JuiceFS post-drain | BrewFS active BW | JuiceFS active BW | BrewFS/JuiceFS BW |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `fio-bigwrite` | 5s | 6s | 4s | 5s | W 1015.9 MiB/s | W 3.27 GiB/s | W 0.30x |
| `fio-bigread` | 1s | 1s | n/a | n/a | R 672.8 MiB/s | R 2.22 GiB/s | R 0.30x |
| `fio-seqread` | 6s | 6s | n/a | n/a | R 1.76 GiB/s | R 2.44 GiB/s | R 0.72x |
| `fio-seqwrite` | 89s | 79s | 36s | 61s | W 1.40 GiB/s | W 970.5 MiB/s | W 1.48x |
| `fio-randread` | 6s | 6s | n/a | n/a | R 598.0 MiB/s | R 3.11 GiB/s | R 0.19x |
| `fio-randwrite` | 121s | 193s | 0s | 120s | W 1.63 GiB/s | W 2.33 GiB/s | W 0.70x |
| `fio-randrw` | 29s | 61s | 0s | 55s | R 1009.3 / W 461.1 MiB/s | R 1.18 GiB/s / W 548.8 MiB/s | R 0.84x / W 0.84x |

Short-matrix metadata comparison:

| Operation | BrewFS | JuiceFS | BrewFS/JuiceFS |
| --- | ---: | ---: | ---: |
| `create` | 2717.3 ops/s | 1350.3 ops/s | 2.01x |
| `open` | 10152.6 ops/s | 23266.6 ops/s | 0.44x |
| `stat` | 1018853.7 ops/s | 1015568.8 ops/s | 1.00x |
| `readdir` | 107258.0 ops/s | 92098.7 ops/s | 1.16x |
| `rename` | 1812.2 ops/s | 2663.1 ops/s | 0.68x |

This run confirms the current shape of the gap: BrewFS can match or exceed
JuiceFS on active sequential write, `create`, `stat`, and `readdir`, while
still trailing on big/cold/random read bandwidth, active random-write bandwidth,
and metadata `open`/`rename`. The new drain accounting changes the interpretation
of the write gap: `seqwrite` is close end-to-end (`89s` BrewFS versus `79s`
JuiceFS) despite BrewFS spending more time inside the fio tool; `randwrite` and
`randrw` now show BrewFS finishing sooner end-to-end on this local RustFS run
because JuiceFS leaves large post-write drain tails (`120s` and `55s`). BrewFS
still needs code work on foreground random-write waits (`121s` tool wall) and
read throughput, rather than more blind pending-watermark tuning. JuiceFS also
reported many 30s disk-cache write timeouts and slow S3 PUTs during the write
heavy phases, so active bandwidth, tool wall, and post-drain should continue to
be read together.

Focused 2026-06-16 read-path raw-fallback check:

This focused check validates the LZ4 raw-fallback read optimization. BrewFS now
reuses raw object payload buffers when an object has no compression header,
instead of copying the 4MiB block into a fresh `Vec<u8>` before populating the
block cache. The comparison used the same writeback throughput profile and the
same short read/mixed sizes as the matrix above:

```bash
PERF_LOG_TO_CONSOLE=false CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_DEBUG=0 \
  PERF_FIO_BIGREAD_SIZE=64m PERF_FIO_SEQREAD_SIZE=128m \
  PERF_FIO_RANDREAD_SIZE=128m PERF_FIO_RANDRW_SIZE=128m \
  PERF_FIO_RUNTIME=5 PERF_FIO_POST_WRITE_DRAIN_TIMEOUT_SECS=300 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-bigread fio-seqread fio-randread fio-randrw"

PERF_LOG_TO_CONSOLE=false \
  PERF_FIO_BIGREAD_SIZE=64m PERF_FIO_SEQREAD_SIZE=128m \
  PERF_FIO_RANDREAD_SIZE=128m PERF_FIO_RANDRW_SIZE=128m \
  PERF_FIO_RUNTIME=5 PERF_FIO_POST_WRITE_DRAIN_TIMEOUT_SECS=300 \
  bash docker/compose-xfstests/run_juicefs_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-bigread fio-seqread fio-randread fio-randrw"
```

Artifacts:

- BrewFS previous lz4 short matrix:
  `docker/compose-xfstests/artifacts/perf-run-1781587688-20926`
- BrewFS raw-fallback candidate:
  `docker/compose-xfstests/artifacts/perf-run-1781589987-5483`
- JuiceFS dev reference, cloned from `juicedata/juicefs` at `fd52b9a`:
  `docker/compose-xfstests/artifacts/juicefs-perf-run-1781590204-7256`

| Workload | BrewFS previous | BrewFS raw-fallback | Delta | JuiceFS dev | BrewFS/JuiceFS |
| --- | ---: | ---: | ---: | ---: | ---: |
| `fio-bigread` | R 672.8 MiB/s | R 679.9 MiB/s | +1.1% | R 2.26 GiB/s | R 0.29x |
| `fio-seqread` | R 1.76 GiB/s | R 1.71 GiB/s | -2.7% | R 2.68 GiB/s | R 0.64x |
| `fio-randread` | R 598.0 MiB/s | R 786.7 MiB/s | +31.6% | R 3.23 GiB/s | R 0.24x |
| `fio-randrw` | R 751.3 / W 332.1 MiB/s focused baseline | R 727.1 / W 320.9 MiB/s | R -3.2% / W -3.4% | R 1.04 GiB/s / W 489.2 MiB/s | R 0.68x / W 0.66x |

`fio-randrw` uses the earlier focused BrewFS baseline
`docker/compose-xfstests/artifacts/perf-run-1781587228-9218` because the full
short matrix's mixed result was inflated by run-order noise. The candidate's
post-write drain was `4s`; the JuiceFS dev reference drained for `67s` and
emitted several `context canceled` read/compact warnings during random read and
mixed phases. The accepted conclusion is narrow: raw fallback reuse is a real
random-read improvement and does not show a large mixed-workload regression, but
it does not explain the remaining `bigread`/`seqread` gap. The next read-path
target remains FUSE/read scheduling and object GET tail behavior.

Session-gated readahead validation, 2026-06-16:

This round follows JuiceFS's read-session discipline more closely: a first
non-zero-offset read is treated as random until the next contiguous read
confirms a stream, and VFS prefetch tasks are submitted only for confirmed
sessions. A too-aggressive intermediate version used the full session `ahead`
window for each prefetch and reproducibly dropped focused `fio-seqread` to
`1.26 GiB/s`; the accepted version keeps the gating but caps each prefetch at
`max(read_len, block_size)`, restoring focused `fio-seqread` to `2.20 GiB/s`
in `docker/compose-xfstests/artifacts/perf-run-1781594242-13781`.

Artifacts:

- BrewFS accepted run:
  `docker/compose-xfstests/artifacts/perf-run-1781594407-19392`
- JuiceFS same-parameter run, `juicedata/juicefs` at `fd52b9a`:
  `docker/compose-xfstests/artifacts/juicefs-perf-run-1781593000-13808`

Both runs used fio `direct=0`, `fio-big*` size `64m`,
`fio-seq*`/`fio-rand*`/`fio-randrw` size `128m`, 5s time-based windows for
seq/random workloads, `metaperf -t 30`, Redis metadata, RustFS S3, writeback
profiles, open-cache `1s/65536`, and cold-read prefill drain/remount/cache-clear.

| Workload | BrewFS tool+drain | JuiceFS tool+drain | BrewFS post-drain | JuiceFS post-drain | BrewFS active BW | JuiceFS active BW | BrewFS/JuiceFS BW |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `fio-bigwrite` | 5s | 7s | 5s | 6s | W 1.07 GiB/s | W 3.05 GiB/s | W 0.35x |
| `fio-bigread` | 1s | 1s | n/a | n/a | R 1.29 GiB/s | R 2.18 GiB/s | R 0.59x |
| `fio-seqread` | 5s | 5s | n/a | n/a | R 2.18 GiB/s | R 2.47 GiB/s | R 0.88x |
| `fio-seqwrite` | 92s | 85s | 32s | 63s | W 1.42 GiB/s | W 1.02 GiB/s | W 1.39x |
| `fio-randread` | 5s | 5s | n/a | n/a | R 1.68 GiB/s | R 3.15 GiB/s | R 0.53x |
| `fio-randwrite` | 135s | 173s | 0s | 102s | W 1.74 GiB/s | W 2.24 GiB/s | W 0.78x |
| `fio-randrw` | 36s | 62s | 2s | 57s | R 1.37 GiB/s / W 653.6 MiB/s | R 1.13 GiB/s / W 525.6 MiB/s | R 1.21x / W 1.24x |

Read-path movement versus the prior full short matrix
`docker/compose-xfstests/artifacts/perf-run-1781587688-20926`:

| Workload | Previous BrewFS | Accepted BrewFS | Delta |
| --- | ---: | ---: | ---: |
| `fio-bigread` | R 672.8 MiB/s | R 1.29 GiB/s | +95.6% |
| `fio-seqread` | R 1.76 GiB/s | R 2.18 GiB/s | +23.9% |
| `fio-randread` | R 598.0 MiB/s | R 1.68 GiB/s | +188.4% |
| `fio-randrw` | R 1009.3 / W 461.1 MiB/s | R 1.37 GiB/s / W 653.6 MiB/s | R +39.5% / W +41.7% |

Metadata comparison from the same run:

| Operation | BrewFS | JuiceFS | BrewFS/JuiceFS |
| --- | ---: | ---: | ---: |
| `create` | 3470.4 ops/s | 1263.5 ops/s | 2.75x |
| `open` | 10106.8 ops/s | 23382.5 ops/s | 0.43x |
| `stat` | 1021723.6 ops/s | 1022365.7 ops/s | 1.00x |
| `readdir` | 109145.2 ops/s | 93126.8 ops/s | 1.17x |
| `rename` | 1915.0 ops/s | 2652.3 ops/s | 0.72x |

The accepted result moves BrewFS much closer on read throughput, especially
`fio-seqread` at `0.88x` of this JuiceFS run and `fio-bigread` at `0.59x`.
The remaining gaps are active `fio-randread` bandwidth (`0.53x`), active
`fio-randwrite` bandwidth (`0.78x`), metadata `open`/`rename`, and BrewFS
memory growth during metaperf. Write-heavy wall time still needs separate
work: BrewFS spends more time inside the fio tool, while this JuiceFS run spent
large tails in post-write drain and emitted local cache write timeout warnings.

Demand-read SliceState bypass validation, 2026-06-16:

This focused check removes the per-read `FileReader` `SliceState` reservation
and completion path for committed demand reads. Demand data still flows through
`DataFetcher` and the per-handle chunk slice metadata cache; writer commits
still invalidate that metadata cache before subsequent reads. The hypothesis was
that cached random reads were paying extra lock/list/notification overhead even
after the object blocks were already served from BrewFS's block cache.

Artifacts:

- BrewFS baseline before the bypass:
  `docker/compose-xfstests/artifacts/perf-run-1781613121-5053`
- BrewFS bypass repeat A:
  `docker/compose-xfstests/artifacts/perf-run-1781633772-21427`
- BrewFS bypass repeat B:
  `docker/compose-xfstests/artifacts/perf-run-1781634035-24683`
- Fresh JuiceFS same-parameter reference:
  `docker/compose-xfstests/artifacts/juicefs-perf-run-1781634158-8289`

All runs used the same focused command shape as the read-path checks above:
fio `direct=0`, `fio-bigread` size `64m`,
`fio-seqread`/`fio-randread`/`fio-randrw` size `128m`, 5s time-based windows
for seq/random workloads, Redis metadata, RustFS S3, cold-read remount/cache
clear, and writeback throughput profiles.

| Workload | BrewFS baseline | BrewFS bypass A | BrewFS bypass B | Fresh JuiceFS | B/JuiceFS |
| --- | ---: | ---: | ---: | ---: | ---: |
| `fio-bigread` | R 1.24 GiB/s, p99 54.3ms | R 1.25 GiB/s, p99 54.8ms | R 1.21 GiB/s, p99 63.2ms | R 2.24 GiB/s, p99 46.4ms | R 0.54x |
| `fio-seqread` | R 2.00 GiB/s, p99 2.7ms | R 2.16 GiB/s, p99 2.5ms | R 2.02 GiB/s, p99 4.5ms | R 2.37 GiB/s, p99 2.1ms | R 0.85x |
| `fio-randread` | R 1.86 GiB/s, p99 24.3ms | R 1.88 GiB/s, p99 22.9ms | R 1.88 GiB/s, p99 24.0ms | R 3.11 GiB/s, p99 11.1ms | R 0.60x |
| `fio-randrw` | R 982.1 / W 446.0 MiB/s, p99 R 38.5ms / W 9.2ms | R 982.7 / W 442.4 MiB/s, p99 R 45.9ms / W 59.0ms | R 932.7 / W 426.3 MiB/s, p99 R 41.7ms / W 9.0ms | R 898.6 / W 413.4 MiB/s, p99 R 204.5ms / W 15.1ms | R 1.04x / W 1.03x |

Internal BrewFS counters show the intended hot-path effect on `fio-randread`:
average FUSE read latency moved from `35.49us` to `32.84us` and `32.19us`
across the two bypass runs, while `brewfs_read_range_gets_total` stayed `0`
and block-cache hits stayed high. The accepted conclusion is narrow: removing
demand `SliceState` reservations is a small cached-random-read improvement, not
the main explanation for the remaining JuiceFS gap. Repeat A had a mixed-write
p99 outlier, so future read-path changes should continue to include `randrw`
guard runs. The next likely read bottlenecks are DataFetcher/cache-copy overhead
inside each 128KiB FUSE read and kernel/FUSE request granularity.

Focused metadata diagnostics:

- Zero-byte `metaperf` isolates metadata from 4KiB small-file writeback. The
  diagnostic shows BrewFS create is not the pure-metadata problem: zero-byte
  create is already faster than JuiceFS, while `open`, `readdir`, and `rename`
  still trail JuiceFS. Default 4KiB create remains slower because it emits tens
  of thousands of tiny staged/uploaded writeback slices.
- Artifacts: BrewFS zero-byte baseline
  `docker/compose-xfstests/artifacts/perf-run-1781570960-17324`, BrewFS root
  fast-path candidate `docker/compose-xfstests/artifacts/perf-run-1781572032-21175`,
  JuiceFS zero-byte comparison
  `docker/compose-xfstests/artifacts/juicefs-perf-run-1781571279-16802`, and
  BrewFS default 4KiB candidate
  `docker/compose-xfstests/artifacts/perf-run-1781572378-29881`.

| Zero-byte `metaperf` operation | BrewFS before | BrewFS root fast path | JuiceFS | New BrewFS/JuiceFS |
| --- | ---: | ---: | ---: | ---: |
| wall | 189s | 188s | 192s | 0.98x |
| `create` | 4975.1 ops/s | 5151.3 ops/s | 4462.4 ops/s | 1.15x |
| `open` | 9838.9 ops/s | 10168.5 ops/s | 23624.0 ops/s | 0.43x |
| `stat` | 1025512.4 ops/s | 1026596.5 ops/s | 1018349.2 ops/s | 1.01x |
| `readdir` | 66733.4 ops/s | 65632.0 ops/s | 90681.7 ops/s | 0.72x |
| `rename` | 1925.7 ops/s | 1955.0 ops/s | 2676.4 ops/s | 0.73x |

For default 4KiB focused `metaperf`, the same root fast path moved `open` to
`10142.3 ops/s` versus nearby focused baselines at `9698.1`, `9776.3`, and
`9681.3 ops/s`; `create` moved to `1100.3 ops/s`; `rename` moved to
`1935.6 ops/s`; `readdir` regressed to `64422.5 ops/s`; wall time rose to
`198s` because the fixed 30s create window completed more files and left a
slightly larger cleanup/writeback tail. The run still emitted `35801` S3 PUTs
for only `146.6 MiB` of staged data, so the next write-path optimization must
reduce tiny explicit-flush object amplification rather than tune root/open
permission checks again.

Latest accepted BrewFS tuning:

- `src/chunk/compress.rs` now returns borrowed data for raw/no-header
  decompression, and `src/chunk/store.rs` uses `decompress_bytes(Bytes)` so LZ4
  raw fallback objects can move from object response to block cache without an
  extra 4MiB copy. This follows the JuiceFS cached-store pattern of keeping the
  downloaded object/page buffer as the cache payload when no transform is
  required. TDD first added `test_raw_data_without_header_is_borrowed`, which
  failed against the old `Vec<u8>` API; the final test set adds
  `test_decompress_bytes_reuses_raw_buffer`.

  Verification passed:
  `cargo test -p brewfs chunk::compress::tests --lib`,
  `cargo test -p brewfs chunk::store::tests --lib`,
  `cargo fmt --all -- --check`, and `cargo check -p brewfs`. Focused compose
  validation `docker/compose-xfstests/artifacts/perf-run-1781589987-5483`
  moved lz4 `fio-randread` from `598.0` to `786.7 MiB/s` while keeping focused
  `randrw` close to the prior focused baseline (`727.1/320.9 MiB/s` versus
  `751.3/332.1 MiB/s`). `bigread` and `seqread` stayed essentially flat, so the
  remaining read gap should be attacked in request scheduling and GET tail
  control rather than raw-fallback copies.
- `src/chunk/store.rs` no longer duplicates a decompressed full-block read into
  the 64KiB page cache before inserting the same block into the full block
  cache. `ObjectBlockStore::read_range` always checks the block cache first, so
  the old compressed full-read path copied and cached the same 4MiB payload
  twice without helping the next read while the block cache entry was present.
  This follows the JuiceFS direction of caching the loaded block once and using
  range/page cache for true range misses. The TDD guard verifies compressed
  full reads skip page-cache population while subsequent range reads still hit
  the block cache. `cargo test -p brewfs --lib chunk::store::tests` and
  `cargo fmt --all --check` passed.

  Full short-matrix validation
  `docker/compose-xfstests/artifacts/perf-run-1781585469-32747` moved BrewFS
  `fio-randread` from `590.7` to `714.1 MiB/s`, `fio-seqread` from `1.74` to
  `1.82 GiB/s`, `fio-randwrite` from `1.35` to `1.64 GiB/s`, and `fio-randrw`
  from R `995.8` / W `454.8 MiB/s` to R `1.06 GiB/s` / W `501.1 MiB/s`.
  Mixed read/write wall improved from `29s` to `25s`, and p99 moved from
  R `58.5ms` / W `11.1ms` to R `49.0ms` / W `4.8ms`. `fio-bigread` bandwidth
  improved from `535.0` to `568.9 MiB/s`, but its p99 regressed from `94.9ms`
  to `227.5ms`, so the next read-path iteration should focus on GET tail
  control rather than more cache duplication changes. The same run's `metaperf`
  wall was polluted by prior `randrw` dirty tail, but standalone
  `docker/compose-xfstests/artifacts/perf-run-1781586159-28465` finished
  `metaperf` in `79s` with `open` at `10100.7 ops/s` and no residual files.
- `src/vfs/fs/mod.rs` now records open-file-cache state for opens that already
  have a trusted freshly-created attribute, and `src/meta/client/mod.rs`
  refreshes an existing open-file-cache entry for timestamp-only `setattr`
  instead of invalidating it. The TDD guards cover both paths and the existing
  size-mutation invalidation test still passes. In short-matrix `metaperf`
  `docker/compose-xfstests/artifacts/perf-run-1781581281-3995`, open-cache
  hit ratio improved from the previous focused run's roughly 74.7% to roughly
  96.1% (`119000` hits and `4800` misses in the metaperf window), reducing
  Redis fresh-stat pressure. This is accepted as backend-load reduction, not as
  an `open` throughput fix: `open` stayed essentially flat at `10099.8 ops/s`
  versus `10060.0 ops/s` in
  `docker/compose-xfstests/artifacts/perf-run-1781579177-18631`, while JuiceFS
  remains at about `23.5k ops/s`.
- `src/vfs/handles.rs` now returns up to 256 directory entries per
  readdir/readdirplus batch instead of 50. This stays comfortably below common
  FUSE reply-buffer limits while cutting userspace pagination for metaperf's
  large directory scans. Focused default 4KiB `metaperf`
  `docker/compose-xfstests/artifacts/perf-run-1781579177-18631` moved
  `readdir` from `65443.4` to `110983.7 ops/s`, passing the same-cycle JuiceFS
  focused result of `91338.6 ops/s`. The same run kept `create` at
  `3053.1 ops/s`, `open` at `10060.0 ops/s`, `stat` at `1031608.2 ops/s`, and
  `rename` at `1944.4 ops/s`; the small create/open movement is within focused
  run noise, while the readdir win is large and targeted.

  | Focused 4KiB `metaperf` | BrewFS sparse-zero `perf-run-1781575796-24729` | BrewFS readdir batch `perf-run-1781579177-18631` | JuiceFS `juicefs-perf-run-1781576125-3207` |
  | --- | ---: | ---: | ---: |
  | wall | 189s | 189s | 206s |
  | `create` | 3059.3 ops/s | 3053.1 ops/s | 1361.4 ops/s |
  | `open` | 10161.1 ops/s | 10060.0 ops/s | 23607.3 ops/s |
  | `stat` | 1023493.1 ops/s | 1031608.2 ops/s | 1003843.3 ops/s |
  | `readdir` | 65443.4 ops/s | 110983.7 ops/s | 91338.6 ops/s |
  | `rename` | 1944.5 ops/s | 1944.4 ops/s | 2688.6 ops/s |
- `src/vfs/fs/mod.rs` now treats small all-zero writes into sparse ranges as a
  metadata-only sparse extension. This follows the same performance idea as
  JuiceFS holes, but keeps BrewFS away from `slice_id=0` metadata because BrewFS
  slice precedence depends on monotonically increasing slice ids. Zero
  overwrites of committed data still fall back to a real slice upload, and TDD
  guards cover cached EOF sparse extension, normal handle flush/close, presized
  sparse ranges, and committed-data overwrite semantics.

  Focused default 4KiB `metaperf` changed the hot small-file create path from
  `35801` S3 PUTs to `1` S3 PUT and moved BrewFS `create` from `1100.3` to
  `3059.3 ops/s`. A same-cycle focused JuiceFS run is included for direction;
  this is not a replacement for the full perf table above.

  | Focused 4KiB `metaperf` | BrewFS before `perf-run-1781572378-29881` | BrewFS sparse-zero `perf-run-1781575796-24729` | JuiceFS `juicefs-perf-run-1781576125-3207` |
  | --- | ---: | ---: | ---: |
  | wall | 198s | 189s | 206s |
  | `create` | 1100.3 ops/s | 3059.3 ops/s | 1361.4 ops/s |
  | `open` | 10142.3 ops/s | 10161.1 ops/s | 23607.3 ops/s |
  | `stat` | 1018832.6 ops/s | 1023493.1 ops/s | 1003843.3 ops/s |
  | `readdir` | 64422.5 ops/s | 65443.4 ops/s | 91338.6 ops/s |
  | `rename` | 1935.6 ops/s | 1944.5 ops/s | 2688.6 ops/s |
  | S3 PUTs / writeback stages | 35801 / 35801 | 1 / 1 | n/a |

  Direct fio regression guard
  `docker/compose-xfstests/artifacts/perf-run-1781576500-15551` passed
  `fio-seqwrite fio-randwrite fio-randrw` with `PERF_FIO_DIRECT=1`:
  `seqwrite` W 73.01 MiB/s, `randwrite` W 52.33 MiB/s, and `randrw`
  R 116.86 / W 52.45 MiB/s. Next focused targets are still JuiceFS gaps in
  `open`, `readdir`, `rename`, and direct random-write/mixed-write bandwidth.
- `src/fuse/mod.rs` now lets uid 0 bypass cached-inode ancestor search
  permission checks before FUSE `open`, matching Linux root search semantics and
  avoiding repeated `get_paths`/ancestor checks in root-run perf workloads. The
  TDD guard verifies root can open through a non-searchable parent while the
  existing non-root cached-inode open rejection still passes. Focused
  zero-byte `metaperf` moved `open` from `9838.9` to `10168.5 ops/s`; focused
  default 4KiB `metaperf` moved `open` to `10142.3 ops/s`, with the caveat that
  `readdir` regressed slightly and the default wall tail remains dominated by
  small-file writeback.
- `src/chunk/cache.rs` now promotes disk-cache hits directly back into the hot
  in-memory cache while the hot tier is below 80% of its byte budget. This keeps
  repeated random reads from paying local disk read, CRC verification, and cache
  file timestamp update costs after the first disk hit, while still avoiding
  cache pollution once memory is mostly full. The TDD guard verifies both the
  positive promotion path and the budget stop condition. Focused validation
  `docker/compose-xfstests/artifacts/perf-run-1781557114-2683` moved
  `fio-randread` to 882.1 MiB/s and `fio-randrw` to R 319.3 / W 142.2 MiB/s.
  The current full run above showed `fio-randrw` wall at 125s, but active
  mixed bandwidth settled at R 205.2 / W 93.7 MiB/s with R p99 320.9ms. Pure
  `randread` also remained far behind JuiceFS at 704.2 MiB/s, so the next
  iteration should focus on writeback tail, random-read miss/tail cost, and
  metadata open/create roundtrips rather than more promotion-threshold tuning.
- `src/vfs/io/writer.rs` now stages cached-only writeback data before starting
  its remote upload, but only for background writeback upload plans whose write
  origin is `CachedOnly`. Foreground, normal-origin, and mixed-origin plans keep
  the previous concurrent best-effort staging/upload behavior. This keeps the
  S3 writeback durability contract tighter for cached writeback without applying
  the slower stage-first policy to direct/normal IO.
- `src/vfs/io/writer.rs` now keeps cached sub-block tails writable during
  `tooMany` slice pressure when the current backlog already has enough
  block-sized slices to drain first. This narrows the previous random half-chunk
  `tooMany` selection so cached writeback tails are less likely to be sealed
  early while larger upload work is already in flight.
- `src/vfs/io/reader.rs` now fills the caller result buffer directly for
  single-span reads by using `DataFetcher::read_at_into`. This avoids building a
  temporary `Bytes` span and then copying it into the final read buffer for the
  common one-chunk read path.
- `src/vfs/cache/write_back.rs` caches already-created dirty directories in
  `FsWriteBackCache`, avoiding repeated `create_dir_all` checks for every
  staged writeback batch under the same inode/chunk directory.
- `docker/compose-xfstests/run_redis_perf.sh --writeback-throughput-profile`
  now defaults `BREWFS_WRITEBACK_UPLOAD_CONCURRENCY=6` instead of 3. Focused
  validation showed better writeback wall time, and the full run reduced
  `fio-randrw` wall from 166s to 161s, `fio-randwrite` wall from 144s to 143s,
  and full `metaperf` wall from 449s to 392s. The trade-off is lower `randrw`
  active bandwidth than the previous BrewFS snapshot, so the next iteration
  should target mixed-workload close/flush tail without adding small-object
  upload amplification.
- `src/chunk/cache_integrity.rs` and `src/chunk/cache.rs` now decode verified
  disk-cache files into `Bytes` slices over the owned file buffer instead of
  copying the payload into a fresh `Vec<u8>` and then copying again during hot
  cache promotion. The TDD guard asserts framed and legacy cache loads reuse the
  original payload pointer. Quick profiling of `fio-randread` improved from
  644.4 MiB/s / 152.0ms p99 to 700.2 MiB/s / 122.2ms p99
  (`tools/perf/results/20260615-174959`), and the focused compose read run
  `docker/compose-xfstests/artifacts/perf-run-1781546223-31195` reached
  `fio-randread` 833.2 MiB/s. The full same-cycle run above still showed
  `fio-randread` at 713.3 MiB/s because the local S3/writeback cycle was noisy;
  follow-up work should stabilize the test environment and then attack the
  remaining CRC/FUSE scheduling cost.
- Full BrewFS perf artifact:
  `docker/compose-xfstests/artifacts/perf-run-1781547390-15411`.
- Compared with the previous accepted BrewFS run
  `docker/compose-xfstests/artifacts/perf-run-1781538386-3498`,
  `fio-seqread` moved from 1.71 GiB/s to 1.79 GiB/s, `fio-seqwrite` moved from
  148s / 75.5 MiB/s to 140s / 73.0 MiB/s with p99 improving from 17.7ms to
  15.5ms, `fio-randwrite` moved from 143s / 114.0 MiB/s to 137s /
  115.8 MiB/s, and `fio-randrw` moved from 161s / R 296.4 / W 132.9 MiB/s to
  159s / R 331.3 / W 147.6 MiB/s. `fio-randread` regressed from 784.1 MiB/s /
  44.8ms p99 to 713.3 MiB/s / 112.7ms p99 in the full run despite the focused
  read/profile improvement, so the next iteration should separate environment
  noise from the remaining cache-hit CPU path.
- Focused validation artifacts:
  `docker/compose-xfstests/artifacts/perf-run-1781537092-760` for the
  `BREWFS_WRITEBACK_UPLOAD_CONCURRENCY=6` direct=0 writeback check,
  `docker/compose-xfstests/artifacts/perf-run-1781537714-9726` for the rejected
  concurrency=4 comparison, and
  `docker/compose-xfstests/artifacts/perf-run-1781540944-16637` as the direct=1
  `fio-seqwrite fio-randwrite fio-randrw` guard.
- The next bottlenecks are pure write active bandwidth, cold/random read
  bandwidth, metadata open/create throughput, and mixed-workload tail:
  sequential write is still about 0.29x of the same-cycle JuiceFS write
  bandwidth, random write is about 0.48x, `fio-randread` is about 0.21x, and
  `fio-randrw` active bandwidth still exceeds this noisy local JuiceFS run but
  takes 125s wall time versus JuiceFS at 61s.

Latest rejected tuning checks:

2026-06-16 hot-path micro-checks:

The following small candidates were tested against accepted short baseline
`docker/compose-xfstests/artifacts/perf-run-1781594407-19392` and then reverted.
Their transient artifacts were removed after comparison to keep local storage
bounded unless the artifact is explicitly listed below. All runs used the
writeback throughput profile with fio `direct=0`, 64MiB `fio-big*` data,
128MiB seq/random data, and 5s timed seq/random windows.

| Candidate | Focused tools | Positive signal | Regression | Decision |
| --- | --- | --- | --- | --- |
| Disk-cache `store_with_permit` `write_vectored` | `fio-bigwrite fio-seqwrite fio-randwrite fio-randrw` | `randwrite` wall improved 135s -> 128s and PUTs/GiB fell 23.5% | bigwrite BW -5.5%, randwrite BW -6.2%, randwrite p999 41ms -> 1250ms, seqwrite wall 60s -> 89s, randrw BW -3% to -5% | reject: stage syscall shape is not the bottleneck |
| Single-chunk dirty-overlay fast path | `fio-bigwrite fio-seqwrite fio-randwrite fio-randrw` | `randwrite` wall improved 135s -> 117s and PUTs/GiB fell 17.4% | bigwrite BW -5.6%, randrw read/write BW -3.7%/-5.1%, randread-tail-adjacent mixed p99 worsened, seqwrite wall 60s -> 83s | reject: mixed/seqwrite regression outweighs object-shape gain |
| Skip disk-cache atime touch until eviction pressure | `fio-bigread fio-seqread fio-randread fio-randrw` | pure `randread` BW improved 1.68 GiB/s -> 1.84 GiB/s | seqread BW -9.5%, bigread BW -3.7%, randrw read/write BW -36.9%/-37.9%, randrw write p99 9.5ms -> 78.1ms | reject: pure read gain is not acceptable with randrw collapse |
| Raise writeback upload concurrency 6 -> 8 | `fio-bigwrite fio-seqwrite fio-randwrite fio-randrw` | seqwrite post-drain improved 32s -> 14s and randrw wall fell 34s -> 31s | bigwrite BW -8.8%, seqwrite BW -4.9%, randrw read/write BW -4.9%/-6.1%, randwrite p999 41ms -> 225ms | reject: more upload workers reduce drain but hurt active throughput and tail latency |
| Background disk-cache atime touch | `fio-bigread fio-seqread fio-randread fio-randrw` | pure `randread` BW improved 1.68 GiB/s -> 1.88 GiB/s | bigread BW -1.5%, seqread BW -9.4%, randrw read/write BW -34.2%/-35.1%, randrw write p999 10.6ms -> 149.9ms | reject: atime deferral still destabilizes mixed reads/writes |
| Disable disk-cache checksum verification | `fio-bigread fio-seqread fio-randread fio-randrw` | pure `randread` BW improved 1.68 GiB/s -> 1.90 GiB/s | bigread BW -3.0%, seqread BW -9.1%, randrw read/write BW -33.8%/-34.6%, randrw write p999 10.6ms -> 187.7ms | reject: checksum hotspot is real, but disabling verification is not a balanced profile |
| DataFetcher single-block direct read plan | `fio-bigread fio-seqread fio-randread fio-randrw` | no material gain; seqread p99 improved 2.70ms -> 2.24ms versus `perf-run-1781613121-5053` | randread BW fell 1.86 -> 1.81 GiB/s and randrw fell R 982/W 446 -> R 970/W 440 MiB/s versus the latest focused baseline | reject: bypassing slice fragmentation setup is not the current read bottleneck |
| Preallocate upload aggregation `Vec` | `fio-bigwrite fio-seqwrite fio-randwrite fio-randrw` | bigwrite BW +9.1%, randwrite BW +14.3%, seqwrite BW +2.2%, randwrite p99 45.4ms -> 33.4ms | seqwrite wall 66s -> 72s, randwrite wall 117s -> 131s, randrw read/write BW -3.9%/-4.5%, randrw write p99 4.6ms -> 9.4ms | reject: active-write gain does not survive wall-time and mixed-workload guards |
| Share upload/stage chunk buffer with `Arc<[Bytes]>` | `fio-bigwrite fio-seqwrite fio-randwrite fio-randrw` | bigwrite BW +8.3%, seqwrite BW +9.2%, randwrite BW +13.0%, randwrite p99 45.4ms -> 30.0ms | seqwrite wall 66s -> 78s, randwrite wall 117s -> 123s, randrw read/write BW -4.6%/-4.7%, randrw write p99 4.6ms -> 9.5ms, randwrite PUTs/GiB +24.8% | reject: removing the Vec clone does not reduce object amplification and worsens wall/mixed guards |
| Count only writable slices for `too_many` pressure | `fio-bigwrite fio-seqwrite fio-randwrite fio-randrw` | seqwrite wall 66s -> 65s, seqwrite PUTs/GiB -6.6%, seqwrite too_many tails 37 -> 0, randwrite BW +11.7%, randwrite p99 45.4ms -> 30.3ms | randwrite wall 117s -> 127s, randwrite PUTs/GiB +32.3%, randwrite partial-tail ratio 0.831 -> 0.885, randrw write p99 4.6ms -> 9.1ms, bigwrite BW -3.4% | reject: delayed too_many pressure shifts work into smaller randwrite objects and longer close/flush tail |
| Enable compact profile defaults (`interval=2s`, `min_slice_count=3`) | `fio-bigwrite fio-seqwrite fio-randwrite fio-randrw` | bigwrite BW 911.0 MiB/s -> 1.04 GiB/s, seqwrite BW 1.29 -> 1.39 GiB/s, randwrite BW 1.56 -> 1.70 GiB/s, randrw wall 32s -> 12s | seqwrite wall 66s -> 83s, randwrite wall 117s -> 129s, randrw read/write BW 1.28 GiB/s / 600.0 MiB/s -> 845.7 / 384.1 MiB/s | reject as default: config pass-through is kept, but low-interval compaction hurts wall time and mixed throughput |
| Redis rename outcome returned to `MetaClient` | `metaperf` | standalone `metaperf` wall was 187s, but this is not comparable to the full-matrix 248s after fio pressure | vs accepted same-parameter run: create 3470.4 -> 3174.0, open 10106.8 -> 9149.6, stat 1021723.6 -> 932515.8, readdir 109145.2 -> 98537.3, rename 1915.0 -> 1863.9 ops/s | reject: avoiding the destination prelookup did not improve the target rename path; code was reverted after local CI and focused perf |

The DataFetcher single-block candidate artifact is
`docker/compose-xfstests/artifacts/perf-run-1781630677-14774`; its same-parameter
JuiceFS focused comparison is
`docker/compose-xfstests/artifacts/juicefs-perf-run-1781630997-4241`.

| Focused workload | BrewFS active BW | JuiceFS active BW | BrewFS/JuiceFS | BrewFS p99 | JuiceFS p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| `fio-bigread` | R 1.24 GiB/s | R 2.29 GiB/s | R 0.54x | R 53.2ms | R 37.5ms |
| `fio-seqread` | R 2.03 GiB/s | R 2.41 GiB/s | R 0.84x | R 2.2ms | R 2.0ms |
| `fio-randread` | R 1.81 GiB/s | R 3.07 GiB/s | R 0.59x | R 26.6ms | R 9.9ms |
| `fio-randrw` | R 969.9 / W 439.9 MiB/s | R 923.2 / W 419.5 MiB/s | R 1.05x / W 1.05x | R 44.3ms / W 9.6ms | R 206.6ms / W 13.3ms |

BrewFS drained the `fio-randrw` post-write tail in 4s in this focused run;
the local JuiceFS run drained in 66s and emitted several `readSlice` and compact
`context canceled` warnings while still completing the fio tools successfully.

The Redis rename-outcome artifact is
`docker/compose-xfstests/artifacts/perf-run-1781627417-18180`. Before that
focused perf check, the local CI gate from `agent.md` passed, including
`cargo fmt --all --check`, `cargo check --workspace`, `cargo build --workspace`,
the BrewFS no-default feature checks, `cargo test --workspace --lib --bins`, and
`cargo clippy --workspace`.

These results point away from micro-optimizing cache file writes, local overlay
allocation shape, or unconditional atime skipping. The next useful attempt
should target writeback slice batching/commit scheduling with explicit mixed
workload guards, or isolate pure read-cache improvements behind a condition
that is disabled while mixed writeback is active.

Same-window focused control, 2026-06-16:

Clean default-profile focused controls are:

- `docker/compose-xfstests/artifacts/perf-run-1781613121-5053` for
  `fio-bigread fio-seqread fio-randread fio-randrw`: `fio-bigread` 1.24 GiB/s,
  `fio-seqread` 2.00 GiB/s, `fio-randread` 1.86 GiB/s, and `fio-randrw`
  982.1 MiB/s read plus 446.0 MiB/s write.
- `docker/compose-xfstests/artifacts/perf-run-1781613484-12800` for
  `fio-bigwrite fio-seqwrite fio-randwrite fio-randrw`: `fio-bigwrite`
  911.0 MiB/s, `fio-seqwrite` 1.29 GiB/s with 66s wall, `fio-randwrite`
  1.56 GiB/s with 117s wall, and `fio-randrw` 1.28 GiB/s read plus
  600.0 MiB/s write with 32s wall.
- `docker/compose-xfstests/artifacts/perf-run-1781621038-23087` for the
  rejected compact-default check. The generated `backend.yml` contained the
  requested top-level `compact:` section, proving the config path works, but
  the same-window write guard regressed `seqwrite`, `randwrite`, and active
  `randrw` throughput.

These controls used 64MiB big fio data, 128MiB seq/random data, fio `direct=0`,
and 5s timed windows. They show the older accepted full-run baseline is noisy
for short focused comparisons; future focused candidates should be compared
against the matching same-window control first, then promoted to the full
BrewFS/JuiceFS matrix only if they beat it without material secondary
regressions.

Older FUSE unique append reuse:

```bash
PERF_LOG_TO_CONSOLE=false CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_DEBUG=0 \
PERF_FIO_BIGWRITE_SIZE=64m PERF_FIO_BIGREAD_SIZE=64m \
PERF_FIO_SEQREAD_SIZE=128m PERF_FIO_SEQWRITE_SIZE=128m \
PERF_FIO_RANDREAD_SIZE=128m PERF_FIO_RANDWRITE_SIZE=128m \
PERF_FIO_RANDRW_SIZE=128m PERF_FIO_SEQREAD_RUNTIME=5 \
PERF_FIO_SEQWRITE_RUNTIME=5 PERF_FIO_RANDREAD_RUNTIME=5 \
PERF_FIO_RANDWRITE_RUNTIME=5 PERF_FIO_RANDRW_RUNTIME=5 \
PERF_METAPERF_SECONDS=8 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw metaperf"
```

Artifacts:
`docker/compose-xfstests/artifacts/perf-run-1781583840-3547` for the BrewFS
candidate and
`docker/compose-xfstests/artifacts/juicefs-perf-run-1781584326-14116` for the
same-parameter JuiceFS comparison.

The candidate allowed an older FUSE `unique` to reuse an existing dirty slice
when the write was a non-overlapping append, while keeping the older-unique
guard for overlap writes. The focused TDD checks passed, including the writer
unit suite after the experiment, but the full short matrix showed unacceptable
regressions. It improved `fio-randwrite` active bandwidth and p99 latency by
reducing random-write slice/object amplification, yet it regressed sequential
write wall time and metadata wall time. The code was reverted; the useful next
step is a narrower coalescing design or instrumentation that separates append,
gap-fill, and overlap older-unique rejections before changing ordering behavior.

| Workload | Accepted BrewFS `perf-run-1781581281-3995` | Candidate BrewFS `perf-run-1781583840-3547` | JuiceFS `juicefs-perf-run-1781584326-14116` | Decision |
| --- | ---: | ---: | ---: | --- |
| `fio-seqwrite` wall | 49s | 79s | 21s | reject: large wall regression |
| `fio-seqwrite` active write BW | 1.42 GiB/s | 1.47 GiB/s | 1011.39 MiB/s | active BW neutral, wall worse |
| `fio-randwrite` wall | 128s | 129s | 72s | wall unchanged |
| `fio-randwrite` active write BW | 1.35 GiB/s | 1.83 GiB/s | 2.23 GiB/s | promising but insufficient |
| `fio-randwrite` write p99 | 36.438ms | 26.345ms | 10.813ms | improved |
| `fio-randrw` wall | 29s | 29s | 6s | unchanged |
| `fio-randrw` active read/write BW | 995.81 / 454.82 MiB/s | 1.04 GiB/s / 489.16 MiB/s | 1.08 GiB/s / 506.90 MiB/s | active BW closer, wall still behind |
| `metaperf` wall | 133s | 146s | 148s | reject: metadata wall regression |

The slice/object counters explain the mixed result. `fio-randwrite` slice
creates dropped from `5521` to `4103`, upload batches from `5908` to `4567`,
and S3 PUTs from `6185` to `4893`. However, `fio-seqwrite` S3 PUTs increased
from `2459` to `2869` and partial-tail uploads from `1617` to `1779`, while
`fio-randrw` S3 PUTs increased from `866` to `962` and partial-tail uploads
from `379` to `468`. That means simple older-unique append reuse helps one
random-write path but disturbs flush batching elsewhere, so it is not an
acceptable default.

Known-empty sparse inode slice-lookup skip:

```bash
PERF_LOG_TO_CONSOLE=false CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_DEBUG=0 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "metaperf"
```

Artifact:
`docker/compose-xfstests/artifacts/perf-run-1781578587-31108`.

The candidate added a local inode hint for "no visible committed slices" so
small all-zero writes into known sparse ranges could skip `meta.get_slices`.
The focused 4KiB `metaperf` run showed no meaningful gain over the accepted
sparse-zero baseline: `create` moved only from `3059.3` to `3069.8 ops/s`,
`open` slipped from `10161.1` to `10152.0 ops/s`, and Redis `LRANGE` /
`brewfs_meta_get_slices_cache_miss_total` only moved from `98600` to `98400`.
That reduction is within workload file-count variance, so the code was
reverted. A useful follow-up needs to trace the real FUSE setattr/writeback
sequence or avoid per-file empty-slice metadata queries at the MetaClient layer.

| Workload | Accepted sparse-zero `perf-run-1781575796-24729` | Candidate `perf-run-1781578587-31108` | Decision |
| --- | ---: | ---: | --- |
| `metaperf` wall | 189s | 188s | neutral |
| `create` | 3059.3 ops/s | 3069.8 ops/s | noise-level gain |
| `open` | 10161.1 ops/s | 10152.0 ops/s | reject: slight regression |
| `stat` | 1023493.1 ops/s | 1027060.5 ops/s | neutral |
| `readdir` | 65443.4 ops/s | 65520.1 ops/s | neutral |
| `rename` | 1944.5 ops/s | 1946.2 ops/s | neutral |
| slice metadata lookups | 98600 misses | 98400 misses | unchanged bottleneck |

Uploaded committed overlay data release check:

```bash
PERF_LOG_TO_CONSOLE=false CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_DEBUG=0 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "metaperf"
```

Artifact:
`docker/compose-xfstests/artifacts/perf-run-1781573541-26081`.

The candidate released resident `recently_committed` page data as soon as a
commit-before-upload slice's remote upload completed. The correctness test
proved that overlay data was still retained while upload was blocked and became
unneeded after object upload, but the focused 4KiB `metaperf` run did not show
a stable performance win. Object shape was unchanged at `36200` one-block
partial-tail uploads, so the bottleneck remained the same tiny explicit-flush
PUT pattern. The code was reverted; a useful next attempt must reduce slice/PUT
count or metadata calls rather than only shortening in-memory residency.

| Workload | Accepted 4KiB focused `perf-run-1781572378-29881` | Candidate `perf-run-1781573541-26081` | Decision |
| --- | ---: | ---: | --- |
| `metaperf` wall | 198s | 196s | neutral/no stable wall gain |
| `create` | 1100.3 ops/s | 1099.6 ops/s | neutral |
| `open` | 10142.3 ops/s | 10052.2 ops/s | reject: open regression |
| `stat` | 1018832.6 ops/s | 1024720.0 ops/s | neutral |
| `readdir` | 64422.5 ops/s | 64685.7 ops/s | neutral |
| `rename` | 1935.6 ops/s | 1943.9 ops/s | neutral |
| S3/writeback shape | 35800 batches / 35801 PUTs | 36200 batches / 36201 PUTs | unchanged tiny-object bottleneck |

Uploaded-slice metadata stage-seal guard check:

```bash
PERF_LOG_TO_CONSOLE=false CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_DEBUG=0 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile
```

Artifact:
`docker/compose-xfstests/artifacts/perf-run-1781567287-24821`.

A follow-up code-review fix tried to extend the local writeback stage-record
readiness guard from the pre-upload commit path to the uploaded-slice metadata
commit path. That was too broad: once remote upload has completed, metadata
commit does not need to wait for the local recovery record. The full run hung
on `fio-bigwrite` for more than five minutes and was stopped manually, leaving
only a partial artifact with no valid summary rows. The writer change and its
tests were rolled back, and the current full BrewFS run
`docker/compose-xfstests/artifacts/perf-run-1781568047-20894` validates that the
rollback restores the complete perf suite.

Multi-block writeback stage/upload overlap check:

```bash
PERF_LOG_TO_CONSOLE=false CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_DEBUG=0 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

Artifact:
`docker/compose-xfstests/artifacts/perf-run-1781562626-29452`.

The candidate tried to follow JuiceFS's per-block staging/upload overlap more
closely by allowing multi-block cached writeback batches to stage locally while
remote upload proceeded, guarded by the new metadata-before-stage-seal check.
The idea reduced some wall time, but it badly regressed random-write tail
latency and mixed active bandwidth, so the stage/upload overlap policy was
reverted.

| Workload | Accepted baseline `perf-run-1781557543-11719` | Candidate `perf-run-1781562626-29452` | Decision |
| --- | ---: | ---: | --- |
| `fio-seqwrite` | 149s, W 73.5 MiB/s, p99 17.7ms | 135s, W 72.6 MiB/s, p99 14.4ms | wall-only gain |
| `fio-randwrite` | 139s, W 140.1 MiB/s, p99 31.9ms | 138s, W 135.0 MiB/s, p99 413.1ms | reject: BW and p99 regression |
| `fio-randrw` | 162s, R 315.5 / W 140.9 MiB/s, R p99 57.4ms | 156s, R 256.6 / W 115.1 MiB/s, R p99 141.6ms | reject: mixed active BW and read tail regression |

Cached append older-unique reuse check:

```bash
PERF_LOG_TO_CONSOLE=false CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_DEBUG=0 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

Artifact:
`docker/compose-xfstests/artifacts/perf-run-1781561001-27750`.

The candidate followed JuiceFS's writable-slice append reuse more closely by
allowing non-overlapping cached appends to reuse an existing writeback slice
even when the FUSE write `unique` was older than the slice's first write. The
TDD guard proved append reuse and preserved the older-unique rejection for
overlapping writes, and all writer tests passed. The code was rejected and
reverted because the focused perf run reduced some slice fragmentation but
shifted cost into worse random-write and mixed workload latency. `fio-seqwrite`
wall time improved, but `fio-randwrite` bandwidth/p99 and `fio-randrw`
wall/active bandwidth/read p99 regressed versus the accepted full baseline.

| Workload | Accepted baseline `perf-run-1781557543-11719` | Candidate `perf-run-1781561001-27750` | Decision |
| --- | ---: | ---: | --- |
| `fio-seqwrite` | 149s, W 73.5 MiB/s | 134s, W 74.4 MiB/s | narrow gain |
| `fio-randwrite` | 139s, W 140.1 MiB/s, p99 31.9ms | 134s, W 135.2 MiB/s, p99 206.6ms | reject: bandwidth and p99 regression |
| `fio-randrw` | 162s, R 315.5 / W 140.9 MiB/s, R p99 57.4ms | 167s, R 258.9 / W 115.7 MiB/s, R p99 240.1ms | reject: wall, bandwidth, and p99 regression |

Redis readdir-plus attr warmup check:

```bash
PERF_LOG_TO_CONSOLE=false CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_DEBUG=0 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "dirperf metaperf"
```

Artifacts:

- Readdir-plus candidate:
  `docker/compose-xfstests/artifacts/perf-run-1781552878-2513`
- Same-window baseline:
  `docker/compose-xfstests/artifacts/perf-run-1781553256-12505`

The candidate implemented Redis `readdir_plus` and made `MetaClient::readdir`
use it to synchronously warm child attr cache, following the JuiceFS pattern of
carrying attrs out of directory scans. Focused Redis tests proved the attrs were
available and a child `stat` hit the MetaClient attr cache after `readdir`, but
the perf run showed this was the wrong attachment point: ordinary `readdir`
paid the plus-path cost while the current `metaperf` workload did not gain
enough from the warmer child attrs. The code was rejected and reverted. A
future retry should attach plus attrs only to a true FUSE `readdirplus` path or
directory-handle mode that can prove it will consume the attrs.

| Workload | Readdir-plus candidate | Baseline | Decision |
| --- | ---: | ---: | --- |
| `dirperf` wall | 8s | 8s | neutral |
| `metaperf` wall | 191s | 190s | reject: no wall gain |
| `create` | 1058.6 ops/s | 1060.9 ops/s | reject: slight regression |
| `open` | 9745.5 ops/s | 9698.1 ops/s | neutral |
| `stat` | 1015091.3 ops/s | 1025026.8 ops/s | reject: slight regression |
| `readdir` | 65027.3 ops/s | 65385.7 ops/s | reject: target regressed |
| `rename` | 1904.7 ops/s | 1918.1 ops/s | reject: slight regression |

Open-file cache bookkeeping cleanup check:

```bash
PERF_LOG_TO_CONSOLE=false CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_DEBUG=0 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "metaperf"
```

Artifact:
`docker/compose-xfstests/artifacts/perf-run-1781570112-3660`.

The candidate removed unused `OpenFileCache` bookkeeping from
`src/meta/client/cache.rs`: the duplicate `entries` map, `refs`, and
`last_check`, and replaced the cached attr lock with `ArcSwap`. The hypothesis
was that repeated open/close on hot files was spending measurable time in async
lock writes that did not affect the current fixed-TTL Moka cache semantics.
`cargo test -p brewfs --lib meta::client` passed, but focused `metaperf` did
not improve: wall time rose to 194s, `open` fell to 9681.3 ops/s versus the
nearby focused baselines at 9698.1 and 9776.3 ops/s, and there was no clear
secondary win. The code and dependency change were reverted.

| Workload | Candidate `perf-run-1781570112-3660` | Reference focused baselines | Decision |
| --- | ---: | ---: | --- |
| `metaperf` wall | 194s | 190s / 192s | reject: wall regression |
| `create` | 1070.6 ops/s | 1060.9 / 1082.8 ops/s | neutral |
| `open` | 9681.3 ops/s | 9698.1 / 9776.3 ops/s | reject: no open gain |
| `stat` | 1025805.6 ops/s | 1025026.8 / 1012718.1 ops/s | neutral |
| `readdir` | 65492.0 ops/s | 65385.7 / 65589.9 ops/s | neutral |
| `rename` | 1921.9 ops/s | 1918.1 / 1915.3 ops/s | neutral |

Open-file cache time-to-idle check:

```bash
PERF_LOG_TO_CONSOLE=false CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_DEBUG=0 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "metaperf"
```

Artifacts:

- Time-to-idle candidate:
  `docker/compose-xfstests/artifacts/perf-run-1781551121-24012`
- Same-window TTL baseline:
  `docker/compose-xfstests/artifacts/perf-run-1781551472-25618`

The candidate changed the Redis open-file attribute cache from fixed
time-to-live to time-to-idle, following the idea that actively reopened files
should stay hot. Focused tests confirmed the cache stayed warm across repeated
opens, and the internal counters improved: open fresh-stat/miss count dropped
from about `41601` to `35401` while open-file cache hits rose from `299000` to
`302600`. It was still rejected and reverted because the same-window metaperf
run did not improve throughput, and TTI would extend the stale-attribute window
under repeated opens unless BrewFS adds a stronger cross-client invalidation or
version check.

| Workload | TTI candidate | TTL baseline | Decision |
| --- | ---: | ---: | --- |
| `metaperf` wall | 191s | 192s | neutral |
| `create` | 1074.7 ops/s | 1082.8 ops/s | reject: slight regression |
| `open` | 9739.7 ops/s | 9776.3 ops/s | reject: no throughput gain |
| `stat` | 1017762.2 ops/s | 1012718.1 ops/s | neutral |
| `readdir` | 65318.7 ops/s | 65589.9 ops/s | reject: slight regression |
| `rename` | 1927.2 ops/s | 1915.3 ops/s | neutral |

Commit wait 20ms polling check:

```bash
PERF_LOG_TO_CONSOLE=false CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_DEBUG=0 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

Artifact: `docker/compose-xfstests/artifacts/perf-run-1781541902-11410`.

The candidate reduced `COMMIT_WAIT_SLICE` from 100ms to 20ms. Targeted
writer/writeback tests passed and the focused run improved wall time for
`fio-seqwrite`/`fio-randwrite`/`fio-randrw` to 136s/133s/154s, but
`commit_wait_upload_ops` grew roughly 5x and `fio-randrw` active bandwidth
fell to R 267.2 / W 119.6 MiB/s. The change was rejected and reverted because
it traded polling pressure and active mixed bandwidth for a modest wall-time
gain.

Registered Notify recheck check:

```bash
PERF_LOG_TO_CONSOLE=false CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_DEBUG=0 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

Artifact: `docker/compose-xfstests/artifacts/perf-run-1781543324-24067`.

The candidate registered the `Notify` waiter before rechecking whether a
writeback slice could commit-before-upload, targeting a possible lost wake-up
between the stage-ready check and the 100ms wait. The targeted
commit-before-upload test and `cargo test -p brewfs --bin brewfs writeback`
passed, but the focused perf counters still averaged about one full
`COMMIT_WAIT_SLICE` per wait: `fio-seqwrite` 20746 waits / 2084.2s,
`fio-randwrite` 39712 waits / 3982.7s, and `fio-randrw` 39924 waits /
3998.5s. Wall time moved to 140s/131s/155s, but `fio-randwrite` p99 regressed
to 206.6ms versus 33.8ms in the accepted full baseline. The change was rejected
and reverted; the remaining bottleneck is not the pre-await lost-wake window.

Range background prefetch disabled check:

```bash
BREWFS_RANGE_BACKGROUND_PREFETCH=false PERF_LOG_TO_CONSOLE=false \
  CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_DEBUG=0 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqread fio-randread fio-randrw"
```

Artifact: `docker/compose-xfstests/artifacts/perf-run-1781544324-26500`.

The focused run passed with `fio-seqread`/`fio-randread`/`fio-randrw` at
60s/60s/158s and active bandwidth of 1.76 GiB/s, 853.3 MiB/s, and
R 323.7 / W 144.7 MiB/s. It is not adopted because the profile's stats showed
`range=0` and `bg_prefetch=0` for all three workloads, so this setting was not
on the hot path under the current `compression=lz4` throughput profile.
`fio-randrw` write p99 also regressed to 137.4ms. The read gap should be
investigated in the block-cache hit path and FUSE/read scheduling, not by
toggling range-triggered full-block prefetch in this profile.

Cached sub-block 4s idle-grace check:

```bash
PERF_FIO_RUNTIME=30 PERF_LOG_TO_CONSOLE=false \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

Artifacts:

- Same-window baseline:
  `docker/compose-xfstests/artifacts/perf-run-1781525876-5023`
- 4s idle-grace candidate:
  `docker/compose-xfstests/artifacts/perf-run-1781526657-15265`

The candidate extended cached-only sub-block idle grace from 3s to 4s. It
improved pure write wall time in the focused buffered run, but mixed `randrw`
lost active read/write bandwidth and shifted many deferred idle tails into
`tooMany` pressure tails. The code was reverted and the direct-IO guard was not
run.

| Workload | Same-window baseline | 4s idle-grace candidate | Decision |
| --- | ---: | ---: | --- |
| `fio-seqwrite` | 137s, W 256.2 MiB/s | 126s, W 293.8 MiB/s | reject: pure-write gain only |
| `fio-randwrite` | 138s, W 120.8 MiB/s, p99 112.7ms | 129s, W 113.0 MiB/s, p99 43.3ms | reject: active BW lower |
| `fio-randrw` | 165s, R 213.3 / W 95.6 MiB/s, R p99 170.9ms | 166s, R 175.1 / W 78.3 MiB/s, R p99 210.8ms | reject: mixed active BW and read tail regression |

The detailed counters explain the mixed-workload regression:
`fio-randrw` idle partial tails dropped from `13433` to `9686`, but `tooMany`
partial tails rose from `2369` to `6835`, total partial tails increased from
`16258` to `17162`, and S3 PUTs rose from `17247` to `18148`.

Cached forward-stream adaptive idle-grace check:

```bash
PERF_FIO_RUNTIME=30 PERF_LOG_TO_CONSOLE=false \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

Artifacts:

- Same-window baseline:
  `docker/compose-xfstests/artifacts/perf-run-1781525876-5023`
- Adaptive forward-stream candidate:
  `docker/compose-xfstests/artifacts/perf-run-1781528039-24164`

The candidate tried to limit the 4s idle grace to cached writes that looked like
a forward stream, leaving other cached/random/direct writes on the existing 3s
grace. It improved wall time in the focused run, but `fio-randrw` active
read/write bandwidth regressed sharply and the final stats showed residual dirty
state. The code was reverted.

| Workload | Same-window baseline | Adaptive candidate | Decision |
| --- | ---: | ---: | --- |
| `fio-seqwrite` | 137s, W 256.2 MiB/s | 128s, W 242.6 MiB/s | reject: wall improved but active BW lower |
| `fio-randwrite` | 138s, W 120.8 MiB/s, p99 112.7ms | 135s, W 292.1 MiB/s, p99 143.7ms | reject: p99 regression and long wall-active tail |
| `fio-randrw` | 165s, R 213.3 / W 95.6 MiB/s, R p99 170.9ms | 161s, R 158.7 / W 71.3 MiB/s, R p99 71.8ms | reject: mixed active BW regression |

The adaptive hint reduced `fio-randrw` idle partial tails from `13433` to
`10749`, but `tooMany` tails rose to `5432`, total partial tails still increased
to `16772`, upload batches increased to `17627`, and the report showed about
`3131 MiB` dirty state after the mixed workload. The next attempt should avoid
time-window extension and instead target real small-write coalescing or upload
scheduling.

Compression-off comparison check:

```bash
BREWFS_COMPRESSION=none PERF_LOG_TO_CONSOLE=false \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

Artifact: `docker/compose-xfstests/artifacts/perf-run-1781513305-12460`.

This was a configuration-only A/B against the then-accepted lz4 snapshot
`docker/compose-xfstests/artifacts/perf-run-1781510059-16265`. It
improved `fio-randwrite` active bandwidth but hurt `fio-seqwrite`, slightly
regressed `fio-randrw`, and worsened random-write p99 latency. It is not a safe
default for the throughput profile.

| Workload | Then-accepted lz4 snapshot | `BREWFS_COMPRESSION=none` | Decision |
| --- | ---: | ---: | --- |
| `fio-seqwrite` | 150s, W 72.8 MiB/s | 139s, W 67.7 MiB/s | reject: lower active BW |
| `fio-randwrite` | 155s, W 78.6 MiB/s, p99 37.0ms | 141s, W 145.3 MiB/s, p99 208.7ms | reject: p99 regression |
| `fio-randrw` | 161s, R 212.6 / W 95.5 MiB/s | 164s, R 203.8 / W 91.3 MiB/s | reject: wall and active BW regression |

Older-unique append reuse check:

```bash
PERF_LOG_TO_CONSOLE=false \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

Artifact: `docker/compose-xfstests/artifacts/perf-run-1781514403-768`.

The candidate allowed non-overlapping cached appends to reuse a slice even when
the incoming FUSE unique id was older than the newest write already in that
slice, while still rejecting older overlapping writes. Focused unit tests passed,
but perf did not reduce the `reject_unique` pressure and mixed `fio-randrw`
regressed materially, so the code was reverted.

| Workload | Accepted snapshot | Older-unique append candidate | Decision |
| --- | ---: | ---: | --- |
| `fio-seqwrite` | 150s, W 72.8 MiB/s | 141s, W 72.6 MiB/s | reject: wall improved but no BW gain |
| `fio-randwrite` | 155s, W 78.6 MiB/s | 138s, W 130.8 MiB/s | reject: narrow gain only |
| `fio-randrw` | 161s, R 212.6 / W 95.5 MiB/s | 162s, R 186.5 / W 83.6 MiB/s | reject: mixed workload regression |

Cached sub-block 10s age-safety check:

```bash
PERF_LOG_TO_CONSOLE=false \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"

PERF_FIO_DIRECT=1 PERF_LOG_TO_CONSOLE=false \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

Artifacts:

- Buffered focused candidate:
  `docker/compose-xfstests/artifacts/perf-run-1781515625-2009`
- Direct-IO guard:
  `docker/compose-xfstests/artifacts/perf-run-1781516260-7539`

The candidate moved cached sub-block auto-freeze from the local 3s/1s idle and
`tooMany` grace to a JuiceFS-like 10s age safety bound. Buffered writes improved
in the focused run, but direct-IO `fio-seqwrite` regressed from 68s to 128s and
write bandwidth slipped from 71.3 to 68.2 MiB/s. The code was reverted because
the direct path is a required guard against page-cache masking.

| Workload | Focused baseline | 10s age-safety candidate | Decision |
| --- | ---: | ---: | --- |
| `fio-seqwrite` direct=0 | 147s, W 71.7 MiB/s | 133s, W 90.0 MiB/s | promising buffered result |
| `fio-randwrite` direct=0 | 138s, W 96.5 MiB/s | 136s, W 110.6 MiB/s | promising buffered result |
| `fio-randrw` direct=0 | 161s, R 221.3 / W 99.4 MiB/s | 155s, R 201.8 / W 89.9 MiB/s | mixed active BW regression |
| `fio-seqwrite` direct=1 | 68s, W 71.3 MiB/s | 128s, W 68.2 MiB/s | reject: direct wall regression |
| `fio-randwrite` direct=1 | 153s, W 55.6 MiB/s | 148s, W 51.0 MiB/s | reject: active BW regression |
| `fio-randrw` direct=1 | 173s, R 118.5 / W 53.3 MiB/s | 154s, R 126.9 / W 56.8 MiB/s | direct mixed improved but not enough to offset seqwrite |

Writeback upload concurrency 8 check:

```bash
BREWFS_WRITEBACK_UPLOAD_CONCURRENCY=8 PERF_LOG_TO_CONSOLE=false \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

Artifact: `docker/compose-xfstests/artifacts/perf-run-1781517044-23657`.

The candidate raised the writeback upload worker count from the then-profile
default of 4 to 8. It reduced sequential-write wall time slightly but increased
S3 PUT latency and hurt mixed read/write tail latency, which points to
RustFS/S3-side queueing rather than an underfilled upload semaphore.

| Workload | Focused baseline | Upload concurrency 8 | Decision |
| --- | ---: | ---: | --- |
| `fio-seqwrite` | 147s, W 71.7 MiB/s, PUT avg 38.6ms | 139s, W 69.9 MiB/s, PUT avg 85.0ms | reject: lower BW and higher PUT latency |
| `fio-randwrite` | 138s, W 96.5 MiB/s, PUT avg 24.0ms | 139s, W 126.8 MiB/s, PUT avg 48.4ms | reject: wall neutral and PUT latency doubled |
| `fio-randrw` | 161s, R 221.3 / W 99.4 MiB/s, R p99 59.0ms | 163s, R 221.4 / W 99.1 MiB/s, R p99 121.1ms | reject: wall and read tail regression |

Unsynced writeback stage flush removal check:

```bash
PERF_LOG_TO_CONSOLE=false \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

Artifact: `docker/compose-xfstests/artifacts/perf-run-1781518481-30002`.

The candidate removed `File::flush()` from unsynced writeback staging while
keeping `writeback_persist_sync=false`. `fio-seqwrite` and `fio-randwrite`
finished in 61s, but `fio-randrw` prefill failed with fsync `EIO`. The code was
reverted: even without durable `sync_all`, Tokio file staging still needs
`flush()` as the async write completion boundary before sealing and committing a
dirty slice.

| Workload | Candidate result | Decision |
| --- | ---: | --- |
| `fio-seqwrite` | 61s | reject: later correctness failure |
| `fio-randwrite` | 61s | reject: later correctness failure |
| `fio-randrw` prefill | fsync `EIO` on `.perf-fio-randrw` files | reject: invalid writeback staging semantics |

Writeback stage vectored IO check:

```bash
PERF_LOG_TO_CONSOLE=false \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"

PERF_FIO_DIRECT=1 PERF_LOG_TO_CONSOLE=false \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

Artifacts:

- Buffered focused candidate:
  `docker/compose-xfstests/artifacts/perf-run-1781519612-13625`
- Direct-IO guard:
  `docker/compose-xfstests/artifacts/perf-run-1781520276-30589`

The candidate kept `File::flush()` in place but changed local writeback staging
from per-`Bytes` `write_all` calls to batched `write_vectored` calls. Unit tests
covered partial vectored writes and the existing writeback/commit-before-upload
tests passed. Buffered pure writes improved substantially, but mixed `randrw`
lost too much active bandwidth, moving BrewFS away from the JuiceFS parity in
the then-accepted snapshot. The direct-IO guard also increased sequential and mixed
wall time versus the current direct baseline. The code change was reverted.

| Workload | Accepted / direct baseline | Vectored stage candidate | Decision |
| --- | ---: | ---: | --- |
| `fio-seqwrite` direct=0 | 150s, W 72.8 MiB/s | 120s, W 107.8 MiB/s | promising pure-write gain |
| `fio-randwrite` direct=0 | 155s, W 78.6 MiB/s | 108s, W 113.0 MiB/s | promising pure-write gain |
| `fio-randrw` direct=0 | 161s, R 212.6 / W 95.5 MiB/s | 164s, R 133.3 / W 59.5 MiB/s | reject: mixed active BW regression |
| `fio-seqwrite` direct=1 | 68s, W 71.3 MiB/s | 79s, W 73.8 MiB/s | reject: direct wall regression |
| `fio-randwrite` direct=1 | 153s, W 55.6 MiB/s | 145s, W 54.8 MiB/s | neutral: wall improved but BW slipped |
| `fio-randrw` direct=1 | 173s, R 118.5 / W 53.3 MiB/s | 183s, R 176.1 / W 78.7 MiB/s | reject: direct wall regression |

Uncompressed aligned vectored PUT fast-path check:

```bash
BREWFS_COMPRESSION=none PERF_FIO_RUNTIME=30 PERF_LOG_TO_CONSOLE=false \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

Artifacts:

- Focused candidate run:
  `docker/compose-xfstests/artifacts/perf-run-1781497160-25385`
- Focused default comparison:
  `docker/compose-xfstests/artifacts/perf-run-1781484237-25679`

The candidate tested a narrow object-store write optimization for the
`compression=none` comparison profile: aligned `write_fresh_vectored` calls
kept caller `Bytes` chunks for `put_object_vectored` instead of coalescing them
before upload. Targeted tests passed, but perf showed the close/flush tail is
still dominated by dirty-slice staging and metadata commit volume rather than
this upload-side copy. The code change was reverted.

| Workload | Default focused baseline | `compression=none` vectored PUT fast path | Decision |
| --- | ---: | ---: | --- |
| `fio-seqwrite` | 142s, W 336.2 MiB/s | 142s, W 294.8 MiB/s | reject: no wall gain and lower active BW |
| `fio-randwrite` | 129s, W 125.5 MiB/s | 135s, W 93.8 MiB/s | reject: wall and active BW regression |
| `fio-randrw` | 158s, R 192.6 / W 86.4 MiB/s | 180s, R 155.8 / W 69.8 MiB/s | reject: wall and active BW regression |

Cached writable stream-upload deferral check:

```bash
PERF_FIO_RUNTIME=10 PERF_LOG_TO_CONSOLE=false PERF_FUSE_OPS_LOG=1 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite"

PERF_FIO_RUNTIME=30 PERF_LOG_TO_CONSOLE=false \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

Artifacts:

- FUSE write diagnostic sample:
  `docker/compose-xfstests/artifacts/perf-run-1781494986-3663`
- Focused candidate run:
  `docker/compose-xfstests/artifacts/perf-run-1781495701-28214`
- Focused default comparison:
  `docker/compose-xfstests/artifacts/perf-run-1781484237-25679`

The diagnostic sample confirmed that fio `bs=4m` does not arrive at BrewFS as
large aligned FUSE writes under kernel writeback-cache. The 10s sequential-write
sample produced 58,448 FUSE `write` requests averaging 169.7 KiB, with all
sampled writes carrying `write_flags=1`. The size histogram was dominated by
4 KiB, 8 KiB, and 1 MiB requests, and the writeback path emitted 13,191
partial-tail uploads. The rejected candidate delayed streaming upload from
cached-only writable slices until the dirty slice reached the configured
writeback target. It was not adopted: the isolated sequential wall time
improved, but random-write and mixed read/write wall times regressed, and the
random-write S3 PUT count increased.

| Workload | Default focused baseline | Cached writable stream deferral | Decision |
| --- | ---: | ---: | --- |
| `fio-seqwrite` | 142s, W 336.2 MiB/s | 130s, W 234.3 MiB/s | reject: wall improved, but active BW was much lower |
| `fio-randwrite` | 129s, W 125.5 MiB/s | 137s, W 110.8 MiB/s | reject: wall and active BW regression |
| `fio-randrw` | 158s, R 192.6 / W 86.4 MiB/s | 163s, R 361.8 / W 162.4 MiB/s | reject: wall regression despite active BW gain |

Writeback upload concurrency 6 check:

```bash
BREWFS_WRITEBACK_UPLOAD_CONCURRENCY=6 \
PERF_FIO_RUNTIME=30 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

Artifacts:

- Focused candidate run:
  `docker/compose-xfstests/artifacts/perf-run-1781493948-4895`
- Focused default comparison:
  `docker/compose-xfstests/artifacts/perf-run-1781484237-25679`

The candidate raised the global commit-before-upload writeback upload pool from
the then-throughput profile default of 4 to 6. It was not adopted: sequential write
wall time improved, but active write bandwidth fell and random write regressed.
The S3 PUT average latency also rose on the candidate run, indicating more
object-store queueing rather than less writeback amplification.

| Workload | Default focused baseline | Writeback upload concurrency 6 | Decision |
| --- | ---: | ---: | --- |
| `fio-seqwrite` | 142s, W 336.2 MiB/s | 136s, W 257.7 MiB/s | reject: wall gain came with much lower active BW and higher PUT latency |
| `fio-randwrite` | 129s, W 125.5 MiB/s | 136s, W 108.7 MiB/s | reject: wall and active BW regression |
| `fio-randrw` | 158s, R 192.6 / W 86.4 MiB/s | 157s, R 194.9 / W 87.3 MiB/s | neutral |

Older-unique append slice-reuse check:

```bash
PERF_FIO_RUNTIME=30 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

Artifacts:

- Focused candidate run:
  `docker/compose-xfstests/artifacts/perf-run-1781493022-1268`
- Focused default comparison:
  `docker/compose-xfstests/artifacts/perf-run-1781484237-25679`

The candidate allowed an older FUSE `unique` request to reuse an existing dirty
slice for pure append while preserving the older-unique rejection for overlapping
writes. Targeted tests confirmed the intended behavior, but the focused perf run
did not validate a stable gain: sequential write improved slightly, while random
write and mixed read/write regressed versus the focused default comparison. The
code change and tests were reverted.

| Workload | Default focused baseline | Older-unique append reuse | Decision |
| --- | ---: | ---: | --- |
| `fio-seqwrite` | 142s, W 336.2 MiB/s | 138s, W 326.2 MiB/s | reject: wall improved but active BW and slice stats did not validate the hypothesis |
| `fio-randwrite` | 129s, W 125.5 MiB/s | 135s, W 116.4 MiB/s | reject: wall and active BW regression |
| `fio-randrw` | 158s, R 192.6 / W 86.4 MiB/s | 163s, R 409.2 / W 183.5 MiB/s | reject: wall regression despite active BW gain |

Cached sub-block 5s coalescing-window check:

```bash
PERF_FIO_RUNTIME=30 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"

bash docker/compose-xfstests/run_redis_perf.sh --s3 --writeback-throughput-profile
```

Artifacts:

- Focused candidate run:
  `docker/compose-xfstests/artifacts/perf-run-1781490684-1968`
- Full candidate run:
  `docker/compose-xfstests/artifacts/perf-run-1781491348-13630`

The candidate aligned cached sub-block auto-freeze timing with the 5s background
flush duration so FUSE writeback-cache fragments would be coalesced longer. It
was not adopted: the focused write run improved, but the full default run
regressed the pure write wall-time checks. The code change was reverted.

| Workload | Accepted BrewFS | 5s cached-sub-block full run | Decision |
| --- | ---: | ---: | --- |
| `fio-seqwrite` | 140s, W 73.2 MiB/s | 142s, W 82.0 MiB/s | reject: wall regression despite active BW gain |
| `fio-randwrite` | 153s, W 79.6 MiB/s | 159s, W 81.5 MiB/s | reject: wall regression |
| `fio-randrw` | 173s, R 170.2 / W 76.4 MiB/s | 161s, R 236.4 / W 105.9 MiB/s | reject: mixed gain does not offset pure-write regression |
| `metaperf` | 353s | 326s | metadata gain only |

Dirty slice file-handle cache check:

```bash
PERF_FIO_RUNTIME=30 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"

bash docker/compose-xfstests/run_redis_perf.sh --s3 --writeback-throughput-profile
```

Artifacts:

- Focused candidate run:
  `docker/compose-xfstests/artifacts/perf-run-1781488136-5319`
- Full candidate run:
  `docker/compose-xfstests/artifacts/perf-run-1781488824-31222`

The candidate reused an open local `.slice` file handle while a dirty slice was
being staged, then closed it when the recoverable record was sealed. It was not
adopted: the 30s focused run looked promising, but the full default run did not
confirm the gain and regressed two primary wall-time checks. The code change was
reverted.

| Workload | Accepted BrewFS | File-handle cache full run | Decision |
| --- | ---: | ---: | --- |
| `fio-seqwrite` | 140s, W 73.2 MiB/s | 151s, W 71.6 MiB/s | reject: wall and active BW regression |
| `fio-randwrite` | 153s, W 79.6 MiB/s | 152s, W 98.5 MiB/s | reject: active BW improved, wall gain not stable |
| `fio-randrw` | 173s, R 170.2 / W 76.4 MiB/s | 176s, R 262.8 / W 117.8 MiB/s | reject: wall regression despite active BW gain |
| `metaperf` | 353s | 357s | reject: metadata wall regression |

Focused writeback slice-threshold check:

```bash
PERF_FIO_RUNTIME=30 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"

PERF_FIO_RUNTIME=30 BREWFS_WRITEBACK_MAX_SLICES_THRESHOLD=2000 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw"
```

Artifacts:

- Default focused baseline:
  `docker/compose-xfstests/artifacts/perf-run-1781484237-25679`
- Threshold override:
  `docker/compose-xfstests/artifacts/perf-run-1781483558-2840`

The threshold override is not adopted. It did not improve the close/flush tail,
and the detailed stats still showed mostly cached partial-tail uploads.

| Workload | Default focused baseline | Threshold 2000 | Decision |
| --- | ---: | ---: | --- |
| `fio-seqwrite` | 142s, W 336.2 MiB/s | 141s, W 307.0 MiB/s | reject: no wall gain, lower active BW |
| `fio-randwrite` | 129s, W 125.5 MiB/s | 131s, W 163.3 MiB/s | reject: wall regression |
| `fio-randrw` | 158s, R 192.6 / W 86.4 MiB/s | 162s, R 228.1 / W 102.4 MiB/s | reject: wall regression |

Cached too-many grace check:

```bash
PERF_LOG_TO_CONSOLE=false \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqwrite fio-randwrite fio-randrw metaperf"
```

Artifact: `docker/compose-xfstests/artifacts/perf-run-1781502023-376`.

The candidate extended cached-only sub-block `too_many` grace from `1s` to the
existing `3s` cached idle grace. It was not adopted: sequential write improved,
but random write and mixed read/write shifted `too_many` tails into idle tails,
increased object/slice amplification, and metaperf regressed sharply. The code
change was reverted.

| Workload | Accepted BrewFS | Cached too-many grace focused run | Decision |
| --- | ---: | ---: | --- |
| `fio-seqwrite` | 150s, W 71.9 MiB/s | 145s, W 77.9 MiB/s | reject: seqwrite gain only |
| `fio-randwrite` | 142s, W 103.3 MiB/s, write p99 41.7ms | 137s, W 107.0 MiB/s, write p99 208.7ms | reject: write tail and PUT/slice amplification regression |
| `fio-randrw` | 166s, R 164.8 / W 73.9 MiB/s | 166s, R 204.3 / W 91.5 MiB/s | reject: no wall gain; PUT and partial-tail counts rose |
| `metaperf` | 371s | 450s | reject: metadata wall regression |

The detailed counters show why the timing-only approach is unsafe: `fio-randrw`
reduced `too_many` partial tails from `5572` to `526`, but idle tails rose from
`10530` to `16059`; `fio-randwrite` partial tails rose from `11451` to `14313`;
and `metaperf` explicit partial tails rose from `27000` to `28600`.

Compression check:

```bash
BREWFS_COMPRESSION=none \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 --writeback-throughput-profile
```

Artifact: `docker/compose-xfstests/artifacts/perf-run-1781481687-25288`.
The full toolset passed, but this profile is not adopted because it regressed
write wall time in the default comparison set.

| Workload | Accepted BrewFS | `compression=none` | Decision |
| --- | ---: | ---: | --- |
| `fio-seqwrite` | 153s, W 72.7 MiB/s | 152s, W 70.4 MiB/s | neutral |
| `fio-randwrite` | 150s, W 95.1 MiB/s | 159s, W 105.4 MiB/s | reject: wall regression |
| `fio-randrw` | 177s, R 171.5 / W 76.9 MiB/s | 179s, R 184.5 / W 82.8 MiB/s | reject: wall regression |
| `metaperf` | 351s | 346s | small metadata gain only |

Interpretation:

- This run used fio buffered IO (`direct=0`), so read bandwidth is a
  cache-aware regression signal rather than a pure cold-object-store maximum.
- BrewFS mixed `randrw` active IO bandwidth remains close to JuiceFS
  (`0.97x` read, `0.95x` write), but BrewFS still has a much longer wall-time
  tail (`166s` versus `61s`).
- The main remaining data-path gap is active write throughput: BrewFS is about
  `0.27x` of JuiceFS on sequential write and `0.30x` on random write in this
  snapshot, even though the JuiceFS random-write wall time was hurt by local
  cache timeouts.
- BrewFS internal stats still point at small-object writeback amplification:
  `fio-seqwrite` emitted 16.5k upload batches averaging about 0.6 MiB with
  97% partial tails, `fio-randwrite` emitted 11.1k batches averaging about
  0.8 MiB with 91% partial tails, and `metaperf` emitted 27.6k tiny
  staged/uploaded slices for 109.4 MiB of FUSE writes.
- The JuiceFS run emitted slow PUT, disk-cache timeout, and `readSlice context
  canceled` warnings on the console, but completed all default scenarios as
  pass. Treat the JuiceFS write numbers as the current local-run result rather
  than an ideal upper bound for JuiceFS.

## Feature Flags

```bash
cargo build -p brewfs --release --features jemalloc
cargo build -p brewfs --release --features profiling
cargo build -p brewfs --release --features rkyv-serialization
```

Available features:

- `jemalloc`: use jemalloc as the global allocator on Linux.
- `jemalloc-profiling`: enable jemalloc heap profiling support.
- `profiling`: enable tracing flamegraph, Chrome trace, and tokio-console integrations.
- `rkyv-serialization`: enable rkyv-based metadata serialization support.

## Documentation

Start with the [documentation index](doc/README.md).

Common entry points:

- [Configuration](doc/operations/configuration.md)
- [Architecture](doc/architecture/arch.md)
- [VFS internals](doc/vfs/README.md)
- [Testing and CI guides](doc/README.md#testing-and-ci)
- [Performance and JuiceFS comparison](doc/README.md#performance-and-juicefs-comparison)
- [JuiceFS internals notes](doc/juicefs/README.md)
- [BrewFS/JuiceFS gap analysis](doc/gap/README.md)
- [Control plane](doc/operations/control-plane.md)

## Repository Map

- `src/`: core filesystem, metadata, chunk, object backend, FUSE, and CLI code.
- `examples/`: SDK, S3, persistence, and local mount examples.
- `doc/`: canonical design notes, operations guides, performance plans, tests, and debugging notes.
- `docker/`: compose stacks, xfstests/LTP/perf runners, and runtime image tooling.
- `tests/`: integration and native stress tests.
- `operator/`: Kubernetes operator prototype and CRD documentation.
- `tools/`: performance and stats helpers.

## Contributing

Issues and PRs are welcome. For larger changes, prefer keeping implementation, tests, and documentation updates together so backend capabilities and operational guidance stay in sync.
