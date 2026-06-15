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

- BrewFS: `docker/compose-xfstests/artifacts/perf-run-1781538386-3498`
- JuiceFS: `docker/compose-xfstests/artifacts/juicefs-perf-run-1781539517-14275`

All default perf tools passed on both filesystems.

Wall seconds include close/flush tail. Fio bandwidth reports the active IO
window, so both columns are useful when diagnosing writeback behavior. This
snapshot used fio `direct=0`, as recorded in the generated fio JSON. The
JuiceFS run again emitted many slow S3 PUT, disk-cache write timeout,
direct-upload fallback, read context-cancel, and slow-flush warnings during
write-heavy and mixed phases, so the JuiceFS numbers below are the local
same-cycle comparison rather than an ideal upper bound. In this run, JuiceFS
spent 26s draining `randread` prefill writeback and 49s draining `randrw`
prefill writeback before the cold read/mixed phases, and reported about 93s/90s
slow flushes in `seqwrite`/`randwrite`.

| Workload | BrewFS wall | JuiceFS wall | BrewFS fio BW | JuiceFS fio BW | BrewFS/JuiceFS BW | BrewFS p99 | JuiceFS p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `fio-bigwrite` | 1s | 1s | W 1.04 GiB/s | W 2.98 GiB/s | W 0.35x | W 41.2ms | W 18.5ms |
| `fio-bigread` | 2s | 1s | R 530.0 MiB/s | R 2.34 GiB/s | R 0.22x | R 114.8ms | R 43.8ms |
| `fio-seqread` | 61s | 60s | R 1.71 GiB/s | R 2.44 GiB/s | R 0.70x | R 3.5ms | R 1.5ms |
| `fio-seqwrite` | 148s | 156s | W 75.5 MiB/s | W 259.5 MiB/s | W 0.29x | W 17.7ms | W 329.3ms |
| `fio-randread` | 61s | 61s | R 784.1 MiB/s | R 3.21 GiB/s | R 0.24x | R 44.8ms | R 7.0ms |
| `fio-randwrite` | 143s | 151s | W 114.0 MiB/s | W 286.5 MiB/s | W 0.40x | W 33.8ms | W 333.4ms |
| `fio-randrw` | 161s | 61s | R 296.4 / W 132.9 MiB/s | R 196.9 / W 88.2 MiB/s | R 1.51x / W 1.51x | R 137.4ms / W 24.8ms | R 826.3ms / W 13.8ms |

Metadata comparison from `metaperf`:

| Operation | BrewFS | JuiceFS | BrewFS/JuiceFS |
| --- | ---: | ---: | ---: |
| `create` | 952.2 ops/s | 1343.8 ops/s | 0.71x |
| `open` | 9712.8 ops/s | 23477.4 ops/s | 0.41x |
| `stat` | 1021875.5 ops/s | 1018006.8 ops/s | 1.00x |
| `readdir` | 63290.4 ops/s | 91255.7 ops/s | 0.69x |
| `rename` | 1880.7 ops/s | 2676.2 ops/s | 0.70x |

The full `metaperf` tool wall time was `392s` on BrewFS and `304s` on JuiceFS.
BrewFS total metadata wall improved from the previous `449s` snapshot, while
`create` and `readdir` per-operation rates moved lower and `rename` improved
slightly. Small-file metadata/writeback cleanup remains a follow-up target.

Latest accepted BrewFS tuning:

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
- Full BrewFS perf artifact:
  `docker/compose-xfstests/artifacts/perf-run-1781538386-3498`.
- Compared with the previous accepted BrewFS run
  `docker/compose-xfstests/artifacts/perf-run-1781532331-14304`,
  `fio-seqwrite` moved from 146s / 74.0 MiB/s to 148s / 75.5 MiB/s with p99
  improving from 29.0ms to 17.7ms, `fio-randwrite` moved from 144s /
  111.2 MiB/s to 143s / 114.0 MiB/s, and `fio-randrw` moved from 166s /
  R 325.0 / W 145.5 MiB/s to 161s / R 296.4 / W 132.9 MiB/s. `fio-randrw`
  read p99 improved from 164.6ms to 137.4ms, while write p99 regressed from
  18.7ms to 24.8ms. The change is accepted as a small writeback profile step,
  with a follow-up to reduce mixed-workload wall time and restore active
  `randrw` bandwidth.
- Focused validation artifacts:
  `docker/compose-xfstests/artifacts/perf-run-1781537092-760` for the
  `BREWFS_WRITEBACK_UPLOAD_CONCURRENCY=6` direct=0 writeback check,
  `docker/compose-xfstests/artifacts/perf-run-1781537714-9726` for the rejected
  concurrency=4 comparison, and
  `docker/compose-xfstests/artifacts/perf-run-1781540944-16637` as the direct=1
  `fio-seqwrite fio-randwrite fio-randrw` guard.
- The next bottlenecks are pure write active bandwidth, cold/random read
  bandwidth, and mixed-workload tail: sequential write is still about 0.29x of
  the same-cycle JuiceFS write bandwidth, random write is about 0.40x, and
  `fio-randrw` active bandwidth still exceeds this noisy local JuiceFS run but
  takes 161s wall time versus JuiceFS at 61s.

Latest rejected tuning checks:

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
