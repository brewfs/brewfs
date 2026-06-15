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

The following snapshot was collected on 2026-06-14 with the full Docker perf
runners under `docker/compose-xfstests/`:

```bash
bash docker/compose-xfstests/run_redis_perf.sh --s3 --writeback-throughput-profile
bash docker/compose-xfstests/run_juicefs_perf.sh --writeback-throughput-profile
```

Artifacts:

- BrewFS: `docker/compose-xfstests/artifacts/perf-run-1781485889-24840`
- JuiceFS: `docker/compose-xfstests/artifacts/juicefs-perf-run-1781467237-32007`

All default perf tools passed on both filesystems.

Wall seconds include close/flush tail. Fio bandwidth reports the active IO
window, so both columns are useful when diagnosing writeback behavior. This
snapshot used fio `direct=0`, as recorded in the generated fio JSON.

| Workload | BrewFS wall | JuiceFS wall | BrewFS fio BW | JuiceFS fio BW | BrewFS/JuiceFS BW |
| --- | ---: | ---: | ---: | ---: | ---: |
| `fio-bigwrite` | 1s | 0s | W 1.03 GiB/s | W 3.36 GiB/s | W 0.31x |
| `fio-bigread` | 3s | 1s | R 542.4 MiB/s | R 2.36 GiB/s | R 0.22x |
| `fio-seqread` | 61s | 61s | R 1.69 GiB/s | R 2.46 GiB/s | R 0.69x |
| `fio-seqwrite` | 140s | 126s | W 73.2 MiB/s | W 281.2 MiB/s | W 0.26x |
| `fio-randread` | 61s | 61s | R 755.5 MiB/s | R 3.21 GiB/s | R 0.23x |
| `fio-randwrite` | 153s | 143s | W 79.6 MiB/s | W 283.4 MiB/s | W 0.28x |
| `fio-randrw` | 173s | 61s | R 170.2 / W 76.4 MiB/s | R 179.6 / W 81.8 MiB/s | R 0.95x / W 0.94x |

Metadata comparison from `metaperf`:

| Operation | BrewFS | JuiceFS | BrewFS/JuiceFS |
| --- | ---: | ---: | ---: |
| `create` | 812 ops/s | 1320 ops/s | 0.62x |
| `open` | 9406 ops/s | 23506 ops/s | 0.40x |
| `stat` | 1020877 ops/s | 1024797 ops/s | 1.00x |
| `readdir` | 63299 ops/s | 91787 ops/s | 0.69x |
| `rename` | 1901 ops/s | 2676 ops/s | 0.71x |

Latest accepted BrewFS tuning:

- `src/vfs/cache/write_back.rs` caches already-created dirty directories in
  `FsWriteBackCache`, avoiding repeated `create_dir_all` checks for every
  staged writeback batch under the same inode/chunk directory.
- Full BrewFS perf artifact:
  `docker/compose-xfstests/artifacts/perf-run-1781485889-24840`.
- Compared with the previous accepted BrewFS run
  `docker/compose-xfstests/artifacts/perf-run-1781466015-27228`,
  `fio-seqwrite` improved from 153s to 140s and `fio-randrw` improved from
  177s to 173s. `fio-randwrite` moved from 150s to 153s, so the remaining
  random-write path still needs targeted work.

Latest rejected tuning checks:

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
the throughput profile default of 4 to 6. It was not adopted: sequential write
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
- BrewFS mixed `randrw` active IO bandwidth is close to JuiceFS, but BrewFS
  still has a much longer wall-time tail in that scenario.
- The main remaining data-path gap is writeback throughput: BrewFS is about
  0.26x of JuiceFS on sequential write and 0.28x on random write in this run.
- BrewFS internal stats point at small-object writeback amplification: this run
  uploaded mostly partial tails (`fio-seqwrite` averaged about 0.6 MiB per
  batch with 97% partial tails; `metaperf` uploaded 28.8k tiny batches for
  112.5 MiB of user writes).
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
