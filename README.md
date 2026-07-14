<div align="center">
  <img src="doc/assets/brewfs.png" alt="BrewFS" width="366" height="167" />
  <p><strong>BrewFS: High-performance distributed storage, built in Rust.</strong></p>

  <p>
    <a href="https://github.com/brewfs/brewfs/actions/workflows/ci.yml"><img src="https://github.com/brewfs/brewfs/actions/workflows/ci.yml/badge.svg" alt="CI" /></a>
    <a href="https://github.com/brewfs/brewfs/releases"><img src="https://img.shields.io/github/v/release/brewfs/brewfs" alt="Release" /></a>
    <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/language-Rust-orange.svg" alt="Rust" /></a>
    <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT license" /></a>
  </p>
  <p>
    <a href="#quick-start">Install</a> ·
    <a href="#performance-vs-juicefs">Benchmarks</a> ·
    <a href="doc/architecture/arch.md">Architecture</a> ·
    <a href="doc/README.md">Documentation</a> ·
    <a href="README_CN.md">中文</a>
  </p>
</div>

BrewFS is an independent distributed filesystem for container, AI, and object-storage-heavy workloads. It combines a POSIX-like FUSE interface with pluggable transactional metadata and S3-compatible data storage.

Against [JuiceFS](https://juicefs.com/) ([GitHub](https://github.com/juicedata/juicefs)) in the current Redis + RustFS benchmark, BrewFS delivers **2.45x random-read throughput**, **2.62x file-create throughput**, and about **12x mixed random-I/O throughput**. The full results, including workloads where the two systems are at parity or JuiceFS leads, are below.

| **2.45x** | **2.62x** | **12.09x** | **1.17x** |
| :---: | :---: | :---: | :---: |
| random read | file create | mixed random read | tuned large write |

![Selected BrewFS benchmark wins relative to JuiceFS](doc/assets/performance-vs-juicefs.svg)

## Why BrewFS

- **Fast data path:** chunked I/O, memory and SSD caches, read-ahead, writeback, and large-write coalescing.
- **Rust throughout:** one modern, memory-safe implementation from FUSE and VFS to metadata and object storage.
- **Storage freedom:** Redis, TiKV, etcd, PostgreSQL, or SQLite metadata with S3-compatible or local object data.
- **Operationally testable:** xfstests, pjdfstest, LTP, stress-ng, fio, metadata benchmarks, fuzzing, and Docker Compose runners live in the repository.

## Performance vs JuiceFS

This local snapshot was collected on the same host with Redis metadata, RustFS S3-compatible storage, compression disabled, and matching fio workload shapes. fio used buffered I/O (`direct=0`); BrewFS used direct-I/O FUSE handles for read-heavy profiles.

### Benchmark Environment (local machine)

- **CPU:** Intel Xeon Platinum (x86_64, 1 socket / 8 vCPU, 2 threads per core)
- **Memory:** 16 GiB total
- **Kernel:** Linux 6.8.0-117-generic
- **OS:** Ubuntu-based kernel image (GNU/Linux)
- **Storage:** 150GB aliyun ESSD AutoPL (8300 IOPS)

Environment file and full logs for each run are stored in `docker/compose-xfstests/artifacts/<run>/` (contains raw profile env and generated reports).

### Data throughput

| Workload | BrewFS | JuiceFS | BrewFS / JuiceFS |
| --- | ---: | ---: | ---: |
| Large write | **820.5 MiB/s** | 702.8 MiB/s | **1.17x** |
| Large read | **2.36 GiB/s** | 1.03 GiB/s | **2.29x** |
| Sequential read | **1.77 GiB/s** | 1.12 GiB/s | **1.57x** |
| Sequential write | 198.5 MiB/s | **204.7 MiB/s** | 0.97x |
| Random read | **2.81 GiB/s** | 1.15 GiB/s | **2.45x** |
| Random write | 200.9 MiB/s | **205.2 MiB/s** | 0.98x |
| Mixed random read | **392.4 MiB/s** | 32.5 MiB/s | **12.09x** |
| Mixed random write | **174.5 MiB/s** | 14.7 MiB/s | **11.86x** |

### Metadata throughput

| Operation | BrewFS | JuiceFS | BrewFS / JuiceFS |
| --- | ---: | ---: | ---: |
| Create | **992.7 ops/s** | 378.8 ops/s | **2.62x** |
| Open | 5,185.2 ops/s | **6,025.0 ops/s** | 0.86x |
| Stat | **684,626.9 ops/s** | 683,779.5 ops/s | 1.00x |
| Readdir | **34,624.8 ops/s** | 21,420.4 ops/s | **1.62x** |
| Rename | 1,014.9 ops/s | **1,341.6 ops/s** | 0.76x |

<details>
<summary><strong>Latency, runtime, and benchmark details</strong></summary>

| Workload | BrewFS wall / active | JuiceFS wall / active | BrewFS p99 | JuiceFS p99 |
| --- | ---: | ---: | ---: | ---: |
| Large write | 2s / 1.248s | 3s / 1.457s | W 67.6ms | W 65.3ms |
| Large read | 1s / 0.424s | 2s / 0.973s | R 0.0ms | R 97.0ms |
| Sequential read | 60s / 60.001s | 61s / 60.001s | R 0.0ms | R 4.4ms |
| Sequential write | 63s / 60.002s | 62s / 60.051s | W 42.2ms | W 120.1ms |
| Random read | 60s / 60.004s | 60s / 60.008s | R 0.0ms | R 51.6ms |
| Random write | 65s / 60.016s | 62s / 60.091s | W 254.8ms | W 215.0ms |
| Mixed random I/O | 65s / 60.034s | 64s / 61.734s | R 107.5ms / W 219.2ms | R 3170.9ms / W 346.0ms |

| Metadata operation | BrewFS latency | JuiceFS latency |
| --- | ---: | ---: |
| Create | **1,007.4 us/op** | 2,639.9 us/op |
| Open | 192.9 us/op | **166.0 us/op** |
| Stat | **1.5 us/op** | **1.5 us/op** |
| Readdir | **28.9 us/op** | 46.7 us/op |
| Rename | 985.3 us/op | **745.4 us/op** |

| Tool | BrewFS wall | JuiceFS wall | Result |
| --- | ---: | ---: | --- |
| `dirstress` | **1s** | 3s | pass / pass |
| `dirperf` | 16s | **14s** | pass / pass |
| `metaperf` | 205s | **193s** | pass / pass |
| `looptest` | 2s | **1s** | pass / pass |

The large-write BrewFS result uses the opt-in `--bigwrite-throughput-profile`, which enables `commit_before_upload`; the normal safer default is `upload_before_commit`. JuiceFS used `JFS_WRITEBACK=false`. The JuiceFS mixed random-I/O result was rerun twice and remained within the same range.

This is a local engineering snapshot from July 9-10, 2026, not a claim about every deployment. Each runner writes its fio JSON, profile environment, logs, and generated report to the gitignored `docker/compose-xfstests/artifacts/` directory for local audit.

</details>

Reproduce the comparison with the Docker runners:

```bash
BREWFS_COMPRESSION=none \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 --read-throughput-profile
bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --bigwrite-throughput-profile --tools fio-bigwrite
JFS_COMPRESS=none JFS_WRITEBACK=false \
  bash docker/compose-xfstests/run_juicefs_perf.sh
```

## Quick Start

Install a complete single-node Linux stack with Redis, RustFS, systemd, and a BrewFS FUSE mount:

```bash
curl -fsSL https://raw.githubusercontent.com/brewfs/brewfs/main/scripts/install_brewfs_single_node.sh \
  | sudo bash -s -- install
```

Or build from source with Rust 1.85+ and `fuse3`:

```bash
cargo build -p brewfs --release

mkdir -p /tmp/brewfs-mnt /tmp/brewfs-data
target/release/brewfs mount /tmp/brewfs-mnt \
  --data-backend local-fs \
  --data-dir /tmp/brewfs-data \
  --meta-backend sqlx \
  --meta-url sqlite:///tmp/brewfs-meta.db
```

See the [binary deployment guide](doc/operations/binary-deployment.md) and [configuration reference](doc/operations/configuration.md) for production backends, tuning profiles, upgrades, and uninstall steps.

## Architecture

BrewFS separates the filesystem interface, metadata, and object data paths:

- **FUSE + VFS** provide inode-based POSIX operations.
- **Metadata** stores namespaces, attributes, slices, sessions, and transactions in Redis, TiKV, etcd, PostgreSQL, or SQLite.
- **Chunk + cache** map files into 64 MiB chunks and 4 MiB blocks, with memory/SSD caching, compaction, and garbage collection.
- **Object adapters** persist blocks to S3-compatible systems such as RustFS, MinIO, AWS S3, and Ceph RGW, or to local storage.

Core operations include create, read, write, truncate, sparse files, rename, hardlinks, symlinks, byte-range locks, compaction, delayed deletion, and runtime `info`/`gc` control commands.

## Test It

```bash
cargo test -p brewfs

cd docker
bash compose-xfstests/run_redis_xfstests.sh --cases "generic/001"
bash compose-xfstests/run_redis_pjdfstest.sh
```

The [testing guide](doc/testing/docker-compose-test-guide.md) covers Redis, TiKV, RustFS, xfstests, pjdfstest, LTP, stress-ng, and performance runners.

## Documentation

- [Documentation index](doc/README.md)
- [Architecture](doc/architecture/arch.md)
- [Configuration](doc/operations/configuration.md)
- [Binary deployment](doc/operations/binary-deployment.md)
- [Benchmark guide](doc/testing/bench.md)
- [BrewFS/JuiceFS gap analysis](doc/gap/README.md)

## Contributing

Issues and pull requests are welcome. Please keep behavior changes, tests, and documentation together whenever possible.

## POSIX Correctness

BrewFS treats filesystem correctness as a release requirement, not a best-effort compatibility claim. Its FUSE and metadata paths are continuously exercised with xfstests, pjdfstest, the Linux Test Project (LTP), and stress-ng across SQLite, Redis, etcd, and TiKV metadata backends.

The current validation baseline completes all 708 configured xfstests cases on every supported metadata backend. Redis and TiKV also pass the complete pjdfstest corpus: 246 test files and 9,134 assertions with no default exclusions. The configured LTP filesystem profiles pass on all four backends. Kernel and FUSE limitations that cannot yet be implemented reliably are narrowly excluded, documented with reproducible evidence, and kept visible for future revalidation.

This breadth of testing gives BrewFS a strong POSIX correctness foundation for builds, containers, AI pipelines, and other workloads that depend on predictable filesystem behavior. See the [filesystem test suite matrix](doc/testing/fs-test-suite-matrix.md) for current artifacts, coverage, and known limitations.

## Contact

Questions, deployment discussions, and collaboration inquiries are welcome at [genedna@gmail.com](mailto:genedna@gmail.com).
