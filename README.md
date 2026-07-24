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

RustFS is one of the S3-compatible object storage backends supported by BrewFS and is used in the repository's reproducible benchmark and filesystem test profiles.

<p align="center">
  <a href="https://github.com/rustfs/rustfs">
    <img src="doc/assets/rustfs.png" alt="RustFS, a supported S3-compatible backend" width="220" height="60" />
  </a>
  <a href="https://github.com/rustfs/rustfs">
    <img src="https://img.shields.io/github/stars/rustfs/rustfs?style=flat-square" alt="RustFS GitHub stars" />
  </a>
</p>

The July 24, 2026 Redis and TiKV comparison uses matched buffered fio profiles,
preserved local read caches, and strict post-write drain. `PERF_FIO_SIZE=512m`
is the per-job size: time-based workloads run for 20 seconds, while Large read
is a one-pass 8-job, 4 GiB aggregate workload. Write results are reported as
complete end-to-end throughput because foreground fio bandwidth alone omits
queued writeback work.

## Why BrewFS

- **Fast data path:** chunked I/O, memory and SSD caches, read-ahead, writeback, and large-write coalescing.
- **Rust throughout:** one modern, memory-safe implementation from FUSE and VFS to metadata and object storage.
- **Storage freedom:** Redis, TiKV, etcd, PostgreSQL, or SQLite metadata with S3-compatible or local object data.
- **Operationally testable:** xfstests, pjdfstest, LTP, stress-ng, fio, metadata benchmarks, fuzzing, and Docker Compose runners live in the repository.

## Performance vs JuiceFS

This July 24, 2026 snapshot compares BrewFS with JuiceFS 1.3.1 using Redis and
TiKV metadata plus RustFS S3-compatible object storage. The four complete runs
used buffered `io_uring`, 4 MiB I/O, a 512 MiB per-job fio size, disabled
compression, durable read prefill, cache-preserving remounts, and explicit
post-write drain. Time-based fio workloads ran for 20 seconds; Large read and
Large write transferred 4 GiB with eight jobs. All ten tools passed in every
run and every write drain ended with zero queued bytes.

No single number describes writeback filesystems correctly. The first table is
application-visible throughput, based on bytes divided by complete tool wall
time. The second extends the finish line through background writeback drain.
Across 14 application-visible data rows and 10 metadata rows, BrewFS leads 15
of 24: 10 of 14 data rows and 5 of 10 metadata rows. Stable persistent-cache
Large read is effectively tied; JuiceFS still leads sequential read, strictly
drained pure writes, and TiKV create/open/rename.

### Effective profile

| System | Effective settings |
| --- | --- |
| BrewFS Redis | `commit_before_upload`; 2 GiB read memory; 2 GiB write memory; 8 GiB read SSD cache; 4 GiB write SSD cache; 6 GiB memory budget; 6 writeback upload workers; S3 concurrency 8; upload concurrency 16 |
| BrewFS TiKV | Same cache layout; resource-safe 6 GiB total memory budget; 4 writeback upload workers; 8 FUSE workers; `max_background=256` so TiKV, PD, and RustFS retain host headroom |
| JuiceFS 1.3.1 | `writeback=true`; 4 GiB buffer; 8 GiB persistent cache; `cache-large-write`; 4 upload connections; 4 stage-write threads; 1-second, 65,536-entry open cache; metadata backup and usage reporting disabled. This build does not support `--max-downloads`. |

Eight BrewFS Redis upload workers were rejected because mixed random I/O fell
about 23% during fio and about 12% after drain. An aggressive 4 GiB read plus
4 GiB write TiKV profile was rejected after it caused multi-minute close tails.
The published profiles are the fastest accepted settings that preserved the
mixed-I/O gate and host stability.

### Application-visible data throughput

Actual bytes divided by complete fio process wall time. Mixed random I/O is
read plus write throughput.

| Workload | BrewFS Redis | JuiceFS Redis | BrewFS TiKV | JuiceFS TiKV |
| --- | ---: | ---: | ---: | ---: |
| Large read | **194.34 MiB/s** | 194.17 MiB/s | **194.35 MiB/s** | 194.09 MiB/s |
| Sequential read | 740.60 MiB/s | **1,000.76 MiB/s** | 892.00 MiB/s | **1,018.67 MiB/s** |
| Random read | **1,726.10 MiB/s** | 1,256.80 MiB/s | 1,253.33 MiB/s | **1,264.20 MiB/s** |
| Large write | **195.05 MiB/s** | 102.40 MiB/s | 95.26 MiB/s | **97.52 MiB/s** |
| Sequential write | **189.33 MiB/s** | 117.94 MiB/s | **128.28 MiB/s** | 117.12 MiB/s |
| Random write | **178.17 MiB/s** | 117.43 MiB/s | **138.17 MiB/s** | 113.92 MiB/s |
| Mixed random I/O | **495.54 MiB/s** | 328.76 MiB/s | **315.31 MiB/s** | 183.60 MiB/s |

Read rows use durable prefill followed by an unmount/remount that preserves the
local SSD cache. They are persistent-cache results, not cold object-store
results. Large read uses three remount/read rounds, targeted
`POSIX_FADV_DONTNEED` eviction of each filesystem's local cache files, and the
median bandwidth. This prevents an asymmetric host page-cache hit from being
reported as a persistent-cache advantage. All four stable Large read artifacts
recorded zero object GET bytes during the measured phase.

### Hot local-cache Large read (Redis)

This separate diagnostic measures the fully hot local page-cache path, not
durable SSD-cache throughput and not cold object-store download. After the
same durable prefill and cache-preserving remount, each filesystem performs
five unreported 4 GiB warmup passes and then three measured 4 GiB passes in
the same mount. No local-cache eviction or inter-pass remount occurs. Both
systems issued zero object GET bytes during the measured phase.

| System | Median | Three-run range | Spread | Warmup |
| --- | ---: | ---: | ---: | ---: |
| BrewFS Redis | **2,395.32 MiB/s** | 2,354.02-2,436.64 MiB/s | 3.45% | 5 passes |
| JuiceFS Redis | 2,356.73 MiB/s | 2,312.82-2,399.53 MiB/s | 3.68% | 5 passes |

The small spread makes this a repeatable host-memory-cache result. It is
published separately so it cannot be mistaken for the remount-stable
persistent-cache comparison above. BrewFS's promoted-slice cache is accounted
for by the zero S3 GET evidence; its generic block-cache hit counter does not
include that path.

### Fully drained write throughput

Actual bytes divided by active I/O time plus post-write drain. This exposes
work left on local SSD after the application finishes. Mixed random I/O reports
total read plus write bytes.

| Workload | BrewFS Redis | JuiceFS Redis | BrewFS TiKV | JuiceFS TiKV |
| --- | ---: | ---: | ---: | ---: |
| Large write | 66.47 MiB/s | **79.13 MiB/s** | 61.34 MiB/s | **77.36 MiB/s** |
| Sequential write | 88.50 MiB/s | **144.44 MiB/s** | 85.63 MiB/s | **144.13 MiB/s** |
| Random write | 89.31 MiB/s | **138.89 MiB/s** | 126.65 MiB/s | **146.37 MiB/s** |
| Mixed random I/O | **218.60 MiB/s** | 113.86 MiB/s | **298.37 MiB/s** | 117.99 MiB/s |

BrewFS's SSD writeback cache is working: it shortens application-visible pure
write time and strongly improves mixed I/O. The remaining pure-write deficit
appears after the foreground phase, where object upload and metadata commit
costs dominate. Large-write P99 close/fsync tails remain an optimization target
rather than being hidden behind foreground bandwidth.

### Write completion accounting

Each cell is `tool wall / post-drain`. Tool wall includes startup, active I/O,
close, and fsync; post-drain ends only when the queue is empty.

| Workload | BrewFS Redis | JuiceFS Redis | BrewFS TiKV | JuiceFS TiKV |
| --- | ---: | ---: | ---: | ---: |
| Large write | 21 s / 41 s | 40 s / 12 s | 43 s / 27 s | 42 s / 11 s |
| Sequential write | 21 s / 24 s | 70 s / 37 s | 29 s / 15 s | 68 s / 35 s |
| Random write | 24 s / 24 s | 70 s / 39 s | 35 s / 18 s | 71 s / 35 s |
| Mixed random I/O | 26 s / 33 s | 21 s / 40 s | 46 s / 28 s | 30 s / 18 s |

### Metadata throughput

Operations per second from the same complete `metaperf` runs.

| Operation | BrewFS Redis | JuiceFS Redis | BrewFS TiKV | JuiceFS TiKV |
| --- | ---: | ---: | ---: | ---: |
| Create | **1,004.61 ops/s** | 549.01 ops/s | 84.53 ops/s | **151.85 ops/s** |
| Open | 4,827.50 ops/s | **11,883.85 ops/s** | 1,137.44 ops/s | **10,534.28 ops/s** |
| Stat | **726,234.51 ops/s** | 724,328.73 ops/s | **724,066.83 ops/s** | 637,033.56 ops/s |
| Readdir | **28,634.94 ops/s** | 16,020.25 ops/s | **15,983.11 ops/s** | 8,675.80 ops/s |
| Rename | 894.64 ops/s | **1,313.40 ops/s** | 125.31 ops/s | **256.68 ops/s** |

### Complete tool wall time

These are all ten tools from each accepted run, not selected microbenchmarks.

| Tool | BrewFS Redis | JuiceFS Redis | BrewFS TiKV | JuiceFS TiKV |
| --- | ---: | ---: | ---: | ---: |
| `fio-bigread` | 31 s | 5 s | 42 s | 20 s |
| `fio-bigwrite` | 21 s | 40 s | 43 s | 42 s |
| `fio-seqread` | 20 s | 21 s | 20 s | 21 s |
| `fio-seqwrite` | 21 s | 70 s | 29 s | 68 s |
| `fio-randread` | 21 s | 20 s | 21 s | 20 s |
| `fio-randwrite` | 24 s | 70 s | 35 s | 71 s |
| `fio-randrw` | 26 s | 21 s | 46 s | 30 s |
| `metaperf` | 293 s | 194 s | 457 s | 359 s |
| `dirstress` | 1 s | 3 s | 5 s | 3 s |
| `dirperf` | 17 s | 14 s | 163 s | 85 s |

### Artifacts and reproduction

Complete-matrix artifacts:

- BrewFS Redis: `perf-run-1784876227-9934`
- JuiceFS Redis: `juicefs-perf-run-1784879992-17249`
- BrewFS TiKV: `perf-run-1784878850-2470`
- JuiceFS TiKV: `juicefs-perf-run-1784880709-16460`

Stable Large read artifacts:

- BrewFS Redis: `perf-run-1784889951-27900`
- JuiceFS Redis: `juicefs-perf-run-1784890100-1138`

Hot local-cache Large read artifacts (Redis, five warmups and three measured
passes):

- BrewFS Redis: `perf-run-1784894177-23661`
- JuiceFS Redis: `juicefs-perf-run-1784894284-19194`
- BrewFS TiKV: `perf-run-1784890275-8226`
- JuiceFS TiKV: `juicefs-perf-run-1784890444-29768`

Each artifact contains effective settings, raw fio JSON, tool logs, cache/object
counters, drain samples, warnings, and a generated report under
`docker/compose-xfstests/artifacts/<run>/`. These are single-host engineering
results, not universal deployment claims.

```bash
PERF_LOG_TO_CONSOLE=false PERF_FIO_SIZE=512m PERF_FIO_RUNTIME=20 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile \
  --tools "fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw fio-bigread fio-bigwrite metaperf dirstress dirperf"

JUICEFS_META_BACKEND=redis PERF_LOG_TO_CONSOLE=false \
PERF_FIO_SIZE=512m PERF_FIO_RUNTIME=20 \
  bash docker/compose-xfstests/run_juicefs_perf.sh \
  --writeback-throughput-profile \
  --tools "fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw fio-bigread fio-bigwrite metaperf dirstress dirperf"
```

Use `run_tikv_perf.sh` for BrewFS TiKV and set `JUICEFS_META_BACKEND=tikv` for
the JuiceFS TiKV run.

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
