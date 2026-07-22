# Meet BrewFS: High-Performance Distributed Storage, Built in Rust

Modern workloads want the convenience of a filesystem without giving up the
scale and flexibility of object storage. Build farms need fast metadata. AI
pipelines need to stream large datasets while creating thousands of small
artifacts. Containers expect familiar POSIX operations even when their data
lives far beyond a local disk.

[BrewFS](https://github.com/brewfs/brewfs) is an open-source distributed
filesystem built in Rust for exactly this space. It presents a FUSE filesystem
to applications, keeps namespace and inode state in a transactional metadata
backend, and stores file data in S3-compatible object storage or on local
storage. The result is a practical bridge between ordinary filesystem tools
and modern storage infrastructure.

## BrewFS compared with JuiceFS

Before looking at the architecture, here is the current performance snapshot.
The July 22, 2026 comparison covered both Redis and TiKV metadata with RustFS
S3-compatible storage on the same host. Every run used buffered `io_uring`, a
512 MiB fio working set, a 20-second runtime, 4 MiB requests, and disabled
compression. All eleven tools passed in all four runs.

Using complete tool-wall throughput rather than foreground-only bandwidth,
BrewFS recorded a 2.24x data-plane geometric mean with Redis and 2.39x with
TiKV. Across the seven data workloads and five metadata operations, the
geometric means were 1.72x and 1.58x respectively. Mixed random I/O is counted
once using total bytes in these geometric means.

### Application-visible throughput

This first table reports fio bandwidth while each workload is actively issuing
I/O. Mixed I/O reports its read and write components separately.

| Workload | BrewFS Redis | JuiceFS Redis | Ratio | BrewFS TiKV | JuiceFS TiKV | Ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Large read | **2,509.8 MiB/s** | 221.7 MiB/s | **11.32x** | **2,179.9 MiB/s** | 186.8 MiB/s | **11.67x** |
| Large write | **188.4 MiB/s** | 120.0 MiB/s | **1.57x** | **184.6 MiB/s** | 126.0 MiB/s | **1.47x** |
| Sequential read | **1,074.6 MiB/s** | 1,039.7 MiB/s | **1.03x** | **1,124.6 MiB/s** | 1,079.8 MiB/s | **1.04x** |
| Sequential write | 213.9 MiB/s | **482.3 MiB/s** | 0.44x | 208.2 MiB/s | **474.4 MiB/s** | 0.44x |
| Random read | **1,467.3 MiB/s** | 1,136.5 MiB/s | **1.29x** | **1,434.9 MiB/s** | 703.1 MiB/s | **2.04x** |
| Random write | 215.5 MiB/s | **588.3 MiB/s** | 0.37x | 207.3 MiB/s | **572.7 MiB/s** | 0.36x |
| Mixed random read | **441.4 MiB/s** | 29.9 MiB/s | **14.76x** | **392.1 MiB/s** | 28.4 MiB/s | **13.81x** |
| Mixed random write | **203.1 MiB/s** | 14.6 MiB/s | **13.91x** | **180.6 MiB/s** | 13.0 MiB/s | **13.89x** |

### Complete end-to-end throughput

Foreground bandwidth can reward a filesystem for moving work into close,
flush, or a writeback queue. These results divide actual bytes by complete tool
wall time, including fio teardown, close, flush, and fsync. Mixed I/O reports
both components here but is counted once, using total bytes, in the geometric
mean. Each BrewFS run ended with zero dirty, live-dirty, pending-upload, and
remote-inflight bytes; JuiceFS used synchronous upload (`writeback=false`).

| Workload | BrewFS Redis | JuiceFS Redis | Ratio | BrewFS TiKV | JuiceFS TiKV | Ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Large read | **2,048.0 MiB/s** | 215.6 MiB/s | **9.50x** | **2,048.0 MiB/s** | 186.2 MiB/s | **11.00x** |
| Large write | **186.2 MiB/s** | 117.0 MiB/s | **1.59x** | **178.1 MiB/s** | 124.1 MiB/s | **1.43x** |
| Sequential read | **1,023.6 MiB/s** | 990.3 MiB/s | **1.03x** | **1,071.2 MiB/s** | 1,028.6 MiB/s | **1.04x** |
| Sequential write | **186.1 MiB/s** | 178.7 MiB/s | **1.04x** | **181.2 MiB/s** | 179.1 MiB/s | **1.01x** |
| Random read | **1,467.8 MiB/s** | 1,086.5 MiB/s | **1.35x** | **1,368.4 MiB/s** | 703.4 MiB/s | **1.95x** |
| Random write | **179.8 MiB/s** | 176.0 MiB/s | **1.02x** | **181.7 MiB/s** | 171.2 MiB/s | **1.06x** |
| Mixed random read | **368.3 MiB/s** | 28.7 MiB/s | **12.83x** | **357.3 MiB/s** | 27.3 MiB/s | **13.09x** |
| Mixed random write | **169.5 MiB/s** | 14.0 MiB/s | **12.11x** | **164.5 MiB/s** | 12.5 MiB/s | **13.12x** |

### Metadata throughput

| Operation | BrewFS Redis | JuiceFS Redis | Ratio | BrewFS TiKV | JuiceFS TiKV | Ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Create | **1,006.8 ops/s** | 236.7 ops/s | **4.25x** | 84.6 ops/s | **110.9 ops/s** | 0.76x |
| Open | 5,293.7 ops/s | **11,967.3 ops/s** | 0.44x | 5,380.2 ops/s | **10,198.1 ops/s** | 0.53x |
| Stat | **682,622.9 ops/s** | 676,511.9 ops/s | **1.01x** | **680,816.7 ops/s** | 604,482.6 ops/s | **1.13x** |
| Readdir | **31,404.2 ops/s** | 18,201.8 ops/s | **1.73x** | **18,450.3 ops/s** | 7,878.6 ops/s | **2.34x** |
| Rename | 943.9 ops/s | **1,313.5 ops/s** | 0.72x | 136.1 ops/s | **266.6 ops/s** | 0.51x |

The tables deliberately retain every measured operation, including the cases
where JuiceFS is faster. BrewFS used upload-before-commit, 16 FUSE workers, S3
concurrency 16, upload concurrency 10, and a one-second metadata open cache.
JuiceFS used `writeback=false`, an 8 GiB buffer, a 4 GiB cache, four upload
workers, and the same open-cache duration and capacity.

The accepted artifacts are:

- BrewFS Redis: `perf-run-1784699784-1494`
- JuiceFS Redis: `juicefs-perf-run-1784700215-27086`
- BrewFS TiKV: `perf-run-1784698922-7734`
- JuiceFS TiKV: `juicefs-perf-run-1784700736-11129`

The TiKV optimization reduced BrewFS's complete metaperf wall time from 593 to
445 seconds. It uses 1,024-inode ID leases instead of locking a global counter
for every create, batches pessimistic namespace reads for create and rename,
returns create attributes from the committing transaction, and avoids repeated
atime write transactions under relatime semantics. Full-run rename throughput
rose from 115.7 to 136.1 ops/s while create rose from 82.1 to 84.6 ops/s.

JuiceFS+TiKV emitted one directory-stat inconsistency warning during dirstress
and immediately scheduled its built-in resynchronization; the tool passed and
the mount flushed and closed its session normally. No run reported a timeout.
This remains a reproducible local engineering snapshot rather than a claim
about every machine or deployment.

BrewFS is independent rather than a fork of an existing filesystem. Its stack,
from the FUSE request path and VFS to metadata transactions, caching, and object
adapters, is implemented in Rust.

## One filesystem, interchangeable storage components

BrewFS separates the filesystem interface, metadata, and data paths:

- **FUSE and VFS** translate Linux filesystem requests into inode-based
  operations with support for files, directories, hard links, symbolic links,
  sparse files, truncation, renames, and byte-range locks.
- **Transactional metadata** stores namespaces, attributes, slices, sessions,
  and locks in Redis, TiKV, etcd, PostgreSQL, or SQLite.
- **Chunk and cache layers** organize files into 64 MiB chunks and 4 MiB blocks,
  backed by memory and SSD caches, read-ahead, writeback, compaction, and garbage
  collection.
- **Object adapters** persist blocks to RustFS, MinIO, AWS S3, Ceph RGW, other
  S3-compatible services, or a local filesystem.

This separation makes BrewFS useful in more than one deployment shape. A
developer can start locally with SQLite metadata and a local data directory,
then move to TiKV and S3-compatible storage without changing how applications
access their files.

## Why Rust for a distributed filesystem?

A userspace filesystem sits on a demanding boundary. It must process concurrent
kernel requests, maintain coherent inode and cache state, coordinate background
uploads, and return errors without leaving partially updated metadata behind.
Those requirements make Rust a natural foundation for BrewFS.

Rust's ownership model helps make buffer and cache lifetimes explicit across
asynchronous tasks. Shared immutable data can move through the read path using
`Bytes` and `Arc`, while `Send` and `Sync` constraints expose unsafe concurrency
assumptions during development instead of under production load. The absence of
a tracing garbage collector also gives BrewFS direct control over when large
pages, blocks, and request buffers are released.

Rust does not make storage fast automatically. Most performance gains still
come from reducing metadata round trips, avoiding duplicate object requests,
coalescing writes, and applying backpressure at the right boundaries. What Rust
provides is a strong base for implementing those optimizations without giving
up memory safety.

## A data path designed for real I/O

BrewFS combines several techniques to keep remote storage away from the
application's critical path whenever semantics allow:

- Layered memory and SSD caches serve hot pages and blocks locally.
- Single-flight object reads allow concurrent cache misses to share one remote
  request.
- Read-ahead turns sequential access into parallel object fetches.
- Dirty overlays let readers observe locally written data while upload work is
  still being coordinated.
- Large-write coalescing reduces object and metadata amplification.
- Bounded upload workers and memory budgets keep asynchronous writeback from
  growing without limit.

The same attention extends to metadata. Backend-specific transactional paths
combine operations where possible, while short-lived inode, directory, and open
caches reduce repeated network round trips. BrewFS keeps these policies
configurable because the best freshness and throughput trade-off for a
single-client build workspace is not necessarily correct for a multi-client
shared filesystem.

## Performance measured through the finish line

Filesystem benchmarks can be misleading when they stop timing as soon as an
application finishes issuing writes. A fast foreground result may simply mean
that more work was left in memory or on local SSD for `close`, `flush`, or a
background uploader to complete later.

BrewFS therefore ships reproducible Docker Compose performance runners that
record both fio's active bandwidth and effective throughput after writeback has
drained. Artifacts include fio JSON, tool logs, profile settings, cache and
writeback metrics, warnings, and derived reports.

In the latest matched Redis, TiKV, and RustFS snapshot, BrewFS achieved a 2.24x
data-plane geometric mean with Redis and 2.39x with TiKV. Across all twelve
reported data and metadata operations, the corresponding geometric means were
1.72x and 1.58x. The tables above retain the cases where JuiceFS was faster,
including open and rename on both backends, TiKV create, and foreground
sequential and random writes.

The headline is not that one local benchmark predicts every deployment. It is
that the benchmark is inspectable: both filesystems use explicit cache and
concurrency profiles, write tests include post-write drain, and the commands
needed to reproduce the comparison live beside the implementation.

## Correctness is part of performance

A distributed filesystem is not useful if a faster data path weakens filesystem
behavior. BrewFS treats correctness tests as release gates rather than optional
compatibility checks.

The repository contains runners for xfstests, pjdfstest, the Linux Test Project,
stress-ng, fio, metadata stress tools, and fuzzing. The current validation
baseline completes all 708 configured xfstests cases on each supported metadata
backend. Redis and TiKV also pass the complete pjdfstest corpus: 246 test files
and 9,134 assertions.

Known Linux FUSE limitations are documented with narrow exclusions instead of
being hidden behind a broad skip list. Performance changes are rejected when
they regress mixed read/write behavior, leave dirty data after a run, move work
into teardown, or fail the workspace correctness gate.

This discipline matters because filesystem optimization is full of attractive
shortcuts. A larger write buffer can improve a chart while increasing recovery
debt. A longer metadata cache can accelerate `open` while weakening
cross-client freshness. BrewFS makes these trade-offs visible and keeps
throughput-oriented behavior in explicit profiles.

## Try BrewFS

A local build requires Rust 1.85 or newer and FUSE 3. The smallest setup uses
SQLite metadata and local data storage:

```bash
cargo build -p brewfs --release

mkdir -p /tmp/brewfs-mnt /tmp/brewfs-data
target/release/brewfs mount /tmp/brewfs-mnt \
  --data-backend local-fs \
  --data-dir /tmp/brewfs-data \
  --meta-backend sqlx \
  --meta-url sqlite:///tmp/brewfs-meta.db
```

For a complete single-node stack with Redis, RustFS, systemd, and a mounted
BrewFS filesystem, the project also provides an installation script:

```bash
curl -fsSL https://raw.githubusercontent.com/brewfs/brewfs/main/scripts/install_brewfs_single_node.sh \
  | sudo bash -s -- install
```

## Help build the next layer

BrewFS is still evolving. Current work includes reducing metadata round trips,
improving multi-client cache invalidation, lowering write amplification, and
expanding operational tooling around compaction, recovery, and observability.

Contributions do not need to start in the deepest part of the write path. The
project benefits from new backend adapters, workload reports, documentation,
portability fixes, test coverage, and production feedback just as much as from
core filesystem changes.

If building a distributed filesystem in Rust sounds interesting, explore the
[BrewFS repository](https://github.com/brewfs/brewfs), read the
[architecture guide](https://github.com/brewfs/brewfs/blob/main/doc/architecture/arch.md),
or reproduce the
[performance comparison](https://github.com/brewfs/brewfs#performance-vs-juicefs).

The filesystem interface may be decades old. The storage stack behind it does
not have to be.

---

Editorial disclosure: this article was drafted with assistance from OpenAI
Codex.
