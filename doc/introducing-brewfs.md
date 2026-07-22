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
The July 22, 2026 comparison used TiKV 8.5.0 metadata, RustFS S3-compatible
storage, buffered `io_uring`, a 512 MiB fio working set, a 20-second runtime,
disabled compression, and explicit close and drain accounting on the same
host. Using complete rather than foreground-only throughput, BrewFS recorded a
2.19x data-plane geometric mean and a 1.48x geometric mean across all twelve
reported data and metadata operations compared with JuiceFS.

### Application-visible throughput

This first table reports fio bandwidth while each workload is actively issuing
I/O. Mixed I/O reports its read and write components separately.

| Workload | BrewFS | JuiceFS | BrewFS / JuiceFS |
| --- | ---: | ---: | ---: |
| Large read | **2,233.4 MiB/s** | 190.0 MiB/s | **11.75x** |
| Large write | **187.2 MiB/s** | 123.8 MiB/s | **1.51x** |
| Sequential read | **1,123.7 MiB/s** | 1,063.6 MiB/s | **1.06x** |
| Sequential write | 213.3 MiB/s | **473.4 MiB/s** | 0.45x |
| Random read | **1,455.3 MiB/s** | 907.7 MiB/s | **1.60x** |
| Random write | 211.8 MiB/s | **574.3 MiB/s** | 0.37x |
| Mixed random read | **389.5 MiB/s** | 28.4 MiB/s | **13.71x** |
| Mixed random write | **180.4 MiB/s** | 14.2 MiB/s | **12.70x** |

### Complete end-to-end throughput

Foreground bandwidth can reward a filesystem for moving work into close,
flush, or a writeback queue. These results divide actual bytes by complete tool
wall time plus any post-write drain. Mixed I/O reports both components here but
is counted once, using total bytes, in the geometric mean.

| Workload | BrewFS | JuiceFS | BrewFS / JuiceFS |
| --- | ---: | ---: | ---: |
| Large read | **1,365.3 MiB/s** | 186.2 MiB/s | **7.33x** |
| Large write | **186.2 MiB/s** | 120.5 MiB/s | **1.55x** |
| Sequential read | **1,126.6 MiB/s** | 1,063.8 MiB/s | **1.06x** |
| Sequential write | 177.8 MiB/s | **178.7 MiB/s** | 0.99x |
| Random read | **1,386.7 MiB/s** | 865.0 MiB/s | **1.60x** |
| Random write | **176.7 MiB/s** | 171.7 MiB/s | **1.03x** |
| Mixed random read | **354.4 MiB/s** | 28.0 MiB/s | **12.66x** |
| Mixed random write | **164.2 MiB/s** | 14.0 MiB/s | **11.73x** |

### Metadata throughput

| Operation | BrewFS | JuiceFS | BrewFS / JuiceFS |
| --- | ---: | ---: | ---: |
| Create | 82.1 ops/s | **106.1 ops/s** | 0.77x |
| Open | 5,389.2 ops/s | **10,447.5 ops/s** | 0.52x |
| Stat | **688,342.8 ops/s** | 606,376.7 ops/s | **1.14x** |
| Readdir | **16,520.8 ops/s** | 7,609.2 ops/s | **2.17x** |
| Rename | 115.7 ops/s | **257.0 ops/s** | 0.45x |

The tables deliberately retain every measured operation, including the cases
where JuiceFS is faster. BrewFS used upload-before-commit, 16 FUSE workers, S3
concurrency 16, upload concurrency 10, and a one-second metadata open cache.
JuiceFS used `writeback=false`, an 8 GiB buffer, a 4 GiB cache, four upload
workers, and the same open-cache duration and capacity. The accepted artifacts
are `perf-run-1784683943-7077` and
`juicefs-perf-run-1784686113-23030`. All eleven tools passed; BrewFS ended with
zero dirty, live-dirty, pending-upload, and remote-inflight bytes, while the
JuiceFS run reported no timeout warnings. This remains a reproducible local
engineering snapshot rather than a claim about every machine or deployment.

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

In the latest matched TiKV and RustFS snapshot, BrewFS achieved a 2.19x
geometric mean across the seven measured data-plane operations and a 1.48x
geometric mean across all twelve reported data and metadata operations compared
with JuiceFS. The tables above retain the cases where JuiceFS was faster,
including create, open, rename, and foreground sequential and random writes.

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
