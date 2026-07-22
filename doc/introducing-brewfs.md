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
In a matched local comparison using Redis metadata, RustFS S3-compatible
storage, disabled compression, and explicit writeback drain accounting, BrewFS
recorded a 1.74x data-plane geometric mean and a 1.38x geometric mean across all
twelve reported data and metadata operations compared with JuiceFS.

![BrewFS performance relative to JuiceFS](assets/performance-vs-juicefs.svg)

### Application-visible throughput

This first table reports fio bandwidth while each workload is actively issuing
I/O. Mixed I/O reports its read and write components separately.

| Workload | BrewFS | JuiceFS | BrewFS / JuiceFS |
| --- | ---: | ---: | ---: |
| Large write | 912.3 MiB/s | **1.07 GiB/s** | 0.83x |
| Large read | 743.7 MiB/s | **936.9 MiB/s** | 0.79x |
| Sequential read | **1.57 GiB/s** | 1.04 GiB/s | **1.52x** |
| Sequential write | 146.5 MiB/s | **280.6 MiB/s** | 0.52x |
| Random read | **3.75 GiB/s** | 1.21 GiB/s | **3.10x** |
| Random write | 127.9 MiB/s | **312.9 MiB/s** | 0.41x |
| Mixed random read | **237.7 MiB/s** | 119.3 MiB/s | **1.99x** |
| Mixed random write | **108.1 MiB/s** | 55.7 MiB/s | **1.94x** |

### Fully drained write throughput

Foreground bandwidth alone can reward a filesystem for leaving more work in
its writeback queue. These results divide the actual bytes written by active
runtime plus post-write drain time. Mixed I/O is counted once using total read
and write bytes.

| Workload | BrewFS | JuiceFS | BrewFS / JuiceFS |
| --- | ---: | ---: | ---: |
| Large write | **327.9 MiB/s** | 103.1 MiB/s | **3.18x** |
| Sequential write | **103.4 MiB/s** | 99.7 MiB/s | **1.04x** |
| Random write | **104.4 MiB/s** | 97.8 MiB/s | **1.07x** |
| Mixed random I/O total | **278.0 MiB/s** | 74.2 MiB/s | **3.75x** |

### Metadata throughput

| Operation | BrewFS | JuiceFS | BrewFS / JuiceFS |
| --- | ---: | ---: | ---: |
| Create | **1,054.5 ops/s** | 651.5 ops/s | **1.62x** |
| Open | 5,018.6 ops/s | **12,027.2 ops/s** | 0.42x |
| Stat | **686,751.8 ops/s** | 683,792.0 ops/s | 1.00x |
| Readdir | **34,480.7 ops/s** | 17,580.2 ops/s | **1.96x** |
| Rename | 965.7 ops/s | **1,306.8 ops/s** | 0.74x |

The tables deliberately retain every measured operation, including the cases
where JuiceFS is faster. This is a reproducible local engineering snapshot
rather than a claim about every machine or deployment; the complete
environment, latency data, artifacts, and commands are documented in the
[BrewFS performance report](https://github.com/brewfs/brewfs#performance-vs-juicefs).

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

In the matched Redis and RustFS snapshot documented in the project README,
BrewFS achieved a 1.74x geometric mean across the measured data-plane
operations and a 1.38x geometric mean across all twelve reported data and
metadata operations compared with JuiceFS. The same report retains the cases
where JuiceFS was faster, including open and rename throughput.

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
