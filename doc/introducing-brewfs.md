# BrewFS: POSIX Files, Transactional Metadata, Object-Storage Scale

> **In a matched 24-row comparison with JuiceFS, BrewFS leads 15 rows: 10 of
> 14 data-path results and 5 of 10 metadata results. It reaches 2,395.32 MiB/s
> on a repeatable hot-cache Large read, while separate cold-page tests prove
> that its local SSD cache survives a remount.**

Applications want files, directories, atomic renames, links, and byte-range
I/O. Infrastructure teams want transactional metadata and inexpensive,
S3-compatible capacity. [BrewFS](https://github.com/brewfs/brewfs) connects
those worlds through an open-source distributed filesystem written in Rust,
without forcing applications to adopt an object-storage API.

Mount BrewFS through Linux FUSE, choose Redis, TiKV, etcd, PostgreSQL, or
SQLite for metadata, and place file data in S3-compatible or local storage.
Memory and SSD caches keep remote storage away from the critical path, while
bounded writeback and explicit drain accounting make deferred work visible.

## The headline results

The strongest Redis results are not small benchmark noise. Under the matched
July 24, 2026 profile, BrewFS delivers **37% more random-read throughput**,
**51% more foreground mixed-I/O throughput**, and **90% more foreground Large
write throughput** than JuiceFS. When background writeback is included,
BrewFS still delivers **92% more mixed-I/O throughput**.

| Representative Redis result | BrewFS | JuiceFS | BrewFS advantage |
| --- | ---: | ---: | ---: |
| Random read | **1,726.10 MiB/s** | 1,256.80 MiB/s | **37% faster** |
| Foreground mixed random I/O | **495.54 MiB/s** | 328.76 MiB/s | **51% faster** |
| Foreground Large write | **195.05 MiB/s** | 102.40 MiB/s | **90% faster** |
| Fully drained mixed random I/O | **218.60 MiB/s** | 113.86 MiB/s | **92% faster** |
| Hot local-cache Large read | **2,395.32 MiB/s** | 2,356.73 MiB/s | **2% faster** |
| Metadata create | **1,004.61 ops/s** | 549.01 ops/s | **83% faster** |

Performance is only useful with filesystem correctness. The same codebase
passes **708 configured xfstests cases per supported metadata backend**. Redis
and TiKV each pass the full pjdfstest corpus: **246 test files and 9,134
assertions**. The workspace gate adds **1,166 passing Rust tests**.

The honest boundary matters: BrewFS does not lead every workload. JuiceFS is
still faster in sequential read, strictly drained pure writes, and TiKV
create/open/rename. The complete tables below include those losses alongside
the wins, with cache state, writeback drain, units, and run artifacts exposed.

## Where BrewFS fits

BrewFS is aimed at workloads that need filesystem compatibility while their
capacity tier lives outside the local machine:

- Build and CI workspaces that create many files but also stream large
  artifacts.
- AI and data pipelines that want local-cache speed with object-storage
  capacity.
- Container platforms that need a mountable shared namespace instead of an
  application-specific storage SDK.
- Storage engineering teams that want Redis, TiKV, etcd, PostgreSQL, or SQLite
  metadata behind one filesystem interface.

It is less compelling for workloads that only need raw object access, require
kernel-filesystem latency, or cannot tolerate the operational trade-offs of
FUSE and distributed metadata.

## A fair comparison with JuiceFS

The July 24, 2026 snapshot compares BrewFS with JuiceFS 1.3.1 on the same host,
using Redis and TiKV metadata and RustFS object storage. All four
complete runs used buffered `io_uring`, 4 MiB I/O, a 512 MiB per-job fio size,
disabled compression, durable read prefill, cache-preserving remounts, and
strict post-write drain. Every one of the ten tools passed, and every write
queue drained to zero.

The outcome is strong but not universal. Across 14 application-visible data
rows and 10 metadata rows, BrewFS leads 15 of 24. It leads 10 of 14 data rows,
including random read, foreground pure writes, and mixed random I/O, and 5 of
10 metadata rows. Stable persistent-cache Large read is effectively tied.
JuiceFS remains ahead on sequential read, strictly drained pure writes, and
TiKV create/open/rename.

### Tuned for throughput without destabilizing the host

BrewFS uses `commit_before_upload`, 2 GiB read and write memory caches, an
8 GiB persistent read cache, a 4 GiB SSD writeback cache, S3 concurrency 8,
and upload concurrency 16. Redis uses six writeback workers. TiKV uses four
workers, eight FUSE workers, `max_background=256`, and a 6 GiB BrewFS memory
budget so TiKV, PD, and RustFS retain headroom.

JuiceFS uses a 4 GiB buffer, an 8 GiB persistent cache, `cache-large-write`,
four upload connections, four stage-write threads, and a 1-second,
65,536-entry open cache. Backup and usage reporting are disabled. The tested
JuiceFS build does not support `--max-downloads`, so the runner records and
skips that requested option.

Larger settings were not automatically accepted. Eight BrewFS Redis upload
workers reduced mixed I/O by about 23% during fio and 12% after drain. An
8 GiB combined read/write memory allocation alongside TiKV caused multi-minute
close tails. Both candidates were rejected.

### Data-path throughput seen by applications

Actual bytes divided by complete fio process wall time, including close and
fsync. Mixed random I/O is read plus write throughput.

| Workload | BrewFS Redis | JuiceFS Redis | BrewFS TiKV | JuiceFS TiKV |
| --- | ---: | ---: | ---: | ---: |
| Large read | **194.34 MiB/s** | 194.17 MiB/s | **194.35 MiB/s** | 194.09 MiB/s |
| Sequential read | 740.60 MiB/s | **1,000.76 MiB/s** | 892.00 MiB/s | **1,018.67 MiB/s** |
| Random read | **1,726.10 MiB/s** | 1,256.80 MiB/s | 1,253.33 MiB/s | **1,264.20 MiB/s** |
| Large write | **195.05 MiB/s** | 102.40 MiB/s | 95.26 MiB/s | **97.52 MiB/s** |
| Sequential write | **189.33 MiB/s** | 117.94 MiB/s | **128.28 MiB/s** | 117.12 MiB/s |
| Random write | **178.17 MiB/s** | 117.43 MiB/s | **138.17 MiB/s** | 113.92 MiB/s |
| Mixed random I/O | **495.54 MiB/s** | 328.76 MiB/s | **315.31 MiB/s** | 183.60 MiB/s |

Read rows use durable prefill and a cache-preserving remount. They measure the
local persistent-cache path, not cold S3 download. Large read uses three
remount/read rounds with targeted `POSIX_FADV_DONTNEED` eviction of each
filesystem's local cache files; the reported value is the median. All four
stable Large read artifacts recorded zero object GET bytes during measurement.

### A repeatable 2.4 GiB/s hot-cache read path

The persistent-cache table deliberately evicts local cache pages between its
remount/read rounds. We also measured the different, fully hot local
page-cache case: after durable prefill and a cache-preserving remount, each
filesystem runs five unreported 4 GiB warmup passes followed by three measured
4 GiB passes in the same mount, without cache eviction or another remount.
Neither system read an object during the measured phase.

| System | Median | Three-run range | Spread | Warmup |
| --- | ---: | ---: | ---: | ---: |
| BrewFS Redis | **2,395.32 MiB/s** | 2,354.02-2,436.64 MiB/s | 3.45% | 5 passes |
| JuiceFS Redis | 2,356.73 MiB/s | 2,312.82-2,399.53 MiB/s | 3.68% | 5 passes |

This is a repeatable host-memory-cache result, not an SSD-cache or cold S3
claim, and is therefore presented separately from the main matrix. BrewFS's
promoted-slice cache is validated by zero S3 GETs; its generic block-cache hit
counter does not include that path.

### Write throughput after every queued byte is drained

This finish line includes the time required to empty the writeback queue.

| Workload | BrewFS Redis | JuiceFS Redis | BrewFS TiKV | JuiceFS TiKV |
| --- | ---: | ---: | ---: | ---: |
| Large write | 66.47 MiB/s | **79.13 MiB/s** | 61.34 MiB/s | **77.36 MiB/s** |
| Sequential write | 88.50 MiB/s | **144.44 MiB/s** | 85.63 MiB/s | **144.13 MiB/s** |
| Random write | 89.31 MiB/s | **138.89 MiB/s** | 126.65 MiB/s | **146.37 MiB/s** |
| Mixed random I/O | **218.60 MiB/s** | 113.86 MiB/s | **298.37 MiB/s** | 117.99 MiB/s |

The split shows that BrewFS's SSD cache is being used effectively: foreground
pure writes finish sooner and mixed I/O remains strong through drain. Remote
completion of pure writes is still slower, especially for large batches,
because upload and metadata commit costs remain after foreground completion.

### Metadata throughput: strong reads, clear transaction targets

| Operation | BrewFS Redis | JuiceFS Redis | BrewFS TiKV | JuiceFS TiKV |
| --- | ---: | ---: | ---: | ---: |
| Create | **1,004.61 ops/s** | 549.01 ops/s | 84.53 ops/s | **151.85 ops/s** |
| Open | 4,827.50 ops/s | **11,883.85 ops/s** | 1,137.44 ops/s | **10,534.28 ops/s** |
| Stat | **726,234.51 ops/s** | 724,328.73 ops/s | **724,066.83 ops/s** | 637,033.56 ops/s |
| Readdir | **28,634.94 ops/s** | 16,020.25 ops/s | **15,983.11 ops/s** | 8,675.80 ops/s |
| Rename | 894.64 ops/s | **1,313.40 ops/s** | 125.31 ops/s | **256.68 ops/s** |

BrewFS leads stat and readdir plus Redis create. Open, rename, and TiKV create
remain clear round-trip and transaction-reduction targets.

### Every published result has an artifact

Complete-matrix artifacts are `perf-run-1784876227-9934` (BrewFS Redis),
`juicefs-perf-run-1784879992-17249` (JuiceFS Redis),
`perf-run-1784878850-2470` (BrewFS TiKV), and
`juicefs-perf-run-1784880709-16460` (JuiceFS TiKV). Stable Large read artifacts
are `perf-run-1784889951-27900` (BrewFS Redis),
`juicefs-perf-run-1784890100-1138` (JuiceFS Redis),
`perf-run-1784890275-8226` (BrewFS TiKV), and
`juicefs-perf-run-1784890444-29768` (JuiceFS TiKV). Each contains effective
settings, raw fio JSON, metadata logs, cache and object counters, process wall
time, post-write drain samples, warning summaries, and a generated report.

The hot local-cache Redis Large read artifacts are
`perf-run-1784894177-23661` (BrewFS) and
`juicefs-perf-run-1784894284-19194` (JuiceFS).

The workspace correctness gate passed 1,166 Rust tests across its two workspace
test phases with zero failures before these results were accepted. This remains
a single-host engineering result with local RustFS, not a claim that one
filesystem wins every workload or deployment.

## Choose metadata and object storage independently

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

This separation supports more than one deployment shape:

| Deployment stage | Metadata backend | Data backend | Typical use |
| --- | --- | --- | --- |
| Local development | SQLite | Local filesystem | Single-node evaluation |
| Small shared service | Redis or PostgreSQL | S3-compatible storage | CI, build, or team workspace |
| Distributed deployment | TiKV or etcd | S3-compatible storage | Scaled metadata and capacity |

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

## Keep object storage off the critical path

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

## A benchmark must count the finish line

Filesystem benchmarks can be misleading when they stop timing as soon as an
application finishes issuing writes. A fast foreground result may simply mean
that more work was left in memory or on local SSD for `close`, `flush`, or a
background uploader to complete later.

BrewFS ships Docker Compose performance runners that record fio bytes, process
wall time, post-write drain, cache state, object requests, warnings, and
effective profile settings. The published end-to-end table uses process wall
plus post-drain, so time spent in close or fsync cannot disappear.

For writeback profiles, the report also records the remount cache protocol:
the prefill drain, preserved cache root, and block-cache/object-store counters
are all captured beside the fio result. This makes it possible to distinguish
a local disk-cache hit from an object-store request and to compare systems
without silently changing cache lifetime semantics.

This snapshot remains a single-host engineering result: 14 GiB memory, a
512 MiB fio working set, 20-second runs, one metadata service, and local
RustFS. It is evidence for this profile, not a claim about every deployment.

Reproduce the same profiles with:

```bash
PERF_FIO_SIZE=512m PERF_FIO_RUNTIME=20 \
  bash docker/compose-xfstests/run_redis_perf.sh --s3 \
  --writeback-throughput-profile

PERF_FIO_SIZE=512m PERF_FIO_RUNTIME=20 \
  bash docker/compose-xfstests/run_juicefs_perf.sh \
  --writeback-throughput-profile
```

## Correctness is a performance requirement

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

## Try BrewFS in minutes

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

## Build the next storage layer with us

BrewFS is still evolving, and the next gains are concrete: fewer metadata
round trips, stronger multi-client cache invalidation, lower write
amplification, and better operational tooling for compaction, recovery, and
observability.

Contributions do not need to start in the deepest part of the write path. The
project benefits from new backend adapters, workload reports, documentation,
portability fixes, test coverage, and production feedback just as much as from
core filesystem changes.

If you are building AI pipelines, CI infrastructure, or shared data platforms,
bring a real workload and challenge these results. Explore the
[BrewFS repository](https://github.com/brewfs/brewfs), read the
[architecture guide](https://github.com/brewfs/brewfs/blob/main/doc/architecture/arch.md),
or reproduce the
[performance comparison](https://github.com/brewfs/brewfs#performance-vs-juicefs).

The filesystem interface may be decades old. The storage stack behind it does
not have to be.

---

Editorial disclosure: this article was drafted with assistance from OpenAI
Codex.
