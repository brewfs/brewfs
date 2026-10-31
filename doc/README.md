# BrewFS Documentation

This directory is the canonical documentation tree for BrewFS. Keep current
architecture, operations, testing, protocol, bug-fix, and workspace material
under `doc/`; documented cross-repository innovations and their evidence live
under [innovations/](../innovations/README.md). Historical execution records belong under `superpowers/` and
should not be treated as current behavior without checking the source.

## Start Here

| Topic | Document |
|---|---|
| Project overview and Quick Start | [../README.md](../README.md) |
| 中文项目说明 | [../README_CN.md](../README_CN.md) |
| Architecture overview | [architecture/arch.md](architecture/arch.md) |
| Configuration | [operations/configuration.md](operations/configuration.md) |
| Binary deployment | [operations/binary-deployment.md](operations/binary-deployment.md) |
| Current packed performance evidence | [performance/native-packed-base-large-scale-validation-2026-09-24.md](performance/native-packed-base-large-scale-validation-2026-09-24.md) |
| Docker and CI test guide | [testing/docker-compose-test-guide.md](testing/docker-compose-test-guide.md) |
| BrewFS–RustFS cooperative I/O innovations | [../innovations/README.md](../innovations/README.md) |
| VFS internals | [vfs/README.md](vfs/README.md) |

## Directory Layout

| Directory | Purpose |
|---|---|
| [architecture/](architecture/) | Core layout, metadata, data path, cache, consistency, POSIX behavior, and compaction/GC design. |
| [operations/](operations/) | Runtime configuration, control plane, observability, profiling, SDK, and stats tooling. |
| [testing/](testing/) | Benchmark, compose, fuzz, lock, xfstests, and CI-oriented test guidance. |
| [performance/](performance/) | Current packed metadata performance evidence and reproducible cold-read rules. |
| [protocols/](protocols/) | Multi-protocol gateway specs (S3, WebDAV, NFS), shared conventions, and the milestone roadmap. |
| [vfs/](vfs/) | VFS module-specific implementation guide. |
| [bugfix/](bugfix/) | Historical bug investigations and fix notes that remain useful for regression context. |
| [superpowers/](superpowers/) | Dated agent plans and specs. Treat these as historical execution records unless a plan is explicitly current. |

## Architecture

| Topic | Document |
|---|---|
| System overview | [architecture/arch.md](architecture/arch.md) |
| Metadata model | [architecture/meta.md](architecture/meta.md) and [architecture/metadata.md](architecture/metadata.md) |
| Chunk and data layout | [architecture/chunk.md](architecture/chunk.md) and [architecture/data-layout.md](architecture/data-layout.md) |
| Read path | [architecture/read-path.md](architecture/read-path.md) |
| Write path | [architecture/write-path.md](architecture/write-path.md) |
| Caching | [architecture/caching.md](architecture/caching.md) |
| Consistency and CAS | [architecture/consistency.md](architecture/consistency.md), [architecture/redis-version-cas.md](architecture/redis-version-cas.md) |
| POSIX namespace behavior | [architecture/permissions.md](architecture/permissions.md), [architecture/link_symlink.md](architecture/link_symlink.md), [architecture/rename_design.md](architecture/rename_design.md) |
| Compaction and GC | [architecture/compaction-gc.md](architecture/compaction-gc.md) |

## Operations

| Topic | Document |
|---|---|
| Configuration | [operations/configuration.md](operations/configuration.md) |
| Binary deployment | [operations/binary-deployment.md](operations/binary-deployment.md) |
| Control plane | [operations/control-plane.md](operations/control-plane.md) |
| Manager lifecycle API | [operations/manager.md](operations/manager.md) |
| Observability | [operations/observability.md](operations/observability.md) |
| Profiling | [operations/profiling.md](operations/profiling.md) |
| Stats tool | [operations/stats-tool.md](operations/stats-tool.md) |
| WebDAV gateway usage | [operations/webdav-gateway.md](operations/webdav-gateway.md) |
| WebDAV protocol spec | [protocols/webdav.md](protocols/webdav.md) |
| WebDAV verification | [protocols/webdav-gateway-verification.md](protocols/webdav-gateway-verification.md) |

## Testing And CI

| Topic | Document |
|---|---|
| Docker compose filesystem tests | [testing/docker-compose-test-guide.md](testing/docker-compose-test-guide.md) |
| Benchmarks | [testing/bench.md](testing/bench.md) |
| Fuzz testing | [testing/fuzz_testing_guide.md](testing/fuzz_testing_guide.md) |
| File lock testing | [testing/file_lock_testing_guide.md](testing/file_lock_testing_guide.md) |
| xfstests fixes | [testing/xfstests-091-001-fix.md](testing/xfstests-091-001-fix.md) |
| pjdfstest compose plan | [superpowers/plans/2026-06-13-pjdfstest-compose.md](superpowers/plans/2026-06-13-pjdfstest-compose.md) |
| GitHub Actions DAG plan | [superpowers/plans/2026-06-14-github-actions-dag-reorg.md](superpowers/plans/2026-06-14-github-actions-dag-reorg.md) |

## Performance And Comparison

| Topic | Document |
|---|---|
| Current packed validation | [performance/native-packed-base-large-scale-validation-2026-09-24.md](performance/native-packed-base-large-scale-validation-2026-09-24.md) |

## Historical Plans

Long-running implementation plans and design specs live under:

- [superpowers/plans/](superpowers/plans/)
- [superpowers/specs/](superpowers/specs/)

These files are useful as historical context. Prefer updating the current
roadmap or creating a new dated plan instead of rewriting old completed plans.
