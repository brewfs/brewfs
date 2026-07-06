# BrewFS Documentation

This directory is the canonical documentation tree for BrewFS. Keep new design
notes, operations guides, performance analysis, test plans, and implementation
plans under `doc/` unless a tool explicitly requires another location.

## Start Here

| Topic | Document |
|---|---|
| Architecture overview | [architecture/arch.md](architecture/arch.md) |
| Configuration | [operations/configuration.md](operations/configuration.md) |
| Binary deployment | [operations/binary-deployment.md](operations/binary-deployment.md) |
| Current performance roadmap | [performance/perf-optimization-roadmap.md](performance/perf-optimization-roadmap.md) |
| BrewFS vs JuiceFS comparison | [performance/brewfs-vs-juicefs-analysis.md](performance/brewfs-vs-juicefs-analysis.md) |
| Docker and CI test guide | [testing/docker-compose-test-guide.md](testing/docker-compose-test-guide.md) |
| VFS internals | [vfs/README.md](vfs/README.md) |

## Directory Layout

| Directory | Purpose |
|---|---|
| [architecture/](architecture/) | Core layout, metadata, data path, cache, consistency, POSIX behavior, and compaction/GC design. |
| [operations/](operations/) | Runtime configuration, control plane, observability, profiling, SDK, and stats tooling. |
| [testing/](testing/) | Benchmark, compose, fuzz, lock, xfstests, and CI-oriented test guidance. |
| [performance/](performance/) | Performance roadmap, JuiceFS comparisons, and focused review notes from previous tuning passes. |
| [meta-api/](meta-api/) | Meta client API audit, mapping, extension plan, and read/write follow-up work. |
| [juicefs/](juicefs/) | JuiceFS internals notes used for cross-project comparison. |
| [gap/](gap/) | BrewFS/JuiceFS module gap analysis and iteration roadmap. |
| [vfs/](vfs/) | VFS module-specific implementation guide. |
| [bugfix/](bugfix/) | Historical bug investigations and fix notes that are still useful for regression context. |
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
| Observability | [operations/observability.md](operations/observability.md) |
| Profiling | [operations/profiling.md](operations/profiling.md) |
| Stats tool | [operations/stats-tool.md](operations/stats-tool.md) |
| SDK | [operations/sdk.md](operations/sdk.md) |

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

## Performance And JuiceFS Comparison

| Topic | Document |
|---|---|
| Current performance roadmap | [performance/perf-optimization-roadmap.md](performance/perf-optimization-roadmap.md) |
| Broader performance backlog | [performance/performance-roadmap.md](performance/performance-roadmap.md) |
| Metadata cache analysis | [performance/perf-agent-metadata-cache.md](performance/perf-agent-metadata-cache.md), [performance/review-metadata-cache.md](performance/review-metadata-cache.md) |
| Read/object/writeback reviews | [performance/review-read-cache.md](performance/review-read-cache.md), [performance/review-object-store-cache.md](performance/review-object-store-cache.md), [performance/review-writeback-writer.md](performance/review-writeback-writer.md) |
| Perf harness review | [performance/review-perf-harness-config.md](performance/review-perf-harness-config.md) |
| Small-file optimization notes | [performance/small-file-read-write-performance-optimization.md](performance/small-file-read-write-performance-optimization.md) |
| BrewFS vs JuiceFS overview | [performance/brewfs-vs-juicefs-analysis.md](performance/brewfs-vs-juicefs-analysis.md) |
| JuiceFS internals | [juicefs/README.md](juicefs/README.md) |
| Gap analysis | [gap/README.md](gap/README.md) |

## Historical Plans

Long-running implementation plans and design specs live under:

- [superpowers/plans/](superpowers/plans/)
- [superpowers/specs/](superpowers/specs/)

These files are useful as historical context. Prefer updating the current
roadmap or creating a new dated plan instead of rewriting old completed plans.
