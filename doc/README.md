# BrewFS Documentation

This directory is the canonical documentation tree for BrewFS. Keep new design
notes, operations guides, performance analysis, test plans, and implementation
plans under `doc/` unless a tool explicitly requires another location.

## Start Here

| Topic | Document |
|---|---|
| Project architecture | [arch.md](arch.md) |
| Configuration | [configuration.md](configuration.md) |
| Metadata model | [meta.md](meta.md) and [metadata.md](metadata.md) |
| Chunk layout | [chunk.md](chunk.md) and [data-layout.md](data-layout.md) |
| Read path | [read-path.md](read-path.md) |
| Write path | [write-path.md](write-path.md) |
| Caching | [caching.md](caching.md) |
| Compaction and GC | [compaction-gc.md](compaction-gc.md) |
| Observability and profiling | [observability.md](observability.md), [profiling.md](profiling.md), [stats-tool.md](stats-tool.md) |
| SDK | [sdk.md](sdk.md) |
| Control plane | [control-plane.md](control-plane.md) |

## Subsystem Guides

| Area | Documents |
|---|---|
| VFS internals | [vfs/README.md](vfs/README.md) |
| POSIX and permission behavior | [permissions.md](permissions.md), [link_symlink.md](link_symlink.md), [rename_design.md](rename_design.md) |
| Redis metadata CAS | [redis-version-cas.md](redis-version-cas.md) |
| Metadata API planning | [meta_client_api_audit.md](meta_client_api_audit.md), [meta_client_api_mapping.md](meta_client_api_mapping.md), [meta_client_api_extension_plan.md](meta_client_api_extension_plan.md), [meta_client_readwrite_todo.md](meta_client_readwrite_todo.md) |
| Bug investigations | [bugfix/](bugfix/) and [generic-074-debug-history.md](generic-074-debug-history.md) |

## Testing And CI

| Topic | Document |
|---|---|
| Docker compose filesystem tests | [docker-compose-test-guide.md](docker-compose-test-guide.md) |
| Benchmarks | [bench.md](bench.md) |
| Fuzz testing | [fuzz_testing_guide.md](fuzz_testing_guide.md) |
| File lock testing | [file_lock_testing_guide.md](file_lock_testing_guide.md) |
| xfstests fixes | [xfstests-091-001-fix.md](xfstests-091-001-fix.md) |
| pjdfstest compose plan | [superpowers/plans/2026-06-13-pjdfstest-compose.md](superpowers/plans/2026-06-13-pjdfstest-compose.md) |
| pjdfstest POSIX fix plan | [superpowers/plans/2026-06-13-pjdfstest-posix-fixes.md](superpowers/plans/2026-06-13-pjdfstest-posix-fixes.md) |
| pjdfstest special-node plan | [superpowers/plans/2026-06-13-pjdfstest-special-nodes.md](superpowers/plans/2026-06-13-pjdfstest-special-nodes.md) |
| GitHub Actions DAG plan | [superpowers/plans/2026-06-14-github-actions-dag-reorg.md](superpowers/plans/2026-06-14-github-actions-dag-reorg.md) |

## Performance And JuiceFS Comparison

| Topic | Document |
|---|---|
| Current performance roadmap | [perf-optimization-roadmap.md](perf-optimization-roadmap.md) and [performance-roadmap.md](performance-roadmap.md) |
| Metadata cache analysis | [perf-agent-metadata-cache.md](perf-agent-metadata-cache.md), [review-metadata-cache.md](review-metadata-cache.md) |
| Read/object/writeback reviews | [review-read-cache.md](review-read-cache.md), [review-object-store-cache.md](review-object-store-cache.md), [review-writeback-writer.md](review-writeback-writer.md) |
| Perf harness review | [review-perf-harness-config.md](review-perf-harness-config.md) |
| BrewFS vs JuiceFS overview | [brewfs-vs-juicefs-analysis.md](brewfs-vs-juicefs-analysis.md) |
| JuiceFS internals | [juicefs/README.md](juicefs/README.md) |
| Gap analysis | [gap/README.md](gap/README.md) |

## Plans And Specs

Long-running implementation plans and design specs live under:

- [superpowers/plans/](superpowers/plans/)
- [superpowers/specs/](superpowers/specs/)

These files are useful as historical context. Prefer updating the current
roadmap or creating a new dated plan instead of rewriting old completed plans.
