# Performance Documents

This directory contains active performance roadmaps, BrewFS/JuiceFS comparison
notes, and focused review outputs from previous tuning passes.

## Current Planning

- [perf-optimization-roadmap.md](perf-optimization-roadmap.md): current
  performance improvement roadmap and validation target.
- [performance-roadmap.md](performance-roadmap.md): broader backlog and staged
  optimization ideas.
- [brewfs-vs-juicefs-performance-analysis-2026-06.md](brewfs-vs-juicefs-performance-analysis-2026-06.md):
  **(2026-06) comprehensive BrewFS/JuiceFS performance report** — fresh measured
  A/B benchmarks (identical Redis meta + local-fs object store) cross-referenced
  with historical S3 numbers, a verified per-subsystem code+architecture gap
  analysis (7 subsystems, with `file:line` root causes), and a P0/P1/P2
  improvement plan. Headline: BrewFS write throughput is a flat ~210 MiB/s that
  does not scale with concurrency (6.6–23.6× behind JuiceFS).
- [brewfs-performance-advantages-over-juicefs.md](brewfs-performance-advantages-over-juicefs.md):
  attribution of the current Redis + RustFS benchmark wins to architecture,
  workload profiles, caching, and Rust runtime properties, including the
  `O_RDWR` open-cache gap.
- [bench-2026-06-21/](bench-2026-06-21/): raw measured data (`summary.tsv`,
  `comparison.md`) and the reproducible host-native benchmark harness
  (`run_bench.sh`, `metabench.c`, `parse_fio.py`, `combine_results.py`) backing
  the 2026-06 report.
- [brewfs-vs-juicefs-analysis.md](brewfs-vs-juicefs-analysis.md): high-level
  BrewFS/JuiceFS comparison (2026-05).
- [small-file-read-write-performance-optimization.md](small-file-read-write-performance-optimization.md):
  small-file read/write optimization notes.
- [performance-agent-guide.md](performance-agent-guide.md): detailed
  performance-work guidance, acceptance criteria, and current optimization
  gaps.

## Review Notes

- [perf-agent-metadata-cache.md](perf-agent-metadata-cache.md): metadata cache
  analysis from perf review.
- [review-metadata-cache.md](review-metadata-cache.md): metadata cache review.
- [review-read-cache.md](review-read-cache.md): read cache review.
- [review-object-store-cache.md](review-object-store-cache.md): object store
  cache review.
- [review-writeback-writer.md](review-writeback-writer.md): writeback writer
  review.
- [review-perf-harness-config.md](review-perf-harness-config.md): perf harness
  configuration review.
- [review-parallel-agents-summary.md](review-parallel-agents-summary.md):
  summary of parallel review findings.
