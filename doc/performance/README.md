# Performance evidence

This directory retains measurements, analysis, rejected-candidate evidence and
the correctness reports still cited by the canonical
[packed-meta](../superpowers/specs/packed-meta.md) and
[overlay-workspace](../superpowers/specs/overlay-workspace.md) specifications.
Each report applies only to its recorded source, binary, environment and date;
its presence here is not a new acceptance claim.

The October 20 consolidation removed 27 superseded per-round/per-gate reports
that neither canonical spec nor the architecture documents cite as acceptance
evidence. References to those reports now point to their exact historical
content in Git, rather than to missing local files. No raw benchmark artifacts
were removed. The [consolidation inventory](../superpowers/plans/2026-10-20-spec-consolidation.md)
lists every kept/deleted file and the link-check procedure.

## Measurements and analysis

| Report | Evidence boundary |
| --- | --- |
| [aliyun-packed-100k-cold-read-test-plan-2026-09-28.md](aliyun-packed-100k-cold-read-test-plan-2026-09-28.md) | Historical test design; not a completed result. |
| [aliyun-packed-cold-read-2026-09-28.md](aliyun-packed-cold-read-2026-09-28.md) | Matched 1,000-file full-payload comparison and metadata-latency limits. |
| [aliyun-packed-vs-juicefs-triple-1m-2026-10-01.md](aliyun-packed-vs-juicefs-triple-1m-2026-10-01.md) | 10k and ordered/shuffled 1M observations; TTL symmetry needs revalidation. |
| [aliyun-packed-vs-juicefs-smallfiles-2026-09-26.md](aliyun-packed-vs-juicefs-smallfiles-2026-09-26.md) | Matched 10,000-file, 100 KiB strict cold comparison after fixture correction. |
| [native-packed-base-large-scale-validation-2026-09-24.md](native-packed-base-large-scale-validation-2026-09-24.md) | Large-file read evidence and 100k-file functional validation; shared-slice small-file numbers are superseded. |
| [packed-decoded-frame-cache-local-2026-10-01.md](packed-decoded-frame-cache-local-2026-10-01.md) | Repeated local warm-frame-cache comparison; not strict-cold acceptance. |
| [packed-index-budget-candidate-2026-10-02.md](packed-index-budget-candidate-2026-10-02.md) | Rejected candidate with mounted EIO; no accepted throughput claim. |
| [packed-metadata-cache-analysis-2026-09-29.md](packed-metadata-cache-analysis-2026-09-29.md) | Cache/request analysis, retained as diagnosis. |
| [packed-vs-juicefs-tikv-metadata-stat-2026-09-30.md](packed-vs-juicefs-tikv-metadata-stat-2026-09-30.md) | Metadata-only workload and local baseline; TiKV image blocker is explicit. |

## Spec-cited correctness evidence

These dated checkpoints retain their original limits and failures. Passing a
focused correctness gate does not establish full product qualification or a
performance advantage.

- [packed-cold-hardlink-validation-2026-10-03.md](packed-cold-hardlink-validation-2026-10-03.md)
- [packed-completion-correctness-2026-10-02.md](packed-completion-correctness-2026-10-02.md)
- [packed-external-validation-2026-10-04.md](packed-external-validation-2026-10-04.md)
- [packed-frozen-source-seek-validation-2026-10-04.md](packed-frozen-source-seek-validation-2026-10-04.md)
- [packed-metadata-closeout-validation-2026-10-07.md](packed-metadata-closeout-validation-2026-10-07.md)
- [packed-namespace-posix-validation-2026-10-04.md](packed-namespace-posix-validation-2026-10-04.md)
- [packed-namespace-validation-2026-10-04.md](packed-namespace-validation-2026-10-04.md)
- [packed-pagination-errno-validation-2026-10-04.md](packed-pagination-errno-validation-2026-10-04.md)
- [packed-source-batch-validation-2026-10-04.md](packed-source-batch-validation-2026-10-04.md)
- [packed-source-validation-2026-10-03.md](packed-source-validation-2026-10-03.md)

## Current workspace experiment

The [r31 delivery handoff](../superpowers/plans/2026-10-17-overlay-no-seal-handoff.md)
records the shipped overlay baseline. The
[entity mount checkpoint](../superpowers/plans/2026-10-20-overlay-entity-mount.md)
records the subsequent preliminary Redis/RustFS experiment. They retain
different format and validation boundaries and must not be combined into an
acceptance result.

Cold-read comparisons are valid only under their documented cache policy and
matched backend/workload settings. A cache hit, interrupted run, known failure
or historical TTL caveat must remain visible when citing a result.
