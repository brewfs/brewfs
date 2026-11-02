# PR #404 round-two evidence

All measurements use production source `d5e53e1ca532762cf7ffb361430d939a0b97eb7c`.
The later repository changes contain documentation and evidence only.

- Binary SHA-256: `e2f12ee08c23f29369be8982ecf87b416d2f190a8cbf85e652c773dfffb32313`.
- [source-manifest.json](source-manifest.json) contains 1,858 individual tracked-file hashes;
  its SHA-256 is `5e954a6ff76ac0c6fa3828d2148620c32d7eb16022d718a8f9a2f82c8fde26bf`.
- [build-identity.json](build-identity.json) records features, compiler scheduling and the
  pre-existing untracked file responsible for `dirty=true`.
- [ci-summary.json](ci-summary.json) records 17 passing local checks.
  [overlay-summary.json](overlay-summary.json) retains both complete zero-failure suites
  and all five focused runs, including the one EBUSY failure (4/5 passed).
- [comparison.json](comparison.json) and [comparison-table.md](comparison-table.md) retain
  all 36 warmup, measured and direct arms. Primary confidence intervals use only the
  three measured pairs per scene.
- [broad-summary.json](broad-summary.json), [broad-comparison.md](broad-comparison.md) and
  [broad-exact-wall.json](broad-exact-wall.json) include complete workloads and tail costs.
  [broad-retry.json](broad-retry.json) identifies the invalid first on arm and its full
  unchanged rerun; [broad-observation-failure.json](broad-observation-failure.json) retains
  the empty stats sample. No failing marker or assertion was removed to obtain a pass.
- [seqread-recheck-summary.json](seqread-recheck-summary.json) and
  [bigread-recheck-summary.json](bigread-recheck-summary.json) retain the additional read
  comparisons. Bigread's reported p99 is histogram-capped and cannot establish its true
  p99; the primary write matrix's p99 measurements are valid.
- [correctness-summary.json](correctness-summary.json) contains release content and
  direct/buffered fsync/close → SIGKILL → remount verification.
- [acceptance.json](acceptance.json) and [supplemental-review.json](supplemental-review.json)
  record the final gates; scope and statistical limits remain in the report.

`raw-evidence.tar.gz` contains raw fio, PUT/RSS/drain records, all validation logs,
the original failed broad arm, harnesses and environment snapshots. Extract with
`tar -xzf raw-evidence.tar.gz`; [raw-evidence-index.json](raw-evidence-index.json) lists
each archived file's size and SHA-256. `SHA256SUMS` covers the published evidence files.

The complete host artifact directory is `/home/hxy/perf-write-coalesce-r2-20261102/`.
The 399 MB executable, CPU profiles and large FUSE traces remain there rather than in
Git. Task compose services/volumes were removed, with no task FUSE mount or D-state
process remaining; the unrelated JuiceFS mount was preserved.
