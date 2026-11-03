# Issue #391 opt-a evidence

This is a draft candidate. The local workspace publication queue is bounded;
the prescribed mounted pressure workload still returns EIO and does not trigger
the new admission limit. End-to-end RSS and metadata p99 acceptance is open.
See [the report](../../issue-391-rest-2026-11-02.md) for the acceptance decision.

`summary.json` contains source/binary identities, per-cell exit status, invalid
protocol markers, local CI summaries, object/pressure measurements and the paired
regression result. A successful fio prefix in an unsuccessful cell is diagnostic
data, not an accepted throughput measurement.

`review-evidence.tar.gz` contains the measurement scripts, frozen product patch,
test logs, object inventories, fio JSON/argv, phase/drain boundaries and sampled
process/metric observations. It excludes ELF binaries, temporary backend data
and the large per-request comments embedded in `.stats`. All numerical metric
lines are preserved verbatim in `selected-metrics/`. `raw-index.json` records both
the original local-file hash and the packaged-file hash/path, explicitly marking
the comment filtering. Full original artifacts remain in `/data/out/issue391/`.

The independent 4 KiB object matrix has four modes; the sparse-write matrix has
another four. `cand-aggregate-d1-c65536` is excluded because its initial counter
snapshot still had a preflight PUT in flight. The baseline and candidate
`*-aggregate-d1-c65536-drained-r2` reruns use the same data disk and strict
zero-debt preflight boundary. Later matched pressure and A/B cells use that same
boundary. Setup failures and invalid cells are retained with their status.

`cand-broad-r3` passes all ten scenes; its metadata tools are the standard Python
fallbacks. `cand-broad-r2` is preserved with its missing-cache-directory harness
failure. The rerun configures and creates an explicit cache root only for broad;
zero SSD budgets mean its page-eviction log correctly reports zero files/bytes.
All three bigread repeats and the warmup are included. This broad smoke does not
replace the fixed three-pair protocol or the failed workspace pressure guard.

Verify the delivered files from this directory:

```bash
sha256sum -c SHA256SUMS
```

To replay numerical analysis in a fresh temporary directory:

```bash
issue391_review_dir=$(mktemp -d)
tar -xzf review-evidence.tar.gz -C "$issue391_review_dir"
cp -a "$issue391_review_dir/selected-metrics/." "$issue391_review_dir/"
python3 "$issue391_review_dir/analyze-baseline.py" \
  --root "$issue391_review_dir" --prefix base- --output-stem replay-baseline
python3 "$issue391_review_dir/analyze-baseline.py" \
  --root "$issue391_review_dir" --prefix cand- --output-stem replay-candidate
python3 "$issue391_review_dir/paired.py" \
  --pair "$issue391_review_dir/ab-1-base:$issue391_review_dir/ab-1-cand" \
  --pair "$issue391_review_dir/ab-2-base:$issue391_review_dir/ab-2-cand" \
  --pair "$issue391_review_dir/ab-3-base:$issue391_review_dir/ab-3-cand" \
  --output "$issue391_review_dir/replay-paired.json"
python3 "$issue391_review_dir/paired-comparator.py" --root "$issue391_review_dir"
```

`paired.py` requires three successful, protocol-valid pairs with all five scenes.
It reports paired Student t intervals with two degrees of freedom. A bandwidth
gate requires the 95% paired-percent lower bound >= -3%; p99 requires the upper
bound <= +10%. A wide interval does not establish equivalence. Flat-v1 regression
does not substitute for workspace RSS or metadata p99 acceptance.

The native repository comparator and its helper are copied without modification
in `repo-tools/`. `paired-comparator.py` pairs its native unrounded derived
values. Native wall uses integer-second tool time and excludes subsequent drain;
native active+drain uses integer-second drain. The separate high-resolution
phase intervals include close and strict drain in wall. Both definitions are
retained: several native intervals are too wide to establish the -3% bound.

For plots, install matplotlib in the temporary directory and use the saved
observations:

```bash
python3 -m pip install --target "$issue391_review_dir/plot-deps" matplotlib
python3 "$issue391_review_dir/plot-pressure.py" base-data-normal cand-data-normal \
  --root "$issue391_review_dir" --output "$issue391_review_dir/matched-pressure.svg"
```

Plots show the tool phase including close, before the later strict-drain phase.
Baseline internal pending gauges are absent and are not drawn as zero. The crash
probe verifies successful fsync prefixes after mount-process death with a new
cache; it does not test backend-server power loss or validate all failed fio data.

## Generated artifacts

`raw-index.json`, `review-evidence.tar.gz` and the rendered `.png`/`.svg` charts are
not committed: they are regenerated from the CSVs plus the SHA256SUMS manifest and
were ~40 MB of binary/large JSON in git history. The archived copy lives outside the
repository at `brewfs:/home/hxy/issue410/` and is referenced from PR #408.
