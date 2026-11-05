# Small-file warm latency evidence

These are compact observations for the experiment documented in
[the plan](../../../superpowers/plans/2026-11-04-smallfile-warm-latency.md).
The candidate misses the 0.40 ms/file target. These observations do not
establish performance acceptance.

- `paired-summary.json`: the original Compose scanner, twelve fresh mounts,
  three alternating main/candidate pairs for warm and cold scans; includes
  binary/source identity, per-run data, paired t tests and 95% intervals.
- `attribution.json`: additive current-main syscall stages for direct=1/0,
  counters and request samples; three repeated scans on each diagnostic mount.
- `attribution-candidate.json`: the same separate diagnostic for candidate
  direct=1. Its repeated-scan timings do not replace the original Compose
  acceptance measurements.
- `paired-traces.json`: all 374 samples from each of six warm acceptance
  arms, recovered from timeseries plus final snapshots and deduplicated.
  Request segments can overlap between requests and are not a wall-time sum.
- `historical-attribution.json` and `historical-exclusive.json`: recovered
  historical #409 evidence; neither the historical nor native baseline was
  re-benchmarked to produce these files.
- `host-environment.json`: current kernel, compiler and Python versions.
- `manifest-key.source.json`: deterministic corpus provenance: 147 complete
  inline groups, 8,192 dentries, 32 MiB payload and zero external frames.
- `validation-summary.json`: all candidate local CI exit codes, test counts,
  log hashes and experiment input identities. Ignored tests are explicitly
  reported and are not counted as executed passes.
- `original-guard-remount-failure.log`: the original candidate mixed guard
  stopped at remount with EADDRINUSE, before timed fio; it is not a valid pair.
- `diagnostic-mixed-summary.json`: three follow-up mixed pairs after adding
  an explicit old-process exit wait. These are diagnostic only; their broad
  confidence intervals do not exclude a material regression. It also records
  full-drain amplification, zero final debt, and additional exit waits.
- `run-guard-diagnostic.sh`: the exact cleanup change used only for those
  diagnostic runs. The original failed artifact is retained separately.
- `remount-probe-status.tsv` and `diagnostic-mixed-status.tsv`: four idle
  remount probes (five remounts each) and six load-bearing diagnostic arms.
- Python scripts reproduce the analysis from the retained raw artifacts;
  their default root is the machine-local path below.

Raw logs, metrics, every-file SHA-256 manifests, preserved executables,
the exact reproduction patch and all experiment scripts are retained at:

```text
/mnt/brewfs-data/home/hxy/smallfile-warm/
```

Main source is `f5706712302435206269a253bf861a1a648761d1`.
Main executable SHA-256 is
`e2a308519ad1e0f5d2295e10cf3fb9eb64ce57009eb64b01c87489b51128f34f`;
candidate executable SHA-256 is
`6fd7fb6ed2c445d6c0262876c196ec2d741b376d82deb9283a450b6315f7d1a7`.
`candidate-reproduction.patch` in the experiment root contains the transport
change and all three regression tests; `candidate-source.patch` is the
tracked-source diff hashed by the run manifests, with the new test file
separately preserved and hashed by `candidate-binaries.sha256`.

The pending-read cancellation test fails on main (two other transport tests
pass), then all three pass on the candidate. The full standalone Tokio vendor
suite passes 72 tests. Workspace tests pass 1,327 with 264 ignored; full
matching Tokio/overlay tests pass 2,805 with 502 ignored. Both strict clippy
gates, fmt, diff, build/check/runtime and shell gates pass. Full commands and
logs remain under the experiment root. No GitHub Actions result is used as
evidence.

The original broad guard is incomplete. Remaining core fio, metadata stress
and direct-write scenes were not completed. The candidate production code
and added tests were reverted; the PR contains the report and evidence only.
