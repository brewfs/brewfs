# Observability acceptance evidence (2026-10-31 UTC)

Generated: 2026-10-31T19:04:30.128408+00:00. Task started 15:05:51 UTC; four-hour deadline 19:05:51 UTC.

**Read thresholds: NOT PASSED.** Full acceptance is not established: comprehensive gating of optional packed/native legacy observers, upstream integration, and revalidation are outstanding. Mixed-I/O and metadata results below must also be considered before acceptance.

Implementation and metric reference: [Prometheus observability](../operations/prometheus.md). Machine-readable raw values and parameters: [evidence JSON](observability-2026-10-31.json).

## Scope and controls

All three binaries use clean baseline `060db0d` or the candidate based on that commit. The baseline was built in a separate clean worktree. Rust/compiler/profile settings are identical: `CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_DEBUG=0 CARGO_PROFILE_RELEASE_CODEGEN_UNITS=256 RUSTFLAGS="-C debuginfo=0"`. Builds have completed before measurement.

Host: Intel(R) Xeon(R) Platinum, 8 logical CPUs; kernel 6.8.0-139-generic; rustc 1.99.0 (b940084d7 2026-09-28); fio 3.39. Compose runner, Redis metadata, LocalFS objects, `upload_before_commit`, no compression; 512 MiB fixed data, 1 GiB userspace read cache, no SSD cache, 2 GiB mount memory budget. Each scene is prefilled with `0x0123456789abcdef` and read completely for SHA256 verification/warmup. Direct I/O, libaio, one job, queue depth 16, 30 seconds per fio scene; 4 KiB randread, 1 MiB sequential read, and 4 KiB randrw (70% reads). Native xfstests metaperf measures create/open/stat/readdir/rename for 5 seconds each; setup and cleanup bring the complete metadata stage to approximately 125 seconds. Enabled runs receive an HTTP scrape every second.

Round order: baseline/default/enabled; default/enabled/baseline; enabled/baseline/default. Container binary SHA256 is verified on every run. Corpus SHA256: `2be3365a2ceeead4cde7793db41967761edf8dd192be60aedeea42399afe41b5`. No round is selected or discarded based on its performance.

LocalFS and warmed data measure instrumentation cost in this profile; these are not S3 or managed-backend performance claims. Upload-before-commit makes successful completion independent of the diagnostic debt counters absent from the default build. Optional strict-drain polling is disabled equally for all modes. Effective wall bandwidth is reported; active-plus-drain bandwidth is unavailable and shown as N/A. This is not a strict-drain acceptance result.

| Binary | SHA256 |
|---|---|
| baseline | `5d7c7228b93a49c03b78cf4283db451e2792bcea1a683a09921ef1e2cbb75d07` |
| default | `e1e253d154072d63435982a18605badf1a6539cc2d6aa2515105baffff4c4b20` |
| enabled | `2752f141dda2e42deb90aedeb1230e81e54b32f5ed5ce1471d68aee354425382` |

## Three raw rounds and medians

Throughput is MiB/s; mixed-I/O throughput is read + write. Delta is `(candidate median / baseline median - 1) * 100`; negative means slower. The default check uses the literal absolute difference ≤1%; enabled overhead must be ≤5%.

| Scene | Mode | Round 1 | Round 2 | Round 3 | Median | Delta vs baseline |
|---|---|---:|---:|---:|---:|---:|
| fio-randread | baseline | 15.267 | 14.978 | 15.607 | 15.267 | +0.00% |
| fio-randread | default | 14.547 | 15.031 | 14.893 | 14.893 | -2.45% |
| fio-randread | enabled | 14.763 | 14.469 | 15.171 | 14.763 | -3.30% |
| fio-seqread | baseline | 1056.198 | 1050.165 | 1061.898 | 1056.198 | +0.00% |
| fio-seqread | default | 1013.633 | 1069.664 | 1035.899 | 1035.899 | -1.92% |
| fio-seqread | enabled | 1045.132 | 1022.033 | 1078.764 | 1045.132 | -1.05% |
| fio-randrw | baseline | 5.639 | 5.858 | 5.842 | 5.842 | +0.00% |
| fio-randrw | default | 5.812 | 5.793 | 5.862 | 5.812 | -0.52% |
| fio-randrw | enabled | 5.587 | 5.449 | 5.813 | 5.587 | -4.36% |

| Read acceptance check | Default absolute difference ≤1% | Enabled overhead vs original baseline ≤5% | Enabled throughput delta vs candidate default |
|---|---|---|---:|
| fio-randread | FAIL | PASS | -0.87% |
| fio-seqread | FAIL | PASS | +0.89% |

**The default build does not meet the required ≤1% difference.** These measurements are not an accepted performance result. The complete fixed matrix is retained without rerunning or selecting rounds to obtain a pass.


## Latency and completion cost

Median values over the same three rounds; all per-round values are retained in the evidence JSON. The repository `compare_artifacts.py` generated six paired comparisons. `N/A` means the runner did not produce that metric; it is not treated as zero.

| Scene | Mode | Read P99 ms | Write P99 ms | Effective wall MiB/s | Active + drain MiB/s |
|---|---|---:|---:|---:|---:|
| fio-randread | baseline | 7.045 | 0.000 | 15.104 | N/A |
| fio-randread | default | 7.242 | 0.000 | 14.413 | N/A |
| fio-randread | enabled | 7.307 | 0.000 | 14.470 | N/A |
| fio-seqread | baseline | 22.151 | 0.000 | 1022.161 | N/A |
| fio-seqread | default | 22.938 | 0.000 | 1013.667 | N/A |
| fio-seqread | enabled | 22.151 | 0.000 | 1022.067 | N/A |
| fio-randrw | baseline | 18.481 | 18.743 | 5.669 | N/A |
| fio-randrw | default | 17.957 | 18.219 | 5.625 | N/A |
| fio-randrw | enabled | 19.005 | 19.268 | 5.588 | N/A |

Enabled mixed-I/O throughput changes by -4.36%; read/write P99 change by +2.84% / +2.80%. These costs remain part of the acceptance review; no mixed-I/O pass is claimed.


## Metadata guard

| Operation | Mode | Round 1 ops/s | Round 2 ops/s | Round 3 ops/s | Median ops/s | Delta |
|---|---|---:|---:|---:|---:|---:|
| create | baseline | 218.8 | 216.0 | 236.7 | 218.8 | +0.00% |
| create | default | 222.6 | 250.0 | 248.0 | 248.0 | +13.37% |
| create | enabled | 221.9 | 246.4 | 229.9 | 229.9 | +5.09% |
| open | baseline | 1543.9 | 1665.4 | 1512.5 | 1543.9 | +0.00% |
| open | default | 1597.8 | 1625.8 | 1760.2 | 1625.8 | +5.30% |
| open | enabled | 1453.1 | 1545.6 | 1701.2 | 1545.6 | +0.11% |
| stat | baseline | 573321.9 | 551979.4 | 557484.3 | 557484.3 | +0.00% |
| stat | default | 584954.1 | 539729.4 | 574235.2 | 574235.2 | +3.00% |
| stat | enabled | 570122.1 | 583160.1 | 578292.1 | 578292.1 | +3.73% |
| readdir | baseline | 8869.8 | 9227.5 | 8986.5 | 8986.5 | +0.00% |
| readdir | default | 9577.9 | 9029.3 | 9263.4 | 9263.4 | +3.08% |
| readdir | enabled | 9289.6 | 9541.5 | 9293.7 | 9293.7 | +3.42% |
| rename | baseline | 158.7 | 166.9 | 168.5 | 166.9 | +0.00% |
| rename | default | 170.6 | 175.2 | 188.0 | 175.2 | +4.94% |
| rename | enabled | 172.2 | 170.9 | 200.5 | 172.2 | +3.14% |

All five metadata operation medians are above the original baseline in both
candidate modes. This metadata result does not change the failed default-read
acceptance result or the mixed-I/O regression recorded above.

## Feature and runtime evidence

The default release milestone was reached at 16:08:53 UTC, 63 minutes after task start, inside the requested 90 minutes. The final default binary was rebuilt after the zero-sized timer refinement and independently checked again.

```text
strings target/release/brewfs | grep -c brewfs_fuse_op_total
0
ss -ltnp | grep 9567  # while the actual default FUSE mount is running
(empty; saved output is 0 bytes)
```

Both default and feature-enabled/runtime-off mounts own no TCP listener. Actual FUSE write/fsync/read and teardown passed. The feature-on mount listens on `127.0.0.1:9567`; custom `127.0.0.1:19567` also passed. `curl` found 2328 `brewfs_` lines, `promtool check metrics` passed, and a real Prometheus server reported `up{job="brewfs"}=1`. `brewfs stats --interval .2 --count 2` passed. All smoke listeners were closed after unmount.

Scrape example from the actual final smoke mount:

```prometheus
brewfs_fuse_op_total{op="read"} 2
brewfs_fuse_op_latency_seconds_count{op="read"} 2
brewfs_fuse_op_errors_total{op="lookup",errno="2"} 2
brewfs_object_get_total{kind="upper"} 1
brewfs_object_put_total 1
brewfs_mount_info{version="0.1.2",profile="flatv1",backend="sqlx"} 1
```

## Correctness and local CI

No new `#[ignore]` attributes and no removed assertions. Existing ignored tests remain reported as ignored, not passed. Histogram TDD first demonstrated a failing test, then checked 100 known samples, bucket boundaries and overflow for each read/write/metadata family. The production-library integration test verifies zero-sized no-op diagnostic types and unevaluated disabled macro arguments.

- `cargo test --workspace --lib --bins` (`gate-test-workspace.log`): `test result: ok. 1280 passed; 0 failed; 262 ignored; 0 measured; 0 filtered out; finished in 78.57s`; `test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s`; `test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s`
- `cargo test -p brewfs --test observability_disabled` (`gate-production-noop.log`): `test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s`
- `cargo test -p brewfs --lib --features workspace-overlay` (`gate-test-overlay-off-retry1.log`): `test result: ok. 2650 passed; 0 failed; 453 ignored; 0 measured; 0 filtered out; finished in 130.71s`
- `cargo test -p brewfs --lib --features workspace-overlay,observability` (`gate-test-overlay-on-retry2.log`): `test result: ok. 2655 passed; 0 failed; 453 ignored; 0 measured; 0 filtered out; finished in 136.44s`

Passed: `cargo fmt --all --check`; workspace check/build; both no-default-feature FUSE runtime checks; `cargo clippy --workspace --all-targets -- -D warnings`; observability + workspace-overlay library/binary Clippy with `-D warnings`; four shell syntax checks; all three required runner report regression tests; Python syntax; `git diff --check`. `rfuse3` is not a workspace member, so its conditional feature matrix does not apply. GitHub Rust CI for implementation commit `b5177aa` also [passed in 6m52s](https://github.com/brewfs/brewfs/actions/runs/41230271138/job/124622838601). The final follow-up commit adds documentation and evidence only.

## Preserved retries and limitations

The host’s existing `/home/hxy/watch-mem.sh` terminated Rust compilers at 16:33:02 and 17:25:36 UTC when available RAM fell below 2 GiB. These were SIGTERM events, not failed assertions. The final test retry uses one Cargo build job and a bounded memory cgroup with swap; the host guard was kept running. `vm.swappiness` was temporarily 60 for compilation and restored to its original 0 before fio. One feature-test compile also found missing `getlk`/`setlk` methods in the new test fixture; both forwarding checks were added without weakening assertions. The initial non-root smoke hit the fresh root inode’s UID-0 ownership; the complete root-run smoke passed. Failed attempt logs are preserved.

The first performance launch (`perf-final`) failed before measurement because Docker Bake rejected a Dockerfile outside its build context; the Dockerfile was copied into the ignored target directory. The next launch (`perf-final-retry1`) revealed that the outer runner did not forward `PERF_FIO_PREP_BS=1m`, so the prefill used 4 KiB. The baseline sequential-read scene and subsequent default run were interrupted. This entire attempt was excluded for configuration mismatch and incomplete scenes, without selecting by throughput. The forwarding was fixed and the helper now rejects fio errors, incomplete runtimes, or missing metadata operations. Its logs/artifacts are retained. The complete nine-run `perf-final-retry2` matrix is the sole performance evidence used in this report.

Main advanced during the task to `2783b1b` through #392/#393. The matched experiment stays on `060db0d`; merging it requires resolving conflicts in `docker/compose-xfstests/run_perf_in_container.sh`, `src/fuse/mount.rs`, `src/meta/client/mod.rs`, `src/vfs/io/writer.rs`, and `src/vfs/stats.rs`, then re-running correctness and matched performance gates. No upstream-integration claim is made.

The normal VFS is the complete source for cache/debt metrics. Experimental EntityFuse contributes FUSE handlers, but its direct backend calls bypass ObjectClient counters. Object counts are API attempts, not hidden SDK retry counts. Optional SIGUSR1 CPU profiling is outside this P0/P1 change. Legacy optional packed/native read-observer code has not been comprehensively reworked; the zero-symbol proof applies to the ordinary default release binary tested here.

## Artifact locations

- Build, test, strings, smoke and comparisons: `/tmp/brewfs-observability-gated`.
- Compose artifacts (retained; no accepted baseline was deleted):

  - Round 1 baseline: `perf-run-1793471237-3795`.
  - Round 1 default: `perf-run-1793471482-8873`.
  - Round 1 enabled: `perf-run-1793471742-22039`.
  - Round 2 baseline: `perf-run-1793472481-917`.
  - Round 2 default: `perf-run-1793471982-12399`.
  - Round 2 enabled: `perf-run-1793472226-30613`.
  - Round 3 baseline: `perf-run-1793472965-27121`.
  - Round 3 default: `perf-run-1793473215-18293`.
  - Round 3 enabled: `perf-run-1793472719-3024`.

All task-owned compose services and volumes are removed after the matrix. The existing README performance comparison tables are unchanged.
