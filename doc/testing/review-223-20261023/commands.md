# Exact validation commands and results

Every Cargo command used `CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0`. Commands ran serially in a 24 GiB memory scope with swap disabled.

| Log | Exact command | Exit | Test results |
| --- | --- | ---: | --- |
| 223-after-1.log | `cargo test -p brewfs --lib chunk::bandwidth::tests::one_mib_ -- --test-threads=1 --nocapture` | 0 | test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 1421 filtered out; finished in 6.07s |
| 223-after-2.log | `cargo test -p brewfs --lib chunk::bandwidth::tests::one_mib_ -- --test-threads=1 --nocapture` | 0 | test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 1421 filtered out; finished in 6.07s |
| 223-after-3.log | `cargo test -p brewfs --lib chunk::bandwidth::tests::one_mib_ -- --test-threads=1 --nocapture` | 0 | test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 1421 filtered out; finished in 6.07s |
| 223-before.log | `cargo test -p brewfs --lib chunk::bandwidth::tests::one_mib_ -- --test-threads=1 --nocapture` | 101 | test result: FAILED. 0 passed; 2 failed; 0 ignored; 0 measured; 1421 filtered out; finished in 0.00s |
| 223-build-workspace.log | `cargo build --workspace` | 0 | Command completed |
| 223-check-overlay.log | `cargo check --workspace --features workspace-overlay` | 0 | Command completed |
| 223-check-tokio.log | `cargo check -p brewfs --no-default-features --features fuse-tokio-runtime` | 0 | Command completed |
| 223-check-uring.log | `cargo check -p brewfs --no-default-features --features fuse-io-uring-runtime` | 0 | Command completed |
| 223-check-workspace.log | `cargo check --workspace` | 0 | Command completed |
| 223-clippy-default.log | `cargo clippy --workspace --all-targets -- -D warnings` | 0 | Command completed |
| 223-clippy-overlay.log | `cargo clippy --workspace --all-targets --features workspace-overlay -- -D warnings` | 0 | Command completed |
| 223-diff-check.log | `git diff --check` | 0 | Command completed |
| 223-fmt.log | `cargo fmt --all --check` | 0 | Command completed |
| 223-focused.log | `cargo test -p brewfs --lib chunk::bandwidth:: -- --test-threads=1` | 0 | test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 1417 filtered out; finished in 6.07s |
| 223-overlay-resolver.log | `cargo test -p brewfs --features workspace-overlay --test workspace_overlay_resolver -- --test-threads=1` | 0 | test result: ok. 14 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s |
| 223-syntax-run_juicefs_perf.log | `bash -n docker/compose-xfstests/run_juicefs_perf.sh` | 0 | Command completed |
| 223-syntax-run_juicefs_perf_in_container.log | `bash -n docker/compose-xfstests/run_juicefs_perf_in_container.sh` | 0 | Command completed |
| 223-syntax-run_perf_in_container.log | `bash -n docker/compose-xfstests/run_perf_in_container.sh` | 0 | Command completed |
| 223-syntax-run_redis_perf.log | `bash -n docker/compose-xfstests/run_redis_perf.sh` | 0 | Command completed |
| 223-test_juicefs_direct_matrix.log | `bash docker/compose-xfstests/test_juicefs_direct_matrix.sh` | 0 | Command completed |
| 223-test_juicefs_perf_report.log | `bash docker/compose-xfstests/test_juicefs_perf_report.sh` | 0 | Command completed |
| 223-test_perf_report_delta.log | `bash docker/compose-xfstests/test_perf_report_delta.sh` | 0 | Command completed |
| 223-workspace-tests.log | `cargo test --workspace --lib --bins -- --test-threads=1` | 0 | test result: ok. 1198 passed; 0 failed; 225 ignored; 0 measured; 0 filtered out; finished in 148.43s; test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s; test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s |

The archive preserves complete output, including dependency warnings, timestamps, exact environment, revision, and source-patch hashes. It includes the test-only baseline patch and fixed source patch. The baseline failure was expected; every after/gate command exited zero.

Archive SHA-256: `8f8c4a0c9ceaffaa3a5b8180c619747695a687f5a32a02ad6de1fdaee1803db1`.
