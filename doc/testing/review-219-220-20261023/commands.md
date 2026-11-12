# Completed validation commands

Only completed commands are listed. See README.md for pending gates and scope.

Full output, exact revisions, environments and source patches are in
`validation.tar.gz`. Ignored tests did not run.

## 219-220-242-before

Exit: **101**. Worktree: `/mnt/brewfs-data/src/brewfs-review-baseline`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-performance/target cargo test -p brewfs --features workspace-overlay --test compactor_test -- --test-threads=1 --nocapture
```

```text
test result: FAILED. 8 passed; 3 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.30s
```

## 219-220-after-1

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-compaction-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-compaction-review/target GIT_OPTIONAL_LOCKS=0 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo test -p brewfs --features workspace-overlay --test compactor_test -- --test-threads=1 --nocapture
```

```text
test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.44s
```

## 219-220-after-2

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-compaction-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-compaction-review/target GIT_OPTIONAL_LOCKS=0 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo test -p brewfs --features workspace-overlay --test compactor_test -- --test-threads=1 --nocapture
```

```text
test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.62s
```

## 219-220-after-3

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-compaction-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-compaction-review/target GIT_OPTIONAL_LOCKS=0 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo test -p brewfs --features workspace-overlay --test compactor_test -- --test-threads=1 --nocapture
```

```text
test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.57s
```

## 219-220-fmt

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-compaction-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-compaction-review/target GIT_OPTIONAL_LOCKS=0 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo fmt --all --check
```

## 219-220-check-overlay

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-compaction-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-compaction-review/target GIT_OPTIONAL_LOCKS=0 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo check --workspace --features workspace-overlay
```

## 219-220-clippy-overlay

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-compaction-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-compaction-review/target GIT_OPTIONAL_LOCKS=0 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo clippy --workspace --all-targets --features workspace-overlay -- -D warnings
```

## 219-220-focused-default

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-compaction-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-compaction-review/target GIT_OPTIONAL_LOCKS=0 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo test --workspace --lib --bins chunk::slice:: -- --test-threads=1
```

```text
test result: ok. 19 passed; 0 failed; 0 ignored; 0 measured; 1402 filtered out; finished in 0.01s
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

## 219-220-workspace-tests

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-compaction-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-compaction-review/target GIT_OPTIONAL_LOCKS=0 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo test --workspace --lib --bins -- --test-threads=1
```

```text
test result: ok. 1196 passed; 0 failed; 225 ignored; 0 measured; 0 filtered out; finished in 158.28s
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

## 219-220-overlay-resolver

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-compaction-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-compaction-review/target GIT_OPTIONAL_LOCKS=0 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo test -p brewfs --features workspace-overlay --test workspace_overlay_resolver -- --test-threads=1
```

```text
test result: ok. 14 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s
```

## 219-220-check-workspace

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-compaction-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-compaction-review/target GIT_OPTIONAL_LOCKS=0 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo check --workspace
```

## 219-220-build-workspace

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-compaction-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-compaction-review/target GIT_OPTIONAL_LOCKS=0 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo build --workspace
```

## 219-220-check-tokio

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-compaction-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-compaction-review/target GIT_OPTIONAL_LOCKS=0 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo check -p brewfs --no-default-features --features fuse-tokio-runtime
```

## 219-220-check-uring

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-compaction-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-compaction-review/target GIT_OPTIONAL_LOCKS=0 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo check -p brewfs --no-default-features --features fuse-io-uring-runtime
```

## 219-220-clippy-default

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-compaction-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-compaction-review/target GIT_OPTIONAL_LOCKS=0 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo clippy --workspace --all-targets -- -D warnings
```

## 219-220-syntax-run_perf_in_container

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-compaction-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-compaction-review/target GIT_OPTIONAL_LOCKS=0 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin bash -n docker/compose-xfstests/run_perf_in_container.sh
```

## 219-220-syntax-run_redis_perf

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-compaction-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-compaction-review/target GIT_OPTIONAL_LOCKS=0 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin bash -n docker/compose-xfstests/run_redis_perf.sh
```

## 219-220-syntax-run_juicefs_perf_in_container

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-compaction-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-compaction-review/target GIT_OPTIONAL_LOCKS=0 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin bash -n docker/compose-xfstests/run_juicefs_perf_in_container.sh
```

## 219-220-syntax-run_juicefs_perf

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-compaction-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-compaction-review/target GIT_OPTIONAL_LOCKS=0 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin bash -n docker/compose-xfstests/run_juicefs_perf.sh
```

## 219-220-test_perf_report_delta

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-compaction-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-compaction-review/target GIT_OPTIONAL_LOCKS=0 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin bash docker/compose-xfstests/test_perf_report_delta.sh
```

## 219-220-test_juicefs_direct_matrix

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-compaction-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-compaction-review/target GIT_OPTIONAL_LOCKS=0 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin bash docker/compose-xfstests/test_juicefs_direct_matrix.sh
```

## 219-220-test_juicefs_perf_report

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-compaction-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-compaction-review/target GIT_OPTIONAL_LOCKS=0 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin bash docker/compose-xfstests/test_juicefs_perf_report.sh
```

## 219-220-diff-check

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-compaction-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-compaction-review/target GIT_OPTIONAL_LOCKS=0 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin git diff --check
```
