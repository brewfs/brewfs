# Completed validation commands

Only completed commands are listed. See README.md for pending gates and scope.

Full output, exact revisions, environments and source patches are in
`validation.tar.gz`. Ignored tests did not run.

## 222-before

Exit: **101**. Worktree: `/mnt/brewfs-data/src/brewfs-review-baseline`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-performance/target GIT_OPTIONAL_LOCKS=0 cargo test -p brewfs --features workspace-overlay --test workspace_session_drop -- --test-threads=1 --nocapture
```

```text
test result: FAILED. 1 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.94s
```

## 222-after-1

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-lease-drop-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-lease-drop-review/target CARGO_PROFILE_DEV_CODEGEN_UNITS=16 CARGO_PROFILE_TEST_CODEGEN_UNITS=16 GIT_OPTIONAL_LOCKS=0 REVIEW_EXCLUSIVE_HOST=1 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo test -p brewfs --features workspace-overlay --test workspace_session_drop -- --test-threads=1 --nocapture
```

```text
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.95s
```

## 222-after-2

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-lease-drop-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-lease-drop-review/target CARGO_PROFILE_DEV_CODEGEN_UNITS=16 CARGO_PROFILE_TEST_CODEGEN_UNITS=16 GIT_OPTIONAL_LOCKS=0 REVIEW_EXCLUSIVE_HOST=1 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo test -p brewfs --features workspace-overlay --test workspace_session_drop -- --test-threads=1 --nocapture
```

```text
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.95s
```

## 222-after-3

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-lease-drop-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-lease-drop-review/target CARGO_PROFILE_DEV_CODEGEN_UNITS=16 CARGO_PROFILE_TEST_CODEGEN_UNITS=16 GIT_OPTIONAL_LOCKS=0 REVIEW_EXCLUSIVE_HOST=1 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo test -p brewfs --features workspace-overlay --test workspace_session_drop -- --test-threads=1 --nocapture
```

```text
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.95s
```

## 222-fmt-publication

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-lease-drop-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_PROFILE_DEV_CODEGEN_UNITS=16 CARGO_PROFILE_TEST_CODEGEN_UNITS=16 GIT_OPTIONAL_LOCKS=0 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo fmt --all --check
```

## 222-focused-lifecycle

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-lease-lifecycle-harness`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-lease-drop-review/target CARGO_PROFILE_DEV_CODEGEN_UNITS=16 CARGO_PROFILE_TEST_CODEGEN_UNITS=16 GIT_OPTIONAL_LOCKS=0 REVIEW_EXCLUSIVE_HOST=1 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo test -p brewfs --features workspace-overlay --lib workspace_overlay::lifecycle:: -- --test-threads=1
```

```text
test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.03s
```

## 222-fmt

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-lease-drop-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-lease-drop-review/target CARGO_PROFILE_DEV_CODEGEN_UNITS=16 CARGO_PROFILE_TEST_CODEGEN_UNITS=16 GIT_OPTIONAL_LOCKS=0 REVIEW_EXCLUSIVE_HOST=1 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo fmt --all --check
```

## 222-check-overlay

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-lease-drop-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-lease-drop-review/target CARGO_PROFILE_DEV_CODEGEN_UNITS=16 CARGO_PROFILE_TEST_CODEGEN_UNITS=16 GIT_OPTIONAL_LOCKS=0 REVIEW_EXCLUSIVE_HOST=1 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo check --workspace --features workspace-overlay
```

## 222-clippy-overlay

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-lease-drop-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-lease-drop-review/target CARGO_PROFILE_DEV_CODEGEN_UNITS=16 CARGO_PROFILE_TEST_CODEGEN_UNITS=16 GIT_OPTIONAL_LOCKS=0 REVIEW_EXCLUSIVE_HOST=1 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo clippy --workspace --all-targets --features workspace-overlay -- -D warnings
```

## 222-overlay-resolver

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-lease-drop-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-lease-drop-review/target CARGO_PROFILE_DEV_CODEGEN_UNITS=16 CARGO_PROFILE_TEST_CODEGEN_UNITS=16 GIT_OPTIONAL_LOCKS=0 REVIEW_EXCLUSIVE_HOST=1 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo test -p brewfs --features workspace-overlay --test workspace_overlay_resolver -- --test-threads=1
```

```text
test result: ok. 14 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s
```

## 222-workspace-tests

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-lease-drop-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-lease-drop-review/target CARGO_PROFILE_DEV_CODEGEN_UNITS=16 CARGO_PROFILE_TEST_CODEGEN_UNITS=16 GIT_OPTIONAL_LOCKS=0 REVIEW_EXCLUSIVE_HOST=1 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo test --workspace --lib --bins -- --test-threads=1
```

```text
test result: ok. 1196 passed; 0 failed; 225 ignored; 0 measured; 0 filtered out; finished in 140.15s
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

## 222-check-workspace

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-lease-drop-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-lease-drop-review/target CARGO_PROFILE_DEV_CODEGEN_UNITS=16 CARGO_PROFILE_TEST_CODEGEN_UNITS=16 GIT_OPTIONAL_LOCKS=0 REVIEW_EXCLUSIVE_HOST=1 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo check --workspace
```

## 222-build-workspace

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-lease-drop-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-lease-drop-review/target CARGO_PROFILE_DEV_CODEGEN_UNITS=16 CARGO_PROFILE_TEST_CODEGEN_UNITS=16 GIT_OPTIONAL_LOCKS=0 REVIEW_EXCLUSIVE_HOST=1 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo build --workspace
```

## 222-check-tokio

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-lease-drop-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-lease-drop-review/target CARGO_PROFILE_DEV_CODEGEN_UNITS=16 CARGO_PROFILE_TEST_CODEGEN_UNITS=16 GIT_OPTIONAL_LOCKS=0 REVIEW_EXCLUSIVE_HOST=1 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo check -p brewfs --no-default-features --features fuse-tokio-runtime
```

## 222-check-uring

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-lease-drop-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-lease-drop-review/target CARGO_PROFILE_DEV_CODEGEN_UNITS=16 CARGO_PROFILE_TEST_CODEGEN_UNITS=16 GIT_OPTIONAL_LOCKS=0 REVIEW_EXCLUSIVE_HOST=1 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo check -p brewfs --no-default-features --features fuse-io-uring-runtime
```

## 222-clippy-default

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-lease-drop-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-lease-drop-review/target CARGO_PROFILE_DEV_CODEGEN_UNITS=16 CARGO_PROFILE_TEST_CODEGEN_UNITS=16 GIT_OPTIONAL_LOCKS=0 REVIEW_EXCLUSIVE_HOST=1 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo clippy --workspace --all-targets -- -D warnings
```

## 222-syntax-run_perf_in_container

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-lease-drop-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-lease-drop-review/target CARGO_PROFILE_DEV_CODEGEN_UNITS=16 CARGO_PROFILE_TEST_CODEGEN_UNITS=16 GIT_OPTIONAL_LOCKS=0 REVIEW_EXCLUSIVE_HOST=1 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin bash -n docker/compose-xfstests/run_perf_in_container.sh
```

## 222-syntax-run_redis_perf

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-lease-drop-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-lease-drop-review/target CARGO_PROFILE_DEV_CODEGEN_UNITS=16 CARGO_PROFILE_TEST_CODEGEN_UNITS=16 GIT_OPTIONAL_LOCKS=0 REVIEW_EXCLUSIVE_HOST=1 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin bash -n docker/compose-xfstests/run_redis_perf.sh
```

## 222-syntax-run_juicefs_perf_in_container

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-lease-drop-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-lease-drop-review/target CARGO_PROFILE_DEV_CODEGEN_UNITS=16 CARGO_PROFILE_TEST_CODEGEN_UNITS=16 GIT_OPTIONAL_LOCKS=0 REVIEW_EXCLUSIVE_HOST=1 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin bash -n docker/compose-xfstests/run_juicefs_perf_in_container.sh
```

## 222-syntax-run_juicefs_perf

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-lease-drop-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-lease-drop-review/target CARGO_PROFILE_DEV_CODEGEN_UNITS=16 CARGO_PROFILE_TEST_CODEGEN_UNITS=16 GIT_OPTIONAL_LOCKS=0 REVIEW_EXCLUSIVE_HOST=1 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin bash -n docker/compose-xfstests/run_juicefs_perf.sh
```

## 222-test_perf_report_delta

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-lease-drop-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-lease-drop-review/target CARGO_PROFILE_DEV_CODEGEN_UNITS=16 CARGO_PROFILE_TEST_CODEGEN_UNITS=16 GIT_OPTIONAL_LOCKS=0 REVIEW_EXCLUSIVE_HOST=1 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin bash docker/compose-xfstests/test_perf_report_delta.sh
```

## 222-test_juicefs_direct_matrix

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-lease-drop-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-lease-drop-review/target CARGO_PROFILE_DEV_CODEGEN_UNITS=16 CARGO_PROFILE_TEST_CODEGEN_UNITS=16 GIT_OPTIONAL_LOCKS=0 REVIEW_EXCLUSIVE_HOST=1 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin bash docker/compose-xfstests/test_juicefs_direct_matrix.sh
```

## 222-test_juicefs_perf_report

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-lease-drop-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-lease-drop-review/target CARGO_PROFILE_DEV_CODEGEN_UNITS=16 CARGO_PROFILE_TEST_CODEGEN_UNITS=16 GIT_OPTIONAL_LOCKS=0 REVIEW_EXCLUSIVE_HOST=1 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin bash docker/compose-xfstests/test_juicefs_perf_report.sh
```

## 222-diff-check

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-lease-drop-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-lease-drop-review/target CARGO_PROFILE_DEV_CODEGEN_UNITS=16 CARGO_PROFILE_TEST_CODEGEN_UNITS=16 GIT_OPTIONAL_LOCKS=0 REVIEW_EXCLUSIVE_HOST=1 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin git diff --check
```
