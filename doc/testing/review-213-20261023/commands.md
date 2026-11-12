# Recorded validation commands

Broader gates remain incomplete; see README.md. This is an evidence export,
not a declaration that every gate passed. Partial compiler/OOM logs without
a completion record are also included in the archive.

## 213-237-before

Exit: **101**. Worktree: `/mnt/brewfs-data/src/brewfs-review-baseline`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-performance/target cargo test -p brewfs --features workspace-overlay --test entity_fragment_reads -- --test-threads=1 --nocapture
```

```text
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.23s
```

## 213-after-1

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-entity-read-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-performance/target GIT_OPTIONAL_LOCKS=0 cargo test -p brewfs --features workspace-overlay --test entity_fragment_reads -- --test-threads=1 --nocapture
```

```text
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.14s
```

## 213-after-2

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-entity-read-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-performance/target GIT_OPTIONAL_LOCKS=0 cargo test -p brewfs --features workspace-overlay --test entity_fragment_reads -- --test-threads=1 --nocapture
```

```text
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.17s
```

## 213-after-3

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-entity-read-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-performance/target GIT_OPTIONAL_LOCKS=0 cargo test -p brewfs --features workspace-overlay --test entity_fragment_reads -- --test-threads=1 --nocapture
```

```text
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.18s
```

## 213-fmt

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-entity-read-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-performance/target GIT_OPTIONAL_LOCKS=0 cargo fmt --all --check
```

## 213-syntax-run_perf_in_container

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-entity-read-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-performance/target GIT_OPTIONAL_LOCKS=0 bash -n docker/compose-xfstests/run_perf_in_container.sh
```

## 213-syntax-run_redis_perf

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-entity-read-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-performance/target GIT_OPTIONAL_LOCKS=0 bash -n docker/compose-xfstests/run_redis_perf.sh
```

## 213-syntax-run_juicefs_perf_in_container

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-entity-read-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-performance/target GIT_OPTIONAL_LOCKS=0 bash -n docker/compose-xfstests/run_juicefs_perf_in_container.sh
```

## 213-syntax-run_juicefs_perf

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-entity-read-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-performance/target GIT_OPTIONAL_LOCKS=0 bash -n docker/compose-xfstests/run_juicefs_perf.sh
```

## 213-test_perf_report_delta

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-entity-read-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-performance/target GIT_OPTIONAL_LOCKS=0 bash docker/compose-xfstests/test_perf_report_delta.sh
```

## 213-test_juicefs_direct_matrix

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-entity-read-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-performance/target GIT_OPTIONAL_LOCKS=0 bash docker/compose-xfstests/test_juicefs_direct_matrix.sh
```

## 213-test_juicefs_perf_report

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-entity-read-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-performance/target GIT_OPTIONAL_LOCKS=0 bash docker/compose-xfstests/test_juicefs_perf_report.sh
```

## 213-check-overlay

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-entity-read-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-performance/target GIT_OPTIONAL_LOCKS=0 cargo check --workspace --features workspace-overlay
```

## 213-clippy-overlay

Exit: **0**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-entity-read-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-performance/target GIT_OPTIONAL_LOCKS=0 cargo clippy --workspace --all-targets --features workspace-overlay -- -D warnings
```

## 213-overlay-unit-suite

Exit: **101**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-entity-read-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-performance/target GIT_OPTIONAL_LOCKS=0 RUSTC_WRAPPER=/mnt/brewfs-data/src/brewfs-review-evidence-20261023/rustc-low-memory.py cargo --config profile.test.package.brewfs.codegen-units=256 test --workspace --lib --bins --features workspace-overlay -- --test-threads=1
```

## 213-overlay-unit-suite-serial

Exit: **-2**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-entity-read-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-performance/target GIT_OPTIONAL_LOCKS=0 RUSTC_WRAPPER=/mnt/brewfs-data/src/brewfs-review-evidence-20261023/rustc-low-memory.py PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo --config profile.test.package.brewfs.codegen-units=256 test --workspace --lib --bins --features workspace-overlay -- --test-threads=1
```

## 213-overlay-resolver

Exit: **-2**. Worktree: `/mnt/brewfs-data/src/brewfs-fix-entity-read-review`.

```bash
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TARGET_DIR=/mnt/brewfs-data/src/brewfs-fix-performance/target GIT_OPTIONAL_LOCKS=0 PATH=/home/hxy/.local/bin:/home/hxy/.local/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin cargo test -p brewfs --features workspace-overlay --test workspace_overlay_resolver -- --test-threads=1
```
