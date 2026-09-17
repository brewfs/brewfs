#!/usr/bin/env bash
# PR06B local CI gate. Keep this aligned with AGENTS.md and record every
# command in the wrapper log for exact-commit evidence.
set -u
set -o pipefail
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/brewfs-target-vigorous-solomon-7caff5}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0
repo=/mnt/d/code/brewfs/.claude/worktrees/vigorous-solomon-7caff5
cd "$repo" || exit 1

run() {
    echo "+ $*"
    "$@"
    local rc=$?
    echo "exit=$rc"
    return "$rc"
}

run cargo fmt --all --check || exit $?
for script in \
    docker/compose-xfstests/run_perf_in_container.sh \
    docker/compose-xfstests/run_redis_perf.sh \
    docker/compose-xfstests/run_juicefs_perf_in_container.sh \
    docker/compose-xfstests/run_juicefs_perf.sh; do
    run bash -n "$script" || exit $?
done
for script in \
    docker/compose-xfstests/test_perf_report_delta.sh \
    docker/compose-xfstests/test_juicefs_direct_matrix.sh \
    docker/compose-xfstests/test_juicefs_perf_report.sh; do
    run bash "$script" || exit $?
done
run cargo check --workspace || exit $?
run cargo build --workspace || exit $?

metadata=$(cargo metadata --no-deps --format-version 1) || exit $?
if python3 -c 'import json, sys; data=json.loads(sys.stdin.read()); members=set(data["workspace_members"]); sys.exit(not any(pkg["name"] == "rfuse3" and pkg["id"] in members for pkg in data["packages"]))' <<< "$metadata"; then
    run cargo check -p rfuse3 --no-default-features --features tokio-runtime || exit $?
    run cargo check -p rfuse3 --no-default-features --features io-uring-runtime || exit $?
    run cargo check -p rfuse3 --no-default-features --features async-io-runtime || exit $?
fi
run cargo check -p brewfs --no-default-features --features fuse-tokio-runtime || exit $?
run cargo check -p brewfs --no-default-features --features fuse-io-uring-runtime || exit $?
run cargo test --workspace --lib --bins || exit $?
run cargo clippy --workspace || exit $?
run git diff --check || exit $?
