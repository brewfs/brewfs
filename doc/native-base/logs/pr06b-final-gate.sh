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
scratch_dir=$(mktemp -d /tmp/brewfs-pr06b-gate.XXXXXX) || exit 1
mkdir -p "$scratch_dir/docker" || exit 1
cp -r docker/compose-xfstests "$scratch_dir/docker/" || exit 1
find "$scratch_dir" -name '*.sh' -exec sed -i 's/\r$//' {} + || exit 1
for name in run_perf_in_container run_redis_perf run_juicefs_perf_in_container run_juicefs_perf; do
    run bash -n "$scratch_dir/docker/compose-xfstests/$name.sh" || exit $?
done
for name in test_perf_report_delta test_juicefs_direct_matrix test_juicefs_perf_report; do
    run bash -lc "cd '$scratch_dir/docker/compose-xfstests' && bash '$name.sh'" || exit $?
done
rm -rf -- "$scratch_dir"
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
