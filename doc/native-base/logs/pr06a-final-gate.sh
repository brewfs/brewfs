#!/usr/bin/env bash
# PR06A final local CI gate. Every step records its own exit code and the
# script fails if any required step fails.
set -u

cd "$(dirname "$0")" || exit 1
cd ../../.. || exit 1
echo "gate root: $(pwd)"
echo "commit: ${GATE_COMMIT:-WORKTREE-NOT-YET-COMMITTED}"

fail=0
run_step() {
    label="$1"
    shift
    echo "=== GATE: $label ==="
    "$@"
    rc=$?
    echo "$label:exit=$rc"
    if [ "$rc" -ne 0 ]; then
        fail=1
    fi
}

run_step "fmt" cargo fmt --all --check

echo "=== GATE: bash-script gates (LF-normalized scratch copy) ==="
scratch_dir=$(mktemp -d /tmp/brewfs-pr06a-gate.XXXXXX) || exit 1
mkdir -p "$scratch_dir/docker" || exit 1
cp -r docker/compose-xfstests "$scratch_dir/docker/" || exit 1
find "$scratch_dir" -name '*.sh' -exec sed -i 's/\r$//' {} + || exit 1
for name in run_perf_in_container run_redis_perf run_juicefs_perf_in_container run_juicefs_perf; do
    run_step "bashn:$name" bash -n "$scratch_dir/docker/compose-xfstests/$name.sh"
done
for name in test_perf_report_delta test_juicefs_direct_matrix test_juicefs_perf_report; do
    run_step "test:$name" bash -lc "cd '$scratch_dir/docker/compose-xfstests' && bash '$name.sh'"
done
rm -rf -- "$scratch_dir"

run_step "check" env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo check --workspace
run_step "build" env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo build --workspace

echo "=== GATE: rfuse3 feature checks (if workspace member) ==="
metadata=$(cargo metadata --no-deps --format-version 1)
if python3 -c 'import json, sys; data = json.loads(sys.stdin.read()); members = set(data["workspace_members"]); sys.exit(not any(pkg["name"] == "rfuse3" and pkg["id"] in members for pkg in data["packages"]))' <<< "$metadata"; then
    run_step "rfuse3:tokio" cargo check -p rfuse3 --no-default-features --features tokio-runtime
    run_step "rfuse3:io-uring" cargo check -p rfuse3 --no-default-features --features io-uring-runtime
    run_step "rfuse3:async-io" cargo check -p rfuse3 --no-default-features --features async-io-runtime
fi

run_step "brewfs:fuse-tokio" env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo check -p brewfs --no-default-features --features fuse-tokio-runtime
run_step "brewfs:fuse-io-uring" env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo check -p brewfs --no-default-features --features fuse-io-uring-runtime
run_step "test" env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins -j 4 -- --test-threads 4
run_step "clippy" env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo clippy --workspace

if [ "$fail" -ne 0 ]; then
    echo "=== GATE: FAILED ==="
    exit 1
fi
echo "=== GATE: ALL PASSED ==="
