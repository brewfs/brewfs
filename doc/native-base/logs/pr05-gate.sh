#!/usr/bin/env bash
# PR05 local CI gate (AGENTS.md "Local CI Gate For Accepted Code").
# Runs every Rust-job command except `git diff --check`, which must run on
# the Windows-side git (WSL git cannot resolve this worktree's gitdir
# path). The compose-xfstests shell scripts are copied to an LF-normalized
# scratch tree first: the checked-in copies carry the Windows CRLF endings
# of this checkout, and `bash -n` / the script tests reject those. Exits
# non-zero if any step fails; each step's exit code is echoed.
set -u

cd "$(dirname "$0")" || exit 1
# doc/native-base/logs -> repository root
cd ../../.. || exit 1
echo "gate root: $(pwd)"
export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0
overall=0

run() {
    local name="$1"; shift
    echo "=== $name ==="
    "$@"
    local rc=$?
    echo "=== $name exit $rc ==="
    if [ $rc -ne 0 ]; then overall=1; fi
}

run "fmt" cargo fmt --all --check

echo "=== bash-script gates (LF-normalized scratch copy) ==="
G=/tmp/brewfs-gate-pr05
rm -rf "$G"
mkdir -p "$G/docker" || exit 1
cp -r docker/compose-xfstests "$G/docker/" || exit 1
find "$G" -name '*.sh' -exec sed -i 's/\r$//' {} + || exit 1
for f in run_perf_in_container run_redis_perf run_juicefs_perf_in_container run_juicefs_perf; do
    run "bash-n-$f" bash -n "$G/docker/compose-xfstests/$f.sh"
done
for f in test_perf_report_delta test_juicefs_direct_matrix test_juicefs_perf_report; do
    run "script-test-$f" bash -c "cd '$G/docker/compose-xfstests' && bash $f.sh"
done

run "focused-test" cargo test --lib -j 4 native_base::ingest -- --test-threads 4
run "check-workspace" cargo check --workspace
run "build-workspace" cargo build --workspace

metadata="$(cargo metadata --no-deps --format-version 1)"
if python3 -c 'import json, sys; data = json.loads(sys.stdin.read()); members = set(data["workspace_members"]); sys.exit(not any(pkg["name"] == "rfuse3" and pkg["id"] in members for pkg in data["packages"]))' <<< "$metadata"; then
    run "check-rfuse3-tokio" cargo check -p rfuse3 --no-default-features --features tokio-runtime
    run "check-rfuse3-io-uring" cargo check -p rfuse3 --no-default-features --features io-uring-runtime
    run "check-rfuse3-async-io" cargo check -p rfuse3 --no-default-features --features async-io-runtime
fi

run "check-brewfs-tokio" cargo check -p brewfs --no-default-features --features fuse-tokio-runtime
run "check-brewfs-io-uring" cargo check -p brewfs --no-default-features --features fuse-io-uring-runtime
run "test-workspace" cargo test --workspace --lib --bins -j 4 -- --test-threads 4
run "clippy" cargo clippy --workspace

echo "OVERALL=$overall"
exit $overall
