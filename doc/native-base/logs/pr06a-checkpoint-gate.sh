#!/usr/bin/env bash
# PR06A checkpoint gate — the AGENTS.md Rust-side gate re-run at the
# extracted-index-builder commit (e19c990), before the lifecycle work
# starts. Unlike pr04-gate.sh this records a per-step exit code for every
# step (including the bash-script gates), so the log is self-describing.
#
# Run from anywhere: the script resolves the repository root itself.
# Output: doc/native-base/logs/pr06a-checkpoint-gate.log
set -u

cd "$(dirname "$0")" || exit 1
cd ../../.. || exit 1
echo "gate root: $(pwd)"
# The WSL git cannot resolve a Windows worktree path, so the commit is passed
# in by the caller (see .claude/run-pr06a-gate.sh, which reads it from the
# Windows-side git into .claude/gate-commit.txt).
echo "commit: ${GATE_COMMIT:-unknown}"

fail=0

step() { echo "=== GATE: $* ==="; }
record() { echo "$1:exit=$?"; }

step "bash-script gates (LF-normalized scratch copy of compose-xfstests)"
G=/tmp/brewfs-gate-pr06a
rm -rf "$G"
mkdir -p "$G/docker" || exit 1
cp -r docker/compose-xfstests "$G/docker/" || exit 1
find "$G" -name '*.sh' -exec sed -i 's/\r$//' {} + || exit 1
for f in run_perf_in_container run_redis_perf run_juicefs_perf_in_container run_juicefs_perf; do
    bash -n "$G/docker/compose-xfstests/$f.sh"; record "bashn:$f"
done
for f in test_perf_report_delta test_juicefs_direct_matrix test_juicefs_perf_report; do
    (cd "$G/docker/compose-xfstests" && bash "$f.sh"); record "test:$f"
done

step "cargo check --workspace"
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo check --workspace; record "check"

step "cargo build --workspace"
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo build --workspace; record "build"

step "rfuse3 feature checks (if workspace member)"
metadata="$(cargo metadata --no-deps --format-version 1)"
if python3 -c 'import json, sys; data = json.loads(sys.stdin.read()); members = set(data["workspace_members"]); sys.exit(not any(pkg["name"] == "rfuse3" and pkg["id"] in members for pkg in data["packages"]))' <<< "$metadata"; then
    cargo check -p rfuse3 --no-default-features --features tokio-runtime; record "rfuse3:tokio"
    cargo check -p rfuse3 --no-default-features --features io-uring-runtime; record "rfuse3:io-uring"
    cargo check -p rfuse3 --no-default-features --features async-io-runtime; record "rfuse3:async-io"
fi

step "brewfs fuse runtime feature checks"
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo check -p brewfs --no-default-features --features fuse-tokio-runtime; record "brewfs:fuse-tokio"
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo check -p brewfs --no-default-features --features fuse-io-uring-runtime; record "brewfs:fuse-io-uring"

step "cargo test --workspace --lib --bins"
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins -j 4 -- --test-threads 4; record "test"

step "cargo clippy --workspace"
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo clippy --workspace; record "clippy"

echo "=== GATE: ALL PASSED ==="
