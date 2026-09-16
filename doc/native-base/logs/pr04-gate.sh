#!/usr/bin/env bash
# PR04 local CI gate (AGENTS.md "Local CI Gate For Accepted Code").
# Runs every Rust-job command except `git diff --check`, which must run on
# the Windows-side git. Exits non-zero on the first failing step.
set -u

cd "$(dirname "$0")/.." || exit 1

step() { echo "=== GATE: $* ==="; }

step "bash-script gates (LF-normalized scratch copy of compose-xfstests)"
G=/tmp/brewfs-gate-pr04
rm -rf "$G"
mkdir -p "$G/docker" || exit 1
cp -r docker/compose-xfstests "$G/docker/" || exit 1
find "$G" -name '*.sh' -exec sed -i 's/\r$//' {} + || exit 1
for f in run_perf_in_container run_redis_perf run_juicefs_perf_in_container run_juicefs_perf; do
    bash -n "$G/docker/compose-xfstests/$f.sh" || exit 1
done
for f in test_perf_report_delta test_juicefs_direct_matrix test_juicefs_perf_report; do
    (cd "$G/docker/compose-xfstests" && bash "$f.sh") || exit 1
done

step "cargo check --workspace"
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo check --workspace || exit 1

step "cargo build --workspace"
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo build --workspace || exit 1

step "rfuse3 feature checks (if workspace member)"
metadata="$(cargo metadata --no-deps --format-version 1)"
if python3 -c 'import json, sys; data = json.loads(sys.stdin.read()); members = set(data["workspace_members"]); sys.exit(not any(pkg["name"] == "rfuse3" and pkg["id"] in members for pkg in data["packages"]))' <<< "$metadata"; then
    cargo check -p rfuse3 --no-default-features --features tokio-runtime || exit 1
    cargo check -p rfuse3 --no-default-features --features io-uring-runtime || exit 1
    cargo check -p rfuse3 --no-default-features --features async-io-runtime || exit 1
fi

step "brewfs fuse runtime feature checks"
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo check -p brewfs --no-default-features --features fuse-tokio-runtime || exit 1
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo check -p brewfs --no-default-features --features fuse-io-uring-runtime || exit 1

step "cargo test --workspace --lib --bins"
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test --workspace --lib --bins || exit 1

step "cargo clippy --workspace"
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo clippy --workspace || exit 1

echo "=== GATE: ALL PASSED ==="
