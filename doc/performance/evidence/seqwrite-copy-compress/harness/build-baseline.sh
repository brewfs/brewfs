#!/usr/bin/env bash
set -euo pipefail
cd /home/hxy/brewfs-seqwrite-baseline
artifact=/home/hxy/perf-seqwrite-20261102
export CARGO_TARGET_DIR=/home/hxy/brewfs-seqwrite-baseline-target
export CARGO_BUILD_JOBS=4 CARGO_INCREMENTAL=0
export RUSTFLAGS='-C debuginfo=0'
export CARGO_PROFILE_RELEASE_DEBUG=0 CARGO_PROFILE_RELEASE_CODEGEN_UNITS=256
finish() {
    local status=$?
    free -g > "$artifact/build/baseline-memory-after.txt"
    printf '%s\n' "$status" > "$artifact/build/baseline.status"
    date -u +%FT%TZ > "$artifact/build/baseline.finished"
}
trap finish EXIT
date -u +%FT%TZ > "$artifact/build/baseline.started"
free -g > "$artifact/build/baseline-memory-before.txt"
git rev-parse HEAD > "$artifact/build/baseline.commit"
env | sort | rg '^(CARGO|RUST)' > "$artifact/build/baseline.environment"
cargo build --locked --release --bin brewfs --features workspace-overlay,observability > "$artifact/build/baseline.log" 2>&1
cp "$CARGO_TARGET_DIR/release/brewfs" "$artifact/bin/brewfs-baseline"
sha256sum "$artifact/bin/brewfs-baseline" > "$artifact/build/baseline.sha256"
