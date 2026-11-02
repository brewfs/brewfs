#!/usr/bin/env bash
set -euo pipefail
cd /home/hxy/brewfs-main
artifact=/home/hxy/perf-seqwrite-20261102
export CARGO_TARGET_DIR=/home/hxy/brewfs-pkv-target
export CARGO_BUILD_JOBS=4 CARGO_INCREMENTAL=0
export RUSTFLAGS='-C debuginfo=0'
export CARGO_PROFILE_RELEASE_DEBUG=0 CARGO_PROFILE_RELEASE_CODEGEN_UNITS=256
finish() {
    local status=$?
    free -g > "$artifact/build/candidate-memory-after.txt"
    printf '%s\n' "$status" > "$artifact/build/candidate.status"
    date -u +%FT%TZ > "$artifact/build/candidate.finished"
}
trap finish EXIT
date -u +%FT%TZ > "$artifact/build/candidate.started"
free -g > "$artifact/build/candidate-memory-before.txt"
python3 "$artifact/harness/source_manifest.py" "$artifact/build/candidate.sources.before.json"
git diff > "$artifact/build/candidate-source.patch"
env | sort | rg '^(CARGO|RUST)' > "$artifact/build/candidate.environment"
cargo build --locked --release --bin brewfs --features workspace-overlay,observability > "$artifact/build/candidate.log" 2>&1
python3 "$artifact/harness/source_manifest.py" "$artifact/build/candidate.sources.after.json"
cmp "$artifact/build/candidate.sources.before.json" "$artifact/build/candidate.sources.after.json"
cp "$CARGO_TARGET_DIR/release/brewfs" "$artifact/bin/brewfs-candidate"
sha256sum "$artifact/bin/brewfs-candidate" > "$artifact/build/candidate.sha256"
