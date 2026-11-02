#!/usr/bin/env bash
set -euo pipefail
cd /home/hxy/brewfs-main
artifact=/home/hxy/perf-seqwrite-20261102
export RUST_TEST_THREADS=2
finish() {
    local status=$?
    printf '%s\n' "$status" > "$artifact/validation/overlay-full.status"
    date -u +%FT%TZ > "$artifact/validation/overlay-full.finished"
}
trap finish EXIT
date -u +%FT%TZ > "$artifact/validation/overlay-full.started"
printf '%s\n' 'bash /home/hxy/perf-seqwrite-20261102/harness/overlay-binary-full.sh (all original overlay library and binary tests, no filters)' > "$artifact/validation/overlay-full.command"
test_bins=(
    /home/hxy/brewfs-pkv-target/ci/debug/deps/brewfs-54dbcca473b8e11e
    /home/hxy/brewfs-pkv-target/ci/debug/deps/brewfs-80dc75f4873f1fe2
    /home/hxy/brewfs-pkv-target/ci/debug/deps/brewfs_stats-60f1abe09b875619
)
sha256sum "${test_bins[@]}" > "$artifact/validation/overlay-test-binaries.sha256"
python3 "$artifact/harness/source_manifest.py" "$artifact/validation/overlay-binary.sources.json"
: > "$artifact/validation/overlay-full.log"
for binary in "${test_bins[@]}"; do
    printf '\nRunning %s (unfiltered)\n' "$binary" >> "$artifact/validation/overlay-full.log"
    "$binary" >> "$artifact/validation/overlay-full.log" 2>&1
done
