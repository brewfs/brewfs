#!/usr/bin/env bash
set -euo pipefail
cd /home/hxy/brewfs-main
artifact=/home/hxy/perf-seqwrite-20261102
export CARGO_TARGET_DIR=/home/hxy/brewfs-pkv-target/ci
export CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
export RUSTFLAGS='-C debuginfo=0' RUST_TEST_THREADS=2
config="$artifact/harness/cargo-ci.toml"
finish() {
    local status=$?
    free -g > "$artifact/validation/memory-after.txt"
    printf '%s\n' "$status" > "$artifact/validation/ci.status"
    date -u +%FT%TZ > "$artifact/validation/ci.finished"
}
trap finish EXIT
free -g > "$artifact/validation/memory-before.txt"
python3 "$artifact/harness/source_manifest.py" "$artifact/validation/ci.sources.json"
run_gate() {
    local name=$1
    shift
    date -u +%FT%TZ > "$artifact/validation/$name.started"
    printf '%q ' "$@" > "$artifact/validation/$name.command"
    local status=0
    "$@" > "$artifact/validation/$name.log" 2>&1 || status=$?
    printf '%s\n' "$status" > "$artifact/validation/$name.status"
    date -u +%FT%TZ > "$artifact/validation/$name.finished"
    return "$status"
}
run_gate fmt cargo fmt --all --check
for script in run_perf_in_container run_redis_perf run_juicefs_perf_in_container run_juicefs_perf; do
    run_gate "syntax-$script" bash -n "docker/compose-xfstests/$script.sh"
done
run_gate perf-report-delta bash docker/compose-xfstests/test_perf_report_delta.sh
run_gate juicefs-direct-matrix bash docker/compose-xfstests/test_juicefs_direct_matrix.sh
run_gate juicefs-perf-report bash docker/compose-xfstests/test_juicefs_perf_report.sh
# The workspace test is a hard gate before performance acceptance.
run_gate test-workspace cargo --config "$config" test --workspace --lib --bins
run_gate clippy-all-targets cargo --config "$config" clippy --workspace --all-targets -- -D warnings
run_gate clippy-overlay cargo --config "$config" clippy --workspace --all-targets --features workspace-overlay,observability -- -D warnings
if [[ ${SEQWRITE_CI_PAUSE_AFTER_WORKSPACE:-false} == true ]]; then
    date -u +%FT%TZ > "$artifact/validation/workspace-gate-passed"
    # Serialize release compilation and all measurements before the remaining
    # CI jobs. Keep this CI process pending rather than marking it complete.
    while [[ ! -f "$artifact/validation/continue-ci" ]]; do sleep 5; done
fi
run_gate overlay-full cargo --config "$config" test --workspace --lib --bins --features workspace-overlay,observability
run_gate check-workspace cargo --config "$config" check --workspace
run_gate build-workspace cargo --config "$config" build --workspace
cargo metadata --no-deps --format-version 1 > "$artifact/validation/cargo-metadata.json"
if python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); m=set(d["workspace_members"]); sys.exit(not any(p["name"]=="rfuse3" and p["id"] in m for p in d["packages"]))' "$artifact/validation/cargo-metadata.json"; then
    for runtime in tokio-runtime io-uring-runtime async-io-runtime; do
        run_gate "rfuse3-$runtime" cargo check -p rfuse3 --no-default-features --features "$runtime"
    done
else
    printf 'not a workspace member\n' > "$artifact/validation/rfuse3.not-applicable"
fi
run_gate check-tokio cargo --config "$config" check -p brewfs --no-default-features --features fuse-tokio-runtime
run_gate check-io-uring cargo --config "$config" check -p brewfs --no-default-features --features fuse-io-uring-runtime
run_gate clippy-workspace cargo --config "$config" clippy --workspace
run_gate diff-check git diff --check
