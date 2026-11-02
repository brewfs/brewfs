#!/usr/bin/env bash
set -euo pipefail
cd /home/hxy/brewfs-main
artifact=/home/hxy/perf-seqwrite-20261102
export PATH=/home/hxy/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export RUSTUP_HOME=/home/hxy/.rustup CARGO_HOME=/home/hxy/.cargo
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
date -u +%FT%TZ > "$artifact/validation/ci.resumed.utc"
free -g > "$artifact/validation/memory-before-remainder.txt"
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
# Preserve the initial unfiltered failure separately. No source, test,
# feature, parallelism or assertion is changed for this full rerun.
if [[ ${SEQWRITE_SKIP_OVERLAY:-false} != true ]]; then
    run_gate overlay-full cargo --config "$config" test --workspace --lib --bins --features workspace-overlay,observability
fi
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
run_gate diff-check bash -c 'git diff --check && git diff --cached --check'
