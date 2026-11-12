#!/usr/bin/env bash
# Local correctness gate, including code behind workspace-overlay features.
set -euo pipefail
source_root=$(cd -- "$(dirname -- "$0")/../.." && pwd)
output=${1:?usage: check_hot_overlay.sh ABSOLUTE_NEW_OUTPUT_DIRECTORY}
[[ $output == /* && ! -e $output ]] || { echo 'Output must be new and absolute' >&2; exit 2; }
mkdir -p -- "$output"
cd -- "$source_root"
export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-2}
features=fuse-io-uring-runtime,workspace-overlay,native-packed-base,frozen-base-metadata,rustfs-ec-block-size-hint,observability
exec > >(tee "$output/check.log") 2>&1
date -u --iso-8601=seconds
git rev-parse HEAD > "$output/source-commit.txt"
git diff --binary HEAD > "$output/source.patch"
rustc -Vv
cargo -V
set -x
cargo fmt --all --check
for f in run_perf_in_container run_redis_perf run_juicefs_perf_in_container run_juicefs_perf; do
    bash -n "docker/compose-xfstests/$f.sh"
done
for f in test_perf_report_delta test_juicefs_direct_matrix test_juicefs_perf_report; do
    bash "docker/compose-xfstests/$f.sh"
done
cargo check --locked --workspace
cargo build --locked --workspace
metadata=$(cargo metadata --no-deps --format-version 1)
if python3 -c 'import json,sys; d=json.load(sys.stdin); m=set(d["workspace_members"]); sys.exit(not any(p["name"] == "rfuse3" and p["id"] in m for p in d["packages"]))' <<< "$metadata"; then
    for runtime in tokio-runtime io-uring-runtime async-io-runtime; do
        cargo check --locked -p rfuse3 --no-default-features --features "$runtime"
    done
fi
cargo check --locked -p brewfs --no-default-features --features fuse-tokio-runtime
cargo check --locked -p brewfs --no-default-features --features fuse-io-uring-runtime
cargo test --locked --workspace --lib --bins
# The default workspace gate above includes CLI and stats. Run the complete
# feature-gated library suite separately so unrelated stats dependencies do not
# force a second build of the same Overlay test cases.
cargo test --locked --lib --no-default-features --features "$features"
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo clippy --locked --workspace --all-targets --no-default-features --features "$features" -- -D warnings
git diff --check
set +x
date -u --iso-8601=seconds
printf '%s\n' passed > "$output/status.txt"
