#!/usr/bin/env bash
# Normal optimized build; run in a Linux checkout. Keep before/after artifacts.
set -euo pipefail
source_root=$(cd -- "$(dirname -- "$0")/../.." && pwd)
output=${1:?usage: build_hot_overlay.sh ABSOLUTE_NEW_OUTPUT_DIRECTORY}
[[ $output == /* && ! -e $output ]] || { echo 'Output must be new and absolute' >&2; exit 2; }
mkdir -p -- "$output"
cd -- "$source_root"
export CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_DEBUG=0
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-2}
unset CARGO_PROFILE_RELEASE_OPT_LEVEL
exec > >(tee "$output/build.log") 2>&1
date -u --iso-8601=seconds
rustc -Vv
cargo -V
git rev-parse HEAD > "$output/source-commit.txt"
git diff --binary HEAD > "$output/source.patch"
if [[ -n $(git ls-files --others --exclude-standard) ]]; then
    echo 'Add new source files to the index (intent-to-add suffices) before recording the build' >&2
    exit 2
fi
set -x
cargo --config profile.release.package.brewfs.opt-level=3 build --locked --release \
  --no-default-features \
  --features fuse-io-uring-runtime,workspace,native-packed-base,frozen-base-metadata,rustfs-ec-block-size-hint,observability \
  --bin brewfs --bin packed_snapshot_fixture
set +x
target=$(cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
cp -- "$target/release/brewfs" "$target/release/packed_snapshot_fixture" "$output/"
(cd -- "$output" && sha256sum brewfs packed_snapshot_fixture source.patch > SHA256SUMS)
date -u --iso-8601=seconds
