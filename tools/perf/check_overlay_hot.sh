#!/usr/bin/env bash
# Redis/KV-only checks for hot Overlay changes; no SQLite tests are executed.
set -euo pipefail
test "$#" = 1 || { echo 'Usage: bash tools/perf/check_overlay_hot.sh /absolute/new-results' >&2; exit 2; }
output=$(realpath -m "$1")
test ! -e "$output"
mkdir -p "$output"
export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
export RUST_TEST_THREADS="${RUST_TEST_THREADS:-2}"
features=fuse-io-uring-runtime,workspace,native-packed-base,frozen-base-metadata,rustfs-ec-block-size-hint,observability
rustc -Vv >"$output/toolchain.txt"
cargo fmt --all --check >"$output/fmt.log" 2>&1
for filter in fenced_layer_capture packed_heartbeat_keeps_file_cache \
  packed_readonly_payload_retention_is_opt_in \
  workspace::stores::kv_store:: \
  workspace::meta_layer::tests::packed_open_preparation_tests:: \
  workspace::meta_layer::row_cache::tests:: \
  vfs::io::writer::tests::packed_snapshot_tests:: \
  workspace::stores::local_metadata::tests:: ready_driver; do
  label=${filter//:/_}
  cargo test --locked --lib --no-default-features --features "$features" "$filter" \
    -- --nocapture >"$output/$label.log" 2>&1
  python3 - "$output/$label.log" <<'PY'
import re, sys
from pathlib import Path
text = Path(sys.argv[1]).read_text()
assert re.search(r'test result: ok\. [1-9][0-9]* passed; 0 failed;', text), sys.argv[1]
PY
done
cargo clippy --locked --workspace --all-targets --no-default-features \
  --features "$features" -- -D warnings >"$output/clippy-measured.log" 2>&1
cargo check --locked --workspace >"$output/check-default.log" 2>&1
cargo build --locked --workspace >"$output/build-default.log" 2>&1
cargo clippy --locked --workspace --all-targets -- -D warnings >"$output/clippy-default.log" 2>&1
for feature in fuse-tokio-runtime fuse-io-uring-runtime; do
  cargo check --locked -p brewfs --no-default-features --features "$feature" \
    >"$output/check-$feature.log" 2>&1
done
for script in run_perf_in_container.sh run_redis_perf.sh run_juicefs_perf_in_container.sh run_juicefs_perf.sh; do
  bash -n "docker/compose-xfstests/$script"
done
for script in test_perf_report_delta.sh test_juicefs_direct_matrix.sh test_juicefs_perf_report.sh; do
  bash "docker/compose-xfstests/$script" >"$output/$script.log" 2>&1
done
printf 'passed\n' >"$output/status.txt"
