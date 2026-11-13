#!/usr/bin/env bash
# Contract guard for the packed-only Aliyun payload.
#
# The cloud runner intentionally does not use the historical generic FIO/metadata
# fallback harness.  It ships only the packed fixture, runner, scanner, and
# release binary, and it places artifacts on the disposable data disk.
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ALIYUN="$SCRIPT_DIR/aliyun"
fail() { echo "FAIL: $*" >&2; exit 1; }

runner="$ALIYUN/run_aliyun_packed_native.sh"
ps1="$ALIYUN/run_aliyun_perf.ps1"
[[ -f "$runner" ]] || fail "missing packed runner: $runner"
[[ -f "$ps1" ]] || fail "missing Aliyun launcher: $ps1"

grep -qF 'packed-tree|packed-stat|packed-smallfiles|packed-gpu-smallfiles' "$runner" \
  || fail 'packed runner does not enforce the packed tool allowlist'
grep -qF 'PACKED_VOLUME_FORMAT' "$runner" \
  || fail 'packed runner does not require PACKED_VOLUME_FORMAT'
grep -qF 'perf-summary.tsv' "$runner" \
  || fail 'packed runner does not emit a performance summary'
grep -qF 'run_packed_native.sh' "$ps1" \
  || fail 'Aliyun launcher does not upload the packed runner'
grep -qF 'smallfiles_scan.py' "$ps1" \
  || fail 'Aliyun launcher does not upload the packed scanner'
grep -qF 'DATA_MOUNT=/mnt/brewfs-data' "$ps1" \
  || fail 'Aliyun launcher does not use the disposable data disk'
grep -qF 'BREWFS_NATIVE_ARTIFACT_DIR' "$ps1" \
  || fail 'Aliyun launcher does not bind the artifact directory'
grep -qF 'Normalize text payloads' "$ps1" \
  || fail 'Aliyun launcher does not normalize text payload line endings'
grep -qF '[char]13' "$ps1" \
  || fail 'Aliyun launcher line-ending normalization is incomplete'

# Keep this guard explicit: the obsolete generic helper must not silently become
# part of the packed-only cloud campaign again.
if grep -qE 'perf_metadata_fallback|run_perf_in_container|run_juicefs_perf_in_container' "$ps1"; then
  fail 'packed-only Aliyun launcher still references the historical generic harness'
fi

bash -n "$runner"

echo 'PASS: packed-only Aliyun payload ships fixture, scanner, and data-disk artifacts'
