#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(realpath "$SCRIPT_DIR/../..")"
RUNNER="$REPO_DIR/docker/compose-xfstests/aliyun/run_aliyun_packed_native.sh"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

# Every executable is a local sentinel. The fixture exits before mount startup,
# so this contract test never publishes an object or creates a FUSE mount.
cat >"$tmpdir/fixture" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$@" >"$FIXTURE_ARGS_LOG"
exit 73
SH
cat >"$tmpdir/unused" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "unexpected BrewFS/scanner invocation" >&2
exit 74
SH
chmod 0755 "$tmpdir/fixture" "$tmpdir/unused"

run_runner() {
    local format="$1" work="$2" log="$3"
    env BREWFS_BIN="$tmpdir/unused" PACKED_FIXTURE_BIN="$tmpdir/fixture" \
        PACKED_VOLUME_FORMAT="$format" BREWFS_S3_BUCKET="contract-test" \
        BREWFS_S3_ENDPOINT="https://unused.invalid" BREWFS_S3_REGION="contract-test" \
        PACKED_SMALLFILE_COUNT=1 PACKED_SMALLFILE_SIZE=64 PACKED_DIR_LEVELS=0 \
        PACKED_DIRS_PER_LEVEL=1 PACKED_FILES_PER_DIR=1 \
        PACKED_SMALLFILES_SCANNER="$tmpdir/unused" BREWFS_NATIVE_WORK="$work" \
        BREWFS_CACHE_ROOT="$work/cache" \
        BREWFS_MOUNT_POINT="$work/mount" BREWFS_NATIVE_ARTIFACT_DIR="$work/artifacts" \
        FIXTURE_ARGS_LOG="$tmpdir/fixture-args" \
        bash "$RUNNER" >"$log" 2>&1
}

for format in packed-metadata-v1 packed-metadata-v2 unknown-format; do
    work="$tmpdir/$format"
    if run_runner "$format" "$work" "$tmpdir/$format.log"; then
        echo "expected rejection before fixture publication: $format" >&2
        exit 1
    fi
    if ! grep -Fq -- "unsupported packed volume format: $format" "$tmpdir/$format.log"; then
        cat "$tmpdir/$format.log" >&2
        echo "runner did not explicitly reject $format" >&2
        exit 1
    fi
    [[ ! -e "$work" ]] || { echo "rejected format created its work directory: $format" >&2; exit 1; }
    [[ ! -e "$tmpdir/fixture-args" ]] || { echo "rejected format invoked the fixture: $format" >&2; exit 1; }
done

status=0
run_runner packed-snapshot "$tmpdir/v3" "$tmpdir/v3.log" || status=$?
if [[ "$status" != 73 ]]; then
    cat "$tmpdir/v3.log" >&2
    echo "v3 must reach the fixture sentinel and stop before mount, status=$status" >&2
    exit 1
fi
python3 - "$tmpdir/fixture-args" <<'PY'
import pathlib
import sys

args = pathlib.Path(sys.argv[1]).read_text().splitlines()
assert args.count("--wire-version") == 1, args
index = args.index("--wire-version")
assert args[index + 1] == "5", args
PY
echo "Aliyun packed runner rejects v1/v2 and selects v3 wire 005 before publication"
