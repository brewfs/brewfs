#!/usr/bin/env bash
#
# Static checks for run_s3_git_clone_smoke.sh, runnable without Docker.
#
# The smoke itself needs a Docker daemon, so CI cannot execute it. These checks
# cover the properties that would silently rot otherwise: the driver parses,
# the embedded in-container driver parses, CRLF is normalised before the inner
# driver is handed to Linux, the S3 credential guard is present, and the
# harness tears its services and volumes down.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
runner="$SCRIPT_DIR/run_s3_git_clone_smoke.sh"

fail() { echo "FAIL: $*" >&2; exit 1; }

[[ -f "$runner" ]] || fail "missing $runner"

bash -n "$runner" || fail "$runner failed bash -n"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

inner="$tmpdir/inner.sh"
awk "/^cat >\"\\\$inner\" <<'SMOKE_INNER'\$/{flag=1;next}/^SMOKE_INNER\$/{flag=0}flag" \
    "$runner" >"$inner"

if ! grep -q 'brewfs mount' "$inner"; then
    fail "could not extract the embedded inner driver from $runner"
fi

bash -n "$inner" || fail "embedded inner driver failed bash -n"

if grep -q $'\r' <<<"$(sed -n '1,5p' "$inner")"; then
    fail "embedded inner driver still contains CRLF"
fi
grep -q "sed -i 's/\\\\r\$//' \"\\\$inner\"" "$runner" \
    || fail "$runner must normalise CRLF in the inner driver before running it"

for needle in \
    ': "${AWS_ACCESS_KEY_ID:' \
    ': "${AWS_SECRET_ACCESS_KEY:' \
    ': "${BREWFS_META_URL:' \
    ': "${BREWFS_S3_ENDPOINT:'; do
    grep -qF "$needle" "$inner" || fail "inner driver is missing the guard: $needle"
done

grep -q 'git clone' "$inner" || fail "inner driver does not clone a git repo"
grep -q 'git fsck' "$inner" || fail "inner driver does not run git fsck"
grep -q 'rm -rf "${cache_root:?}"' "$inner" \
    || fail "inner driver must wipe the local cache before the remount check"

grep -q 'compose down -v --remove-orphans' "$runner" \
    || fail "$runner must tear down services and volumes"
grep -q 'BREWFS_SMOKE_KEEP' "$runner" \
    || fail "$runner must expose BREWFS_SMOKE_KEEP for debugging"

echo "test_s3_git_clone_smoke_script: OK"
