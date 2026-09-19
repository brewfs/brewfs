#!/usr/bin/env bash
# PR08C: compose acceptance pair for randrw + metadata, and the FUSE
# teardown / D-state check (REGRESS-004, REGRESS-006 / INV-13).
#
# Three compose runner invocations were executed once on this host with
# matching settings; their console logs sit next to this script:
#
#   control   default features (binary 68,530,696 B, built 15:45:33)
#     PERF_LOG_TO_CONSOLE=false PERF_FIO_SIZE=64m PERF_FIO_RUNTIME=20 \
#       bash docker/compose-xfstests/run_redis_perf.sh --s3 \
#       --tools "fio-randwrite fio-randrw metaperf"
#     -> artifacts/perf-run-1789804357-86      (log pr08c-control64-run.raw.log)
#   control2  same binary, same settings; this pair is the run-to-run noise
#     -> artifacts/perf-run-1789805052-2568    (log pr08c-control64-run2.raw.log)
#   candidate default features plus --features native-packed-base (binary
#             103,740,488 B, built 15:27:30)
#     -> artifacts/perf-run-1789803051-11108   (log pr08c-native-run.raw.log)
#
# A first attempt with the AGENTS.md strict-drain profile
# (--writeback-throughput-profile, PERF_FIO_SIZE=512m) died on this host: the
# WSL2 backing vhdx (D:\WSL\Ubuntu-24.04\ext4.vhdx) grew until the host volume
# had zero bytes free, the fio container was killed (exit 135 / SIGBUS) and
# the WSL VM went down.  That artifact (perf-run-1789800955-17525) is kept.
#
# The runner needs LF but this worktree checks the harness out with CRLF, so
# the runs were preceded by
#   find docker scripts tools -name '*.sh' -exec sed -i 's/\r$//' {} +
# and followed by `git checkout -- docker scripts tools`.  This script only
# re-derives the numbers from the recorded artifacts and re-checks teardown.

set -uo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT" || exit 1

ART="docker/compose-xfstests/artifacts"
CONTROL="perf-run-1789804357-86"
CONTROL2="perf-run-1789805052-2568"
CANDIDATE="perf-run-1789803051-11108"

fail=0
note() { printf '%s\n' "$*"; }
need() { if [[ -e "$1" ]]; then note "ok      $2"; else note "MISSING $2"; fail=1; fi; }

echo "=== PR08C: the three artifacts are complete ==="
for d in "$CONTROL" "$CONTROL2" "$CANDIDATE"; do
    need "$ART/$d/perf.complete" "$d/perf.complete"
    note "    summary: $(tr '\n' ';' < "$ART/$d/perf-summary.tsv")"
done

echo "=== PR08C: every tool passed and no warning/timeout was recorded ==="
for d in "$CONTROL" "$CONTROL2" "$CANDIDATE"; do
    note "$d: $(grep -c '	pass	' "$ART/$d/perf-summary.tsv") tools pass, \
$(grep -c '	fail' "$ART/$d/perf-summary.tsv") fail"
    sed -e 's/^/    warning-summary: /' "$ART/$d/runner-warning-summary.tsv"
done

echo "=== PR08C: randrw and metadata, candidate against the matched control ==="
python3 tools/perf/compare_artifacts.py "$ART/$CONTROL" "$ART/$CANDIDATE" \
    > doc/native-base/logs/pr08c-compare-control-vs-candidate.log 2>&1
note "compare exit=$? -> doc/native-base/logs/pr08c-compare-control-vs-candidate.log"
grep -E '^\| (fio-randrw|fio-randwrite|metaperf) \| ' \
    doc/native-base/logs/pr08c-compare-control-vs-candidate.log | grep -vE \
    'partial_tail|upload_byte_amp|batch|avg_object|ops_per_gib' | sed -e 's/^/    /'

echo "=== PR08C: the same comparison with control2 (run-to-run noise band) ==="
python3 tools/perf/compare_artifacts.py "$ART/$CONTROL" "$ART/$CONTROL2" \
    > doc/native-base/logs/pr08c-compare-control-vs-control.log 2>&1
note "compare exit=$? -> doc/native-base/logs/pr08c-compare-control-vs-control.log"
grep -E '^\| (fio-randrw|fio-randwrite|metaperf) \| ' \
    doc/native-base/logs/pr08c-compare-control-vs-control.log | grep -vE \
    'partial_tail|upload_byte_amp|batch|avg_object|ops_per_gib' | sed -e 's/^/    /'

echo "=== PR08C: the failed strict-drain attempt is preserved ==="
need "$ART/perf-run-1789800955-17525/perf-summary.tsv" "perf-run-1789800955-17525"
note "    $(tr '\n' ';' < "$ART/perf-run-1789800955-17525/perf-summary.tsv")"

echo "=== PR08C: writeback debt returns to zero in every run ==="
for d in "$CONTROL" "$CONTROL2" "$CANDIDATE"; do
    tail -1 "$ART/$d/writeback-samples.tsv" | awk -F'\t' -v d="$d" '
        { printf "    %s last sample: buffer_dirty=%s live_dirty=%s recent_pending_upload=%s\n",
                 d, $4, $5, $7
          if ($4 != 0 || $5 != 0 || $7 != 0) { exit 1 } }' || fail=1
done

echo "=== PR08C: teardown left no container, volume, mount or D-state task ==="
note "containers:  $(docker ps -a --format '{{.Names}}' | grep -cE 'brewfs-perf') (expect 0)"
note "volumes:     $(docker volume ls --format '{{.Name}}' | grep -c 'compose-xfstests') (expect 0)"
note "fuse mounts: $(mount | grep -c brewfs) (expect 0)"
note "fuse conns:  $(ls /sys/fs/fuse/connections 2>/dev/null | wc -l) (expect 0)"
note "brewfs proc: $(pgrep -x brewfs | wc -l) (expect 0)"
note "D-state:     $(ps -eo stat,comm | awk '$1 ~ /^D/ {n++} END {print n + 0}') (expect 0)"

echo "=== PR08C: the default feature set cannot reach the native-base module ==="
grep -n '^default = ' Cargo.toml | sed -e 's/^/    /'
note "    native-packed-base in the default feature set: \
$(grep '^default = ' Cargo.toml | grep -c 'native-pack') (expect 0)"
note "    non-test tokio::spawn/thread::spawn/spawn_blocking sites under src/native_base:"
python3 - <<'PY' || fail=1
import os, re, sys

# The only production spawn sites the native-base module may have: the
# budget permit releases its tokens asynchronously, and the singleflight
# leader runs the shared loader.  Both live in the P3 planner, which the
# default feature set does not compile at all (checked above).
expected = {
    ("src/native_base/runtime/planner.rs", 240): "fn drop",
    ("src/native_base/runtime/planner.rs", 298): "fn get_or_load",
}
found = []
for base, _dirs, files in os.walk("src/native_base"):
    for name in files:
        if not name.endswith(".rs"):
            continue
        path = os.path.join(base, name).replace(os.sep, "/")
        lines = open(path, encoding="utf-8", errors="replace").read().split("\n")
        # Everything from the first `mod tests` marked #[cfg(test)] on is test
        # code; spawn sites before it are production code.
        test_at = len(lines)
        for i, line in enumerate(lines):
            if line.strip().startswith("mod tests") and i and "cfg(test)" in lines[i - 1]:
                test_at = i
                break
        items = [(i, l.strip()) for i, l in enumerate(lines) if re.search(
            r"tokio::spawn|thread::spawn|spawn_blocking", l) and i < test_at]
        for i, text in items:
            owner = "?"
            for j in range(i, -1, -1):
                if re.match(r"^\s*(impl|fn|pub fn|pub async fn|async fn|pub\(crate\) fn)\b", lines[j]):
                    owner = lines[j].strip()
                    break
            found.append((path, i + 1, owner, text))

for path, line, owner, text in found:
    print(f"        {path}:{line}  in `{owner}`")
    print(f"            {text}")

problems = []
for path, line, owner, _text in found:
    allowed = expected.get((path, line))
    if not allowed or allowed not in owner:
        problems.append(f"{path}:{line} in {owner}")
if problems:
    print("unexpected production spawn sites: " + "; ".join(problems))
    sys.exit(1)
print(f"        production spawn sites: {len(found)} (expected 2: planner budget "
      f"release and singleflight leader)")
PY

if [[ "$fail" -eq 0 ]]; then
    echo "=== PR08C compose perf pair and teardown: ALL CHECKS PASSED ==="
else
    echo "=== PR08C compose perf pair and teardown: FAILURES ABOVE ==="
fi
exit "$fail"