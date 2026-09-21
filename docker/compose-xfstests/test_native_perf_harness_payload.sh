#!/usr/bin/env bash
# Contract guard between the perf runners and the Aliyun native harness payload.
#
# The 2026-09-19 re-run of the current main died on the freshly provisioned ECS
# with:
#   python3: can't open file '/usr/local/bin/perf_manifest.py': [Errno 2] ...
# The runner had been taught to write run-manifest.json, but the helper only
# existed in the checkout that introduced it. The VM image bake and the per-run
# harness refresh shipped run_perf_in_container.sh and left the helper behind,
# so a full ECS + Tair + OSS round was paid for before the leg failed.
#
# Every /usr/local/bin/*.py helper a perf runner invokes must therefore:
#   1. have a source file in this repository,
#   2. ride along with the Aliyun binary upload,
#   3. be installed by the image bake and by the per-run refresh,
#   4. be copied into the container image used by the compose runners.
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ALIYUN="$SCRIPT_DIR/aliyun"

fail() { echo "FAIL: $*" >&2; exit 1; }

runners=(
    "$SCRIPT_DIR/run_perf_in_container.sh"
    "$SCRIPT_DIR/run_juicefs_perf_in_container.sh"
)
for runner in "${runners[@]}"; do
    [[ -f "$runner" ]] || fail "missing perf runner: $runner"
    grep -qF 'repeat_count="${PERF_FIO_BIGREAD_REPEATS:-3}"' "$runner" \
        || fail "$(basename "$runner") reports three bigread repeats but does not execute three by default"
    grep -qF 'warmup_count="${PERF_FIO_BIGREAD_WARMUP_PASSES:-1}"' "$runner" \
        || fail "$(basename "$runner") reports one bigread warmup but does not execute one by default"
done
echo "OK bigread     : reported and executed repeat defaults agree"

helpers="$(grep -rhoE '/usr/local/bin/[A-Za-z0-9_.-]+\.py' "${runners[@]}" | sort -u)"
[[ -n "$helpers" ]] || fail 'no /usr/local/bin/*.py helper references found in the perf runners'

echo "helper references discovered:"
printf '  %s\n' $helpers

while IFS= read -r ref; do
    [[ -n "$ref" ]] || continue
    name="${ref##*/}"

    src=''
    for candidate in "$SCRIPT_DIR/$name" "$REPO_ROOT/tools/perf/$name"; do
        if [[ -f "$candidate" ]]; then
            src="$candidate"
            break
        fi
    done
    [[ -n "$src" ]] || fail "$name is invoked as $ref but has no source file in this repository"
    echo "OK source      : $name <- ${src#"$REPO_ROOT"/}"

    grep -q "'$name'" "$ALIYUN/run_aliyun_perf.ps1" \
        || fail "$name is missing from the Aliyun binary-upload harness list (run_aliyun_perf.ps1)"
    grep -q "refresh_harness $name " "$ALIYUN/run_aliyun_perf.ps1" \
        || fail "$name is not refreshed on the VM on every run (run_aliyun_perf.ps1)"
    echo "OK upload      : $name is shipped and refreshed per run"

    grep -q "/usr/local/bin/$name" "$ALIYUN/prepare_native_perf_vm.sh" \
        || fail "$name is not installed by the VM image bake (prepare_native_perf_vm.sh)"
    grep -q "$name" "$ALIYUN/invoke_native_vm_prepare.ps1" \
        || fail "$name is not staged into the prepare payload (invoke_native_vm_prepare.ps1)"
    grep -q "/usr/local/bin/$name" "$ALIYUN/maintain_aliyun_perf_image.ps1" \
        || fail "$name is not installed by the image maintainer (maintain_aliyun_perf_image.ps1)"
    echo "OK image       : $name is installed by both image paths"

    grep -qE "^COPY .*$name /usr/local/bin/$name" "$SCRIPT_DIR/Dockerfile" \
        || fail "$name is not copied into the container image (docker/compose-xfstests/Dockerfile)"
    echo "OK container   : $name is copied into the compose image"
done <<<"$helpers"

# Line-ending guard. Every payload builder runs on Windows, where a checkout
# with core.autocrlf=true hands back CRLF text. The 2026-09-19 run11 attempt
# shipped run_native_perf.sh that way and the VM answered with
#   /opt/brewfs-perf/native/run_native_perf.sh: line 2: $'\r': command not found
# so each builder must normalize to LF before the bytes leave the machine.
for builder in \
    "$ALIYUN/run_aliyun_perf.ps1" \
    "$ALIYUN/invoke_native_vm_prepare.ps1" \
    "$ALIYUN/maintain_aliyun_perf_image.ps1"; do
    grep -qF '`r`n' "$builder" \
        || fail "$(basename "$builder") does not normalize CRLF before shipping harness files"
done
echo "OK line-endings: every payload builder normalizes CRLF to LF"

echo "PASS: every perf-runner helper is available in every provisioning path"
