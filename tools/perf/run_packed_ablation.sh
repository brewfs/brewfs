#!/usr/bin/env bash
# Same-trace packed/005 ablation. Every row uses one release binary, one
# fixture generator, one scanner seed/layout (identical file trace), identical
# FUSE/cache budgets and the paired native-vs-packed driver, varying exactly
# one factor against the baseline row. Each row is its own paired group with
# its own artifact directory, RustFS bucket lifetime and TiKV namespace.
#
# A p90-training row needs a training trace captured from a separate, earlier
# run (never the measured run); pass it with ABLATION_P90_TRACE or the row is
# recorded as incomplete instead of being fabricated.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT="${ABLATION_ARTIFACT_ROOT:?set ABLATION_ARTIFACT_ROOT}"
P90_TRACE="${ABLATION_P90_TRACE:-}"
ROWS="${ABLATION_ROWS:-baseline inline-on metadata-raw data-raw static-1mib p90}"
mkdir -p "$OUT"

# Baseline: dynamic (size-only) placement, inline off, zstd/zstd. Inline off is
# the baseline because the native-tikv arm rejects inline payloads.
declare -A FRAME=([baseline]=size-only [inline-on]=size-only [metadata-raw]=size-only [data-raw]=size-only [static-1mib]=static-1mib [p90]=p90-training)
declare -A INLINE=([baseline]=off [inline-on]=on [metadata-raw]=off [data-raw]=off [static-1mib]=off [p90]=off)
declare -A MCODEC=([baseline]=zstd [inline-on]=zstd [metadata-raw]=raw [data-raw]=zstd [static-1mib]=zstd [p90]=zstd)
declare -A DCODEC=([baseline]=zstd [inline-on]=zstd [metadata-raw]=zstd [data-raw]=raw [static-1mib]=zstd [p90]=zstd)

status=0
for row in $ROWS; do
    [[ -n "${FRAME[$row]:-}" ]] || { echo "unknown ablation row $row" >&2; exit 2; }
    extra=()
    if [[ "$row" == p90 ]]; then
        if [[ -z "$P90_TRACE" ]]; then
            printf '{"row":"p90","status":"incomplete","reason":"no independent training trace supplied"}\n' >>"$OUT/ablation.jsonl"
            echo "row=p90 incomplete (no training trace)"
            continue
        fi
        extra=(PACKED_LOCAL_P90_TRAINING_TRACE="$P90_TRACE")
    fi
    # inline-on is packed-only: the native arm rejects inline payloads, so the
    # factor is measured on the packed arm alone (still same trace/budget).
    arms_note=paired
    [[ "$row" == inline-on ]] && arms_note=packed-only
    env "${extra[@]}" PAIRED_ARTIFACT_ROOT="$OUT" PAIRED_LABEL="abl-$row" \
        PAIRED_ARMS="$( [[ $arms_note == packed-only ]] && echo packed || echo 'packed native-tikv' )" \
        PACKED_LOCAL_FRAME_POLICY="${FRAME[$row]}" PACKED_LOCAL_INLINE_DATA="${INLINE[$row]}" \
        PACKED_LOCAL_METADATA_CODEC="${MCODEC[$row]}" PACKED_LOCAL_DATA_CODEC="${DCODEC[$row]}" \
        bash "$ROOT/tools/perf/run_packed_paired.sh" >"$OUT/abl-$row.console.log" 2>&1
    rc=$?
    group="$(grep -oE 'group=[^ ]+' "$OUT/abl-$row.console.log" | tail -1 | cut -d= -f2)"
    printf '{"row":"%s","arms":"%s","exit":%s,"group":"%s"}\n' "$row" "$arms_note" "$rc" "$group" >>"$OUT/ablation.jsonl"
    echo "row=$row exit=$rc"
    (( rc == 0 )) || status=1
done
exit "$status"
