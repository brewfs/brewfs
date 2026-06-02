#!/usr/bin/env bash

set -euo pipefail

INPUT_DIR=""
NO_TAR=false

usage() {
    cat <<EOF
usage: $(basename "$0") <artifact_root or artifact_run_dir> [options]

description:
  - if the path contains results/, treat it as a single run's artifact_run_dir
  - otherwise pick the newest subdirectory under artifact_root

options:
  --no-tar    skip tarball generation
  -h, --help  show help
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --no-tar)
            NO_TAR=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            if [[ -z "$INPUT_DIR" ]]; then
                INPUT_DIR="$1"
                shift
            else
                echo "unknown arg: $1" >&2
                usage
            fi
            ;;
    esac
done

if [[ -z "$INPUT_DIR" ]]; then
    usage
fi

if [[ ! -d "$INPUT_DIR" ]]; then
    echo "directory not found: $INPUT_DIR" >&2
    exit 1
fi

run_dir=""
if [[ -d "$INPUT_DIR/results" ]]; then
    run_dir="$INPUT_DIR"
else
    latest="$(ls -1dt "$INPUT_DIR"/*/ 2>/dev/null | head -n 1 || true)"
    latest="${latest%/}"
    if [[ -z "$latest" || ! -d "$latest/results" ]]; then
        echo "no run directory with results/ found under $INPUT_DIR" >&2
        exit 1
    fi
    run_dir="$latest"
fi

results_dir="$run_dir/results"
output_dir="$run_dir/output"

report_md="$run_dir/report.md"
failures_txt="$run_dir/failures.txt"
summary_txt="$run_dir/summary.txt"

tail_file() {
    local path="$1"
    local lines="$2"
    if [[ -f "$path" ]]; then
        tail -n "$lines" "$path"
    else
        echo "missing: $path"
    fi
}

relpath() {
    local path="$1"
    local base="$2"
    if [[ "$path" == "$base" ]]; then
        echo "."
    elif [[ "$path" == "$base/"* ]]; then
        echo "${path#"$base/"}"
    else
        echo "$path"
    fi
}

# collect failed test log files from LTP results
failed_files=()
if [[ -d "$results_dir" ]]; then
    while IFS= read -r -d '' f; do
        failed_files+=("$f")
    done < <(find "$results_dir" -type f -name 'LTP_*' -print0 2>/dev/null | sort -z)
fi

# count failures from LTP results (look for FAIL lines)
fail_count=0
if [[ -d "$results_dir" ]]; then
    fail_count=$(grep -r '^.*\sFAIL\s' "$results_dir" 2>/dev/null | wc -l || echo 0)
fi

{
    echo "artifact_dir: $run_dir"
    echo "generated_at: $(date -Is)"
    echo
    echo "key_files:"
    for f in "brewfs.log" "ltp.console.log" "backend.yml" "results/" "output/"; do
        if [[ -e "$run_dir/$f" ]]; then
            echo "  - $f"
        else
            echo "  - $f (missing)"
        fi
    done
    echo
    echo "failures_count: $fail_count"
    if [[ "${#failed_files[@]}" -gt 0 ]]; then
        echo "result_files:"
        for f in "${failed_files[@]}"; do
            echo "  - $(relpath "$f" "$run_dir")"
        done
    fi
} >"$summary_txt"

{
    echo "# BrewFS LTP test report"
    echo
    echo "- artifact_dir: $run_dir"
    echo "- generated_at: $(date -Is)"
    echo
    echo "## key files"
    echo
    echo "- brewfs.log"
    echo "- ltp.console.log"
    echo "- backend.yml"
    echo "- results/"
    echo "- output/"
    echo
    echo "## summary"
    echo
    echo '```text'
    cat "$summary_txt"
    echo '```'
    echo
    echo "## failures"
    echo
    if [[ "$fail_count" -eq 0 ]]; then
        echo "- none"
    else
        echo "FAIL count: $fail_count"
        echo
        if [[ -f "$results_dir/LTP_".* ]]; then
            :
        fi
        echo '```text'
        if [[ -d "$results_dir" ]]; then
            grep -r '^.*\sFAIL\s' "$results_dir" 2>/dev/null | head -n 50 || true
        fi
        echo '```'
    fi
    echo
    echo "## ltp.console.log (tail)"
    echo
    echo '```text'
    tail_file "$run_dir/ltp.console.log" 200
    echo '```'
} >"$report_md"

{
    if [[ -d "$results_dir" ]]; then
        grep -r '^.*\sFAIL\s' "$results_dir" 2>/dev/null || true
    fi
} >"$failures_txt"

if [[ "$NO_TAR" == false ]]; then
    if command -v tar >/dev/null 2>&1; then
        parent="$(dirname "$run_dir")"
        base="$(basename "$run_dir")"
        tarball="${run_dir}.tar.gz"
        tar -C "$parent" -czf "$tarball" "$base"
    fi
fi

echo "report: $report_md"
echo "summary: $summary_txt"
echo "failures: $failures_txt"
if [[ "$NO_TAR" == false && -f "${run_dir}.tar.gz" ]]; then
    echo "tarball: ${run_dir}.tar.gz"
fi
