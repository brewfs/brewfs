#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(realpath "$SCRIPT_DIR/../..")"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

artifact_dir="$tmpdir/artifact"
export BREWFS_ARTIFACT_DIR="$artifact_dir"
mkdir -p "$artifact_dir/results" "$artifact_dir/tools" "$artifact_dir/diagnostics"

cat >"$artifact_dir/perf-summary.tsv" <<'EOF'
tool	status	seconds	log
fio-seqwrite-direct0	pass	2	/artifacts/juicefs-perf-run/tools/fio-seqwrite-direct0.log
EOF

cat >"$artifact_dir/post-write-drain.tsv" <<'EOF'
tool	post_fio_drain_s	stage_blocks	stage_bytes	uploading	put_bytes	get_bytes
fio-seqwrite-direct0	3	0	0	0	16777216	0
EOF

cat >"$artifact_dir/juicefs-profile.env" <<'EOF'
JFS_COMPRESS=none
JFS_WRITEBACK=true
JFS_BUFFER_SIZE_MIB=8192
JFS_CACHE_SIZE_MIB=4096
EOF

cat >"$artifact_dir/results/fio-seqwrite-direct0.json" <<'EOF'
{
  "jobs": [
    {
      "job_runtime": 2000,
      "job options": {
        "rw": "write",
        "bs": "4m",
        "size": "16m",
        "numjobs": "1",
        "direct": "0",
        "runtime": "1"
      },
      "read": {
        "io_bytes": 0,
        "bw_bytes": 0,
        "iops": 0,
        "runtime": 0,
        "clat_ns": {"mean": 0, "N": 0, "percentile": {}}
      },
      "write": {
        "io_bytes": 16777216,
        "bw_bytes": 104857600,
        "iops": 25,
        "runtime": 1000,
        "clat_ns": {
          "mean": 1000000,
          "N": 25,
          "percentile": {
            "95.000000": 2000000,
            "99.000000": 3000000
          }
        }
      }
    }
  ]
}
EOF

# Source all functions without running main.
source <(sed '$d' "$REPO_DIR/docker/compose-xfstests/run_juicefs_perf_in_container.sh")
artifact_dir="$BREWFS_ARTIFACT_DIR"

generate_perf_report

report="$artifact_dir/report.md"
trap 'status=$?; if [[ $status -ne 0 && -f "$report" ]]; then cat "$report" >&2; fi; rm -rf "$tmpdir"' EXIT

grep -Fq '# JuiceFS Perf Report' "$report"
grep -Fq '| JFS_WRITEBACK | true |' "$report"
grep -Fq '| fio-seqwrite-direct0 | pass | 2 | tools/fio-seqwrite-direct0.log |' "$report"
grep -Fq '| fio-seqwrite-direct0 | write | 0 | 4m | 1 | 0.00 B/s | 0.00 | 100.00 MiB/s | 25.00 | 0.000 ms | 3.000 ms | results/fio-seqwrite-direct0.json |' "$report"
grep -Fq '| fio-seqwrite-direct0 | 3 | 0 | 0 | 0 | 16777216 | 0 |' "$report"
