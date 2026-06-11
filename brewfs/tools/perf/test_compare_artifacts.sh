#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$SCRIPT_DIR/compare_artifacts.py"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

baseline="$tmp_dir/perf-run-baseline"
candidate="$tmp_dir/perf-run-candidate"

mkdir -p "$baseline/results" "$baseline/diagnostics" "$candidate/results" "$candidate/diagnostics"

write_summary() {
    local dir="$1"
    cat >"$dir/perf-summary.tsv" <<'EOF'
tool	status	seconds	log
fio-seqwrite-direct1	pass	11	tools/fio-seqwrite-direct1.log
fio-randrw-direct1	pass	21	tools/fio-randrw-direct1.log
EOF
}

write_drain() {
    local dir="$1"
    local seq_drain="$2"
    local rand_pending="$3"
    cat >"$dir/post-write-drain.tsv" <<EOF
tool	post_fio_drain_s	pending_bytes	dirty_bytes	buffer_dirty_bytes
fio-seqwrite-direct1	$seq_drain	0	0	0
fio-randrw-direct1	timeout:30	$rand_pending	2097152	0
EOF
}

write_stats() {
    local dir="$1"
    local pending="$2"
    local dirty="$3"
    local put_ops="$4"
    local put_bytes="$5"
    local get_ops="$6"
    local get_bytes="$7"
    local uploaded="$8"
    cat >"$dir/diagnostics/stats-fio-randrw-direct1-after.txt" <<EOF
2026-06-11T00:00:00+00:00

brewfs_writeback_recent_pending_upload_bytes $pending
brewfs_writeback_dirty_bytes $dirty
brewfs_writeback_live_dirty_bytes $dirty
brewfs_buffer_dirty_bytes 0
brewfs_writeback_recent_uploaded_bytes $uploaded
brewfs_fuse_write_bytes_total 536870912
brewfs_fuse_read_bytes_total 268435456
brewfs_s3_put_ops_total $put_ops
brewfs_s3_put_bytes_total $put_bytes
brewfs_s3_put_avg_lat_us 25000
brewfs_s3_get_ops_total $get_ops
brewfs_s3_get_bytes_total $get_bytes
brewfs_s3_get_avg_lat_us 12000
brewfs_writeback_backpressure_soft_sleep_ops 12
brewfs_writeback_backpressure_soft_sleep_us 36000
brewfs_writeback_backpressure_hard_wait_ops 3
brewfs_writeback_backpressure_hard_wait_us 9000
EOF
}

write_missing_stats() {
    local dir="$1"
    local missing_bytes="$2"
    {
        cat <<'EOF'
2026-06-11T00:00:00+00:00

brewfs_writeback_recent_pending_upload_bytes 0
brewfs_writeback_dirty_bytes 0
brewfs_writeback_live_dirty_bytes 0
brewfs_buffer_dirty_bytes 0
brewfs_writeback_recent_uploaded_bytes 1048576
brewfs_fuse_write_bytes_total 1048576
brewfs_fuse_read_bytes_total 1048576
brewfs_s3_put_ops_total 1
brewfs_s3_get_ops_total 1
brewfs_s3_put_avg_lat_us 1000
brewfs_s3_get_avg_lat_us 1000
brewfs_writeback_backpressure_soft_sleep_ops 0
brewfs_writeback_backpressure_soft_sleep_us 0
brewfs_writeback_backpressure_hard_wait_ops 0
brewfs_writeback_backpressure_hard_wait_us 0
EOF
        if [[ "$missing_bytes" != "true" ]]; then
            cat <<'EOF'
brewfs_s3_put_bytes_total 1048576
brewfs_s3_get_bytes_total 1048576
EOF
        fi
    } >"$dir/diagnostics/stats-fio-missing-after.txt"
}

write_fio() {
    local path="$1"
    local rw="$2"
    local read_bw="$3"
    local write_bw="$4"
    local read_p99="$5"
    local write_p99="$6"
    cat >"$path" <<EOF
{
  "jobs": [
    {
      "job options": {
        "rw": "$rw",
        "bs": "4m",
        "numjobs": "1",
        "direct": "1"
      },
      "job_runtime": 10000,
      "read": {
        "bw_bytes": $read_bw,
        "iops": 10,
        "runtime": 10000,
        "total_ios": 100,
        "io_bytes": 1048576000,
        "clat_ns": {
          "mean": 50000000,
          "N": 100,
          "percentile": {
            "95.000000": 80000000,
            "99.000000": $read_p99,
            "99.900000": $((read_p99 * 2))
          }
        }
      },
      "write": {
        "bw_bytes": $write_bw,
        "iops": 20,
        "runtime": 10000,
        "total_ios": 200,
        "io_bytes": 2097152000,
        "clat_ns": {
          "mean": 75000000,
          "N": 200,
          "percentile": {
            "95.000000": 120000000,
            "99.000000": $write_p99,
            "99.900000": $((write_p99 * 2))
          }
        }
      }
    }
  ]
}
EOF
}

write_summary "$baseline"
write_summary "$candidate"
write_drain "$baseline" 4 1048576
write_drain "$candidate" 8 524288
write_stats "$baseline" 1048576 2097152 32 134217728 16 67108864 104857600
write_stats "$candidate" 524288 1048576 64 268435456 8 33554432 157286400
write_missing_stats "$baseline" false
write_missing_stats "$candidate" true

write_fio "$baseline/results/fio-seqwrite-direct1.json" write 0 104857600 0 200000000
write_fio "$candidate/results/fio-seqwrite-direct1.json" write 0 131072000 0 250000000
write_fio "$baseline/results/fio-randrw-direct1.json" randrw 209715200 83886080 100000000 150000000
write_fio "$candidate/results/fio-randrw-direct1.json" randrw 230686720 94371840 90000000 180000000

python3 "$SCRIPT" --format tsv "$baseline" "$candidate" >"$tmp_dir/out.tsv"
grep -F $'kind	item	metric	baseline	candidate	delta_pct	unit' "$tmp_dir/out.tsv" >/dev/null
grep -F $'fio	fio-seqwrite-direct1	write_bw_mib_s	100.000	125.000	+25.0	MiB/s' "$tmp_dir/out.tsv" >/dev/null
grep -F $'fio	fio-randrw-direct1	read_p99_ms	100.000	90.000	-10.0	ms' "$tmp_dir/out.tsv" >/dev/null
grep -F $'fio	fio-randrw-direct1	write_p999_ms	300.000	360.000	+20.0	ms' "$tmp_dir/out.tsv" >/dev/null
grep -F $'drain	fio-seqwrite-direct1	post_write_drain_s	4.000	8.000	+100.0	s' "$tmp_dir/out.tsv" >/dev/null
grep -F $'stats	fio-randrw-direct1	backpressure_pending_mib	1.000	0.500	-50.0	MiB' "$tmp_dir/out.tsv" >/dev/null
grep -F $'stats	fio-randrw-direct1	s3_put_mib	128.000	256.000	+100.0	MiB' "$tmp_dir/out.tsv" >/dev/null
grep -F $'stats	fio-randrw-direct1	s3_get_mib	64.000	32.000	-50.0	MiB' "$tmp_dir/out.tsv" >/dev/null
grep -F $'stats	fio-randrw-direct1	writeback_soft_sleep_ops	12.000	12.000	+0.0	ops' "$tmp_dir/out.tsv" >/dev/null
grep -F $'stats	fio-randrw-direct1	writeback_hard_wait_ms	9.000	9.000	+0.0	ms' "$tmp_dir/out.tsv" >/dev/null
grep -F $'amplification	fio-randrw-direct1	upload_byte_amp	0.250	0.500	+100.0	ratio' "$tmp_dir/out.tsv" >/dev/null
grep -F $'amplification	fio-randrw-direct1	put_ops_per_gib_written	64.000	128.000	+100.0	ops/GiB' "$tmp_dir/out.tsv" >/dev/null
grep -F $'runtime	fio-seqwrite-direct1	active_plus_drain_s	14.000	18.000	+28.6	s' "$tmp_dir/out.tsv" >/dev/null
grep -F $'runtime	fio-randrw-direct1	wall_active_tail_s	11.000	11.000	+0.0	s' "$tmp_dir/out.tsv" >/dev/null
grep -F $'gap	fio-missing	missing_critical_stats	none	brewfs_s3_put_bytes_total,brewfs_s3_get_bytes_total		' "$tmp_dir/out.tsv" >/dev/null

python3 "$SCRIPT" --format markdown --baseline-label base --candidate-label cand "$baseline" "$candidate" >"$tmp_dir/out.md"
grep -F "Baseline: \`base\`" "$tmp_dir/out.md" >/dev/null
grep -F "## Fio" "$tmp_dir/out.md" >/dev/null
grep -F "## Drain And Backpressure" "$tmp_dir/out.md" >/dev/null
grep -F "## Runtime And Tail" "$tmp_dir/out.md" >/dev/null
grep -F "## Object And Upload Amplification" "$tmp_dir/out.md" >/dev/null
grep -F "## Metric Gaps" "$tmp_dir/out.md" >/dev/null

if python3 "$SCRIPT" "$tmp_dir/missing" "$candidate" >"$tmp_dir/missing.out" 2>"$tmp_dir/missing.err"; then
    echo "expected missing artifact to fail" >&2
    exit 1
fi
grep -F "missing artifact directory" "$tmp_dir/missing.err" >/dev/null

echo "compare_artifacts fixture passed"
