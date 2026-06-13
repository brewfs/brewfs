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
    local batch_ops="$9"
    local batch_bytes="${10}"
    local batch_blocks="${11}"
    local partial_tail_ops="${12}"
    cat >"$dir/diagnostics/stats-fio-randrw-direct1-after.txt" <<EOF
2026-06-11T00:00:00+00:00

brewfs_writeback_recent_pending_upload_bytes $pending
brewfs_writeback_dirty_bytes $dirty
brewfs_writeback_live_dirty_bytes $dirty
brewfs_writeback_live_slices 2
brewfs_buffer_dirty_bytes 0
brewfs_writeback_recent_uploaded_bytes $uploaded
brewfs_writeback_recent_pending_upload_slices 1
brewfs_writeback_recent_uploaded_slices 2
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
brewfs_writeback_commit_wait_upload_ops_total $((put_ops / 2))
brewfs_writeback_commit_wait_upload_us_total $((put_ops * 1000))
brewfs_writeback_commit_wait_retry_ops_total $partial_tail_ops
brewfs_writeback_commit_wait_retry_us_total $((partial_tail_ops * 2000))
brewfs_writeback_slice_create_ops_total 20
brewfs_writeback_slice_reuse_ops_total 100
brewfs_writeback_slice_reject_older_unique_ops_total 2
brewfs_writeback_slice_reject_dispatched_prefix_ops_total 5
brewfs_writeback_freeze_size_ops_total 6
brewfs_writeback_freeze_size_bytes_total 12582912
brewfs_writeback_freeze_max_unflushed_ops_total 1
brewfs_writeback_freeze_max_unflushed_bytes_total 1048576
brewfs_writeback_freeze_explicit_flush_ops_total 2
brewfs_writeback_freeze_explicit_flush_bytes_total 2097152
brewfs_writeback_freeze_auto_ops_total 3
brewfs_writeback_freeze_auto_bytes_total 3145728
brewfs_writeback_freeze_commit_age_ops_total 0
brewfs_writeback_freeze_commit_age_bytes_total 0
brewfs_writeback_upload_batch_ops_total $batch_ops
brewfs_writeback_upload_batch_bytes_total $batch_bytes
brewfs_writeback_upload_batch_blocks_total $batch_blocks
brewfs_writeback_upload_partial_tail_ops_total $partial_tail_ops
brewfs_writeback_upload_partial_tail_size_ops_total 0
brewfs_writeback_upload_partial_tail_max_unflushed_ops_total 1
brewfs_writeback_upload_partial_tail_explicit_flush_ops_total $((partial_tail_ops / 2))
brewfs_writeback_upload_partial_tail_auto_ops_total $((partial_tail_ops - partial_tail_ops / 2 - 1))
brewfs_writeback_upload_partial_tail_auto_age_ops_total $((partial_tail_ops - partial_tail_ops / 2 - 1))
brewfs_writeback_upload_partial_tail_auto_idle_ops_total 0
brewfs_writeback_upload_partial_tail_auto_pressure_ops_total 0
brewfs_writeback_upload_partial_tail_auto_too_many_ops_total 0
brewfs_writeback_upload_partial_tail_auto_buffer_high_ops_total 0
brewfs_writeback_upload_partial_tail_auto_flush_duration_ops_total 0
brewfs_writeback_upload_partial_tail_auto_unknown_ops_total 0
brewfs_writeback_upload_partial_tail_commit_age_ops_total 0
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
brewfs_writeback_live_slices 0
brewfs_writeback_recent_pending_upload_slices 0
brewfs_writeback_recent_uploaded_slices 1
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
brewfs_writeback_slice_create_ops_total 1
brewfs_writeback_upload_batch_ops_total 1
brewfs_writeback_upload_batch_bytes_total 1048576
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
write_stats "$baseline" 1048576 2097152 32 134217728 16 67108864 104857600 8 67108864 16 2
write_stats "$candidate" 524288 1048576 64 268435456 8 33554432 157286400 8 134217728 32 4
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
grep -F $'stats	fio-randrw-direct1	writeback_commit_wait_upload_ops	16.000	32.000	+100.0	ops' "$tmp_dir/out.tsv" >/dev/null
grep -F $'stats	fio-randrw-direct1	writeback_commit_wait_upload_ms	32.000	64.000	+100.0	ms' "$tmp_dir/out.tsv" >/dev/null
grep -F $'stats	fio-randrw-direct1	writeback_commit_wait_retry_ops	2.000	4.000	+100.0	ops' "$tmp_dir/out.tsv" >/dev/null
grep -F $'stats	fio-randrw-direct1	writeback_commit_wait_retry_ms	4.000	8.000	+100.0	ms' "$tmp_dir/out.tsv" >/dev/null
grep -F $'stats	fio-randrw-direct1	writeback_slice_create_ops	20.000	20.000	+0.0	ops' "$tmp_dir/out.tsv" >/dev/null
grep -F $'stats	fio-randrw-direct1	writeback_freeze_explicit_flush_ops	2.000	2.000	+0.0	ops' "$tmp_dir/out.tsv" >/dev/null
grep -F $'stats	fio-randrw-direct1	writeback_upload_batch_mib	64.000	128.000	+100.0	MiB' "$tmp_dir/out.tsv" >/dev/null
grep -F $'stats	fio-randrw-direct1	writeback_upload_partial_tail_explicit_flush_ops	1.000	2.000	+100.0	ops' "$tmp_dir/out.tsv" >/dev/null
grep -F $'stats	fio-randrw-direct1	writeback_upload_partial_tail_auto_ops	0.000	1.000		ops' "$tmp_dir/out.tsv" >/dev/null
grep -F $'stats	fio-randrw-direct1	writeback_upload_partial_tail_auto_age_ops	0.000	1.000		ops' "$tmp_dir/out.tsv" >/dev/null
grep -F $'amplification	fio-randrw-direct1	upload_byte_amp	0.250	0.500	+100.0	ratio' "$tmp_dir/out.tsv" >/dev/null
grep -F $'amplification	fio-randrw-direct1	put_ops_per_gib_written	64.000	128.000	+100.0	ops/GiB' "$tmp_dir/out.tsv" >/dev/null
grep -F $'amplification	fio-randrw-direct1	writeback_avg_upload_batch_mib	8.000	16.000	+100.0	MiB/op' "$tmp_dir/out.tsv" >/dev/null
grep -F $'amplification	fio-randrw-direct1	s3_puts_per_upload_batch	4.000	8.000	+100.0	ops/batch' "$tmp_dir/out.tsv" >/dev/null
grep -F $'amplification	fio-randrw-direct1	writeback_partial_tail_ratio	0.250	0.500	+100.0	ratio' "$tmp_dir/out.tsv" >/dev/null
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
