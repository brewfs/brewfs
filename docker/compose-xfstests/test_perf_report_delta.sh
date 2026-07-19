#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(realpath "$SCRIPT_DIR/../..")"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

artifact_dir="$tmpdir/artifact"
mkdir -p "$artifact_dir/diagnostics" "$artifact_dir/results" "$artifact_dir/tools"

cat >"$artifact_dir/perf-summary.tsv" <<'EOF'
tool	status	seconds	log
fio-test	pass	1	/artifacts/perf-run/tools/fio-test.log
EOF

cat >"$artifact_dir/post-write-drain.tsv" <<'EOF'
tool	post_write_drain_s	pending_bytes	dirty_bytes	buffer_dirty_bytes
fio-test	1	0	0	0
EOF

cat >"$artifact_dir/results/fio-test.json" <<'EOF'
{
  "jobs": [{
    "job options": {"rw": "write", "direct": "0", "bs": "1m", "numjobs": "1"},
    "job_runtime": 1000,
    "read": {"io_bytes": 0, "bw_bytes": 0, "iops": 0, "runtime": 0},
    "write": {
      "io_bytes": 104857600,
      "bw_bytes": 104857600,
      "iops": 100,
      "runtime": 1000,
      "clat_ns": {"mean": 1000000, "N": 100, "percentile": {"99.000000": 1000000}}
    }
  }]
}
EOF

cat >"$artifact_dir/diagnostics/stats-fio-test-before.txt" <<'EOF'
2026-06-14T00:00:00+00:00

brewfs_cache_hits_total 10
brewfs_cache_misses_total 5
brewfs_cache_requests_total 15
brewfs_fuse_read_bytes_total 10485760
brewfs_fuse_write_bytes_total 104857600
brewfs_s3_get_ops_total 2
brewfs_s3_get_bytes_total 20971520
brewfs_s3_get_lat_us_total 20000
brewfs_s3_put_ops_total 3
brewfs_s3_put_bytes_total 31457280
brewfs_s3_put_lat_us_total 30000
brewfs_writeback_stage_ops_total 4
brewfs_writeback_stage_bytes_total 41943040
brewfs_writeback_stage_lat_us_total 40000
brewfs_writeback_flush_wait_ops_total 1
brewfs_writeback_flush_wait_us_total 100000
brewfs_writeback_flush_wait_slices_total 2
brewfs_writeback_upload_batch_ops_total 5
brewfs_writeback_upload_batch_bytes_total 52428800
brewfs_writeback_upload_batch_blocks_total 10
brewfs_writeback_upload_batch_single_block_ops_total 2
brewfs_writeback_upload_batch_multi_block_ops_total 3
brewfs_writeback_upload_partial_tail_ops_total 1
brewfs_writeback_freeze_size_ops_total 7
brewfs_writeback_freeze_explicit_flush_ops_total 8
brewfs_writeback_freeze_auto_ops_total 9
brewfs_writeback_freeze_max_unflushed_ops_total 10
brewfs_writeback_freeze_commit_age_ops_total 11
EOF

cat >"$artifact_dir/diagnostics/stats-fio-test-after.txt" <<'EOF'
2026-06-14T00:00:01+00:00

brewfs_cache_hits_total 40
brewfs_cache_misses_total 10
brewfs_cache_requests_total 50
brewfs_fuse_read_bytes_total 31457280
brewfs_fuse_write_bytes_total 314572800
brewfs_s3_get_ops_total 7
brewfs_s3_get_bytes_total 73400320
brewfs_s3_get_lat_us_total 120000
brewfs_s3_put_ops_total 11
brewfs_s3_put_bytes_total 115343360
brewfs_s3_put_lat_us_total 230000
brewfs_writeback_dirty_bytes 1048576
brewfs_writeback_live_dirty_bytes 2097152
brewfs_writeback_live_slices 2
brewfs_writeback_stage_ops_total 14
brewfs_writeback_stage_bytes_total 146800640
brewfs_writeback_stage_lat_us_total 240000
brewfs_writeback_flush_wait_ops_total 4
brewfs_writeback_flush_wait_us_total 700000
brewfs_writeback_flush_wait_slices_total 8
brewfs_writeback_upload_batch_ops_total 15
brewfs_writeback_upload_batch_bytes_total 157286400
brewfs_writeback_upload_batch_blocks_total 30
brewfs_writeback_upload_batch_single_block_ops_total 6
brewfs_writeback_upload_batch_multi_block_ops_total 9
brewfs_writeback_upload_partial_tail_ops_total 4
brewfs_writeback_freeze_size_ops_total 17
brewfs_writeback_freeze_explicit_flush_ops_total 18
brewfs_writeback_freeze_auto_ops_total 19
brewfs_writeback_freeze_max_unflushed_ops_total 20
brewfs_writeback_freeze_commit_age_ops_total 21
EOF

# Source all functions without running main.
source <(sed '$d' "$REPO_DIR/docker/compose-xfstests/run_perf_in_container.sh")

artifact_dir="$tmpdir/artifact"
meta_backend=redis
generate_perf_report

report="$artifact_dir/report.md"
trap 'status=$?; if [[ $status -ne 0 && -f "$report" ]]; then cat "$report" >&2; fi; rm -rf "$tmpdir"' EXIT
grep -Fq '| fio-test | 85.7% (30/35) | 20.0 MiB | 200.0 MiB |' "$report"
grep -Fq 'GET=5, PUT=8' "$report"
grep -Fq 'stage=10 ops/100.0 MiB/200.0 ms' "$report"
grep -Fq 'flush_wait=3 ops/0.60s/6 slices' "$report"
grep -Fq 'upload_batch=10 avg=10.0 MiB blocks=2.00/batch partial_tail=0.30' "$report"
grep -Fq '| fio-test | 1.000 s | 1.000 s | 2.000 s | 0.00 MiB/s | 50.00 MiB/s | 50.00 MiB/s |' "$report"
grep -Fq $'fio-test\t1.000000\t1.000000\t2.000000\t0\t104857600\t0.000000\t50.000000\t50.000000' \
    "$artifact_dir/fully-drained-throughput.tsv"

juicefs_artifact_dir="$tmpdir/juicefs-artifact"
mkdir -p "$juicefs_artifact_dir/results"
cp "$artifact_dir/perf-summary.tsv" "$juicefs_artifact_dir/perf-summary.tsv"
cp "$artifact_dir/results/fio-test.json" "$juicefs_artifact_dir/results/fio-test.json"
cat >"$juicefs_artifact_dir/post-write-drain.tsv" <<'EOF'
tool	post_write_drain_s	stage_blocks	stage_bytes	uploading	put_bytes	get_bytes
fio-test	1	0	0	0	0	0
EOF

(
    source <(sed '$d' "$REPO_DIR/docker/compose-xfstests/run_juicefs_perf_in_container.sh")
    artifact_dir="$juicefs_artifact_dir"
    generate_perf_report
)

grep -Fq '| fio-test | 1.000 s | 1.000 s | 2.000 s | 0.00 MiB/s | 50.00 MiB/s | 50.00 MiB/s |' \
    "$juicefs_artifact_dir/report.md"
grep -Fq $'fio-test\t1.000000\t1.000000\t2.000000\t0\t104857600\t0.000000\t50.000000\t50.000000' \
    "$juicefs_artifact_dir/fully-drained-throughput.tsv"
