#!/bin/bash
set -u
cd /home/hxy/brewfs-main || exit 1
echo "=== trace 是怎么输出的 ==="
grep -n 'BREWFS_REQUEST_TRACE\|trace_file\|trace_path\|jsonl' src/vfs/stats.rs 2>/dev/null | head -12 | cut -c1-140 | sed 's/^/  /'
echo
echo "=== 分段字段名（找 struct 定义）==="
grep -n 'pub struct .*Trace\|pub struct .*Segment\|queue_wait' src/vfs/stats.rs 2>/dev/null | head -10 | cut -c1-140 | sed 's/^/  /'
echo
echo "=== 若有 struct，打印其字段（前 40 行）==="
awk '/pub struct .*(Trace|Segment)/,/^}/' src/vfs/stats.rs 2>/dev/null | head -40 | cut -c1-120 | sed 's/^/  /'
echo
echo "=== 环境变量清单 ==="
grep -rhoE 'BREWFS_[A-Z_]+' src/vfs/stats.rs 2>/dev/null | sort -u | head -12 | sed 's/^/  /'
