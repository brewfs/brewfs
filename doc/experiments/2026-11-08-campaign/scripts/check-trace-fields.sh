#!/bin/bash
set -u
cd /home/hxy/brewfs-main || exit 1
echo "=== 请求级分段计时的能力边界（main 现状）==="
sed -n '455,490p' src/vfs/stats.rs | sed 's/^/  /'
echo
echo "=== 是否有十段字段（queue_wait/metadata_rpc/object_put 等）==="
for f in queue_wait file_lock chunk_lock dirty_merge flush metadata_rpc metadata_commit object_put_queue object_put reply; do
  N=$(grep -rn "$f" src/vfs/stats.rs src/observability/ 2>/dev/null | wc -l)
  printf "  %-20s %s\n" "$f" "$N"
done
echo
echo "=== observability 模块提供的指标 ==="
grep -n 'pub struct\|pub fn' src/observability/mod.rs 2>/dev/null | head -12 | cut -c1-110 | sed 's/^/  /'
