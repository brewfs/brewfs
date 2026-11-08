#!/bin/bash
set -u
cd /home/hxy/brewfs-main || exit 1
echo "=== 挂载点里的虚拟 stats 文件 ==="
grep -rn '"\.stats"\|\.stats\b' src/vfs/*.rs src/fuse/*.rs 2>/dev/null | head -10 | cut -c1-150 | sed 's/^/  /'
echo
echo "=== 该文件的读取路径与内容格式 ==="
grep -rn 'stats' src/vfs/mod.rs 2>/dev/null | head -8 | cut -c1-140 | sed 's/^/  /'
echo
echo "=== 序列化结构（找 to_json / serde on snapshot）==="
grep -n 'derive(.*Serialize' src/vfs/stats.rs 2>/dev/null | head -6 | cut -c1-120 | sed 's/^/  /'
grep -n 'pub struct RequestObservationSnapshot' -A 20 src/vfs/stats.rs 2>/dev/null | head -24 | cut -c1-120 | sed 's/^/  /'
