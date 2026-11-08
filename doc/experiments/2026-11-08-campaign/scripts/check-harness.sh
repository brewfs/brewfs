#!/bin/bash
# 检查 harness 可用性（E0 复测前置）
set -u
export PATH=/usr/local/bin:$PATH
D=/mnt/brewfs-data/home/hxy/regress-20261104
echo "=== $(date '+%H:%M:%S') harness 可用性 ==="
echo "--- 目录 ---"
ls "$D" 2>/dev/null | head -10 | sed 's/^/  /'
echo
echo "--- harness 脚本 ---"
for f in "$D"/harness/*.py "$D"/harness/*.sh; do
  [ -f "$f" ] && printf "  %-40s %s B\n" "$(basename "$f")" "$(stat -c%s "$f")"
done
echo
echo "--- 已有三臂二进制 ---"
for arm in main parent407 parent409; do
  f="$D/bin/$arm/brewfs"
  [ -f "$f" ] && printf "  %-12s %6.0f MB\n" "$arm" "$(awk "BEGIN{print $(stat -c%s "$f")/1048576}")" || echo "  $arm 缺失"
done
echo
echo "--- protocol.json 里的场景定义 ---"
python3 -c "
import json
d=json.load(open('$D/protocol.json'))
print('  场景数:', len(d.get('scenes',[])) or len(d.get('cases',[])))
for s in (d.get('scenes') or d.get('cases') or [])[:12]:
    print('   -', s.get('id') or s.get('name'))
" 2>/dev/null | head -16 || echo "  （解析失败）"
echo
echo "--- 归档的 provenance ---"
ls "$D"/artifacts 2>/dev/null | head -4 | sed 's/^/  /'
echo "  已完成测量轮次: $(ls -d "$D"/artifacts/*/ 2>/dev/null | wc -l)"
