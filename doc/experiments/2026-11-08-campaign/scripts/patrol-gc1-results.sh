#!/bin/bash
# 巡检 gc1 的 C2 replay 结果（sklearn / sympy）
set -u
K=/root/.ssh/ax-c1-20261106
echo "=== tmux ==="
tmux ls 2>/dev/null | sed 's/^/  /'
A=$(stat -c%s /root/gc1.log 2>/dev/null || echo 0); sleep 6; B=$(stat -c%s /root/gc1.log 2>/dev/null || echo 0)
echo "  gc1 日志: $A -> $B B（$( [ "${B:-0}" -gt "${A:-0}" ] && echo 推进 || echo 静止)）"

echo
echo "=== c2 results ==="
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
ls -lh /srv/ax-data/c1c2/c2/results 2>/dev/null | sed 's/^/  /'
echo
for f in /srv/ax-data/c1c2/c2/results/*.json; do
  [ -f "$f" ] || continue
  python3 - "$f" <<'PY'
import json, sys
p = sys.argv[1]
try:
    d = json.load(open(p))
    print(f"  {p.split('/')[-1]}: completed={d.get('completed')} ops={d.get('operations')} "
          f"reads={d.get('actual_read_bytes')} writes={d.get('actual_write_bytes')} handles={d.get('remaining_handles')}")
except Exception as e:
    print(f"  {p.split('/')[-1]}: 解析失败 {e}")
PY
done
echo
echo "=== 正在跑 ==="
pgrep -af 'c2-replay-case|full-fuse-replay' | head -3 | cut -c1-120 | sed 's/^/  /'
echo
echo "=== 报告尾 6 ==="
tail -6 /srv/ax-data/c1c2/AGENT-REPORT-C1C2.md 2>/dev/null | cut -c1-140 | sed 's/^/  /'
EOS
