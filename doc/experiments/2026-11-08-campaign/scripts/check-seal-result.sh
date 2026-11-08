#!/bin/bash
# 看 seal E2E 后半段（fork → 可见性 → 隔离）的结果
set -u
K=/root/.ssh/ax-c1-20261106
ssh -o StrictHostKeyChecking=no -i $K root@10.20.2.164 'bash -s' <<'EOS' 2>&1
set -u
OUT=/srv/ax-data/sealtest
echo "=== fork.json ==="
tail -12 $OUT/fork.json 2>/dev/null | cut -c1-160 | sed 's/^/  /'
echo "=== 挂载状态 ==="
echo "  mount=$(mountpoint -q $OUT/mnt && echo y || echo n)"
echo "=== fork 目录内容 ==="
ls $OUT/mnt 2>/dev/null | head -5 | sed 's/^/  /'
echo "  sealbase 文件数=$(ls $OUT/mnt/sealbase 2>/dev/null | wc -l)"
echo "=== 隔离检查 ==="
[ -f $OUT/mnt/FORK_ONLY.txt ] && echo "  (当前挂的是 fork，含 FORK_ONLY)" || echo "  当前挂载无 FORK_ONLY.txt"
echo "=== workdir 列表 ==="
ls -la $OUT | head -10 | sed 's/^/  /'
EOS
