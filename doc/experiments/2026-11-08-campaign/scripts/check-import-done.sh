#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== 1. 目录构成 ==="
echo "  总目录: $(ls -1 /mnt/brewfs-c3/base 2>/dev/null | wc -l)"
echo "  匹配 d0NNN 的: $(ls -1 /mnt/brewfs-c3/base 2>/dev/null | grep -cE '^d[0-9]{4}$')"
echo "  不匹配的: $(ls -1 /mnt/brewfs-c3/base 2>/dev/null | grep -vE '^d[0-9]{4}$' | tr '\n' ' ')"
echo
echo "=== 2. 抽查 3 个目录的文件数 ==="
for d in d0000 d0500 d1023; do
  echo "  $d: $(find /mnt/brewfs-c3/base/$d -maxdepth 1 -type f 2>/dev/null | wc -l) 文件"
done
echo
echo "=== 3. verify-base 是否在跑/已产出 ==="
echo "  进程: $(pgrep -fc 'smallfiles verify-base' || echo 0)"
ls -d /srv/ax-data/c3/verify-base-brew-* 2>/dev/null | tail -2 | sed 's/^/  目录: /'
for v in $(ls -d /srv/ax-data/c3/verify-base-brew-* 2>/dev/null | tail -1); do
  echo "  内容: $(ls -A $v 2>/dev/null | tr '\n' ' ')"
  [ -f $v/result.json ] && echo "  ✅ result: $(head -c 300 $v/result.json)"
done
echo
echo "=== 4. 导入 CSV ==="
cat /srv/ax-data/c3/import-brew-final.csv 2>/dev/null | sed 's/^/  /'
echo
echo "=== 5. 事务检查 ==="
L=$(ls -t /srv/ax-data/c3/*mount*.log 2>/dev/null | head -1)
echo "  日志: $L"
echo "  txn drop: $(grep -ac 'Dropping an active transaction' $L 2>/dev/null || echo 0)"
echo "  credential: $(grep -ac 'credential provider' $L 2>/dev/null || echo 0)"
EOS
