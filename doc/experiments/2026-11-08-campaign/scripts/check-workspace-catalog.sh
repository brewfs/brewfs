#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
B=/srv/ax-data/artifacts/bin/brewfs-patched
echo "=== workspace 子命令 ==="
$B workspace --help 2>&1 | sed -n '/^Commands:/,/^$/p' | head -20 | sed 's/^/  /'
echo
echo "=== 是否已有 workspace catalog（sqlite 文件 / 目录）==="
find /srv/ax-data -maxdepth 3 -name '*.sqlite*' -o -maxdepth 3 -name '*workspace*' 2>/dev/null | head -10 | sed 's/^/  /'
echo
echo "=== C3 挂载配置是什么卷类型 ==="
grep -E 'volume_format|format|workspace' /etc/ax-c1/brewfs-c3.yaml 2>/dev/null | head -5 | sed 's/^/  /'
echo "  c1 配置:"; grep -E 'volume_format|workspace' /etc/ax-c1/brewfs-c1.yaml 2>/dev/null | head -3 | sed 's/^/    /'
EOS
