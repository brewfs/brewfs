#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== 历史 C2 运行记录（c2 目录）==="
ls -1 /srv/ax-data/c2 2>/dev/null | head -12 | sed 's/^/  /'
echo
echo "=== 找 C2 的命令行（脚本/日志）==="
grep -rl 'full-fuse-replay' /srv/ax-data/src /srv/ax-data/c2 /srv/ax-data/c2b /root/*.sh 2>/dev/null | head -8 | sed 's/^/  /'
echo
echo "=== 命令样例 ==="
grep -rh 'full-fuse-replay' /srv/ax-data/src /srv/ax-data/c2 /srv/ax-data/c2b /root/*.sh 2>/dev/null | head -6 | cut -c1-200 | sed 's/^/  /'
echo
echo "=== pytest trace 目录内容 ==="
ls -1 /srv/ax-data/traces/extracted/pytest-full-02/capture 2>/dev/null | head -5 | sed 's/^/  capture: /'
ls -1 /srv/ax-data/traces/extracted/pytest-full-02/backing 2>/dev/null | head -5 | sed 's/^/  backing: /'
EOS
