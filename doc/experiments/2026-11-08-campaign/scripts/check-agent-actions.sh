#!/bin/bash
# 看 agent 在 a-client 上实际跑什么（它自己的脚本/进程），判断它是否在推进 pilot
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== 正在跑的进程（smallfiles/brewfs/cp/脚本）==="
ps -eo pid,etime,pcpu,args 2>/dev/null | grep -E 'smallfiles|brewfs-patched|cp -a|materialize|pilot|segverify' | grep -v grep | head -8 | cut -c1-150 | sed 's/^/  /'
echo
echo "=== c3 目录里最新文件（agent 动作痕迹）==="
ls -lt /srv/ax-data/c3 | head -8 | awk '{print "  "$6" "$7" "$8"  "$9}'
echo
echo "=== pilot 目录内容 ==="
ls -lt /srv/ax-data/c3/pilot-brew-* 2>/dev/null | head -8 | sed 's/^/  /'
echo
echo "=== ws 目录 ==="
echo "  ws-brew 目录数: $(ls -1 /mnt/brewfs-c3/ws-brew 2>/dev/null | wc -l)"
echo "  ws-brew 文件数: $(find /mnt/brewfs-c3/ws-brew -maxdepth 2 -type f 2>/dev/null | wc -l)"
EOS
