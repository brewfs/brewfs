#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== BrewFS c3 配置里的元数据端点 ==="
grep -E 'pd_endpoints|endpoint|bucket|namespace|prefix' -A2 /etc/ax-c1/brewfs-c3.yaml | head -12 | sed 's/^/  /'
echo
echo "=== BrewFS c3 数据是否真的可读 ==="
echo "  d* 目录数: $(ls /mnt/brewfs-c3/base/small 2>/dev/null | wc -l)"
D=$(ls /mnt/brewfs-c3/base/small 2>/dev/null | head -1)
F=$(ls /mnt/brewfs-c3/base/small/$D 2>/dev/null | head -1)
echo "  抽样: $D/$F size=$(stat -c%s /mnt/brewfs-c3/base/small/$D/$F 2>/dev/null)"
timeout 10 head -c 64 /mnt/brewfs-c3/base/small/$D/$F >/dev/null 2>&1 && echo "  读取: OK" || echo "  读取: FAIL"
echo
echo "=== 挂载进程与启动时间 ==="
ps -o pid,lstart,args -p $(pgrep -f 'brewfs-patched mount' | head -1) 2>/dev/null | tail -1 | cut -c1-150 | sed 's/^/  /'
EOS
