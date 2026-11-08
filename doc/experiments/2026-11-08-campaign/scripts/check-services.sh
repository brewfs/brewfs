#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
echo "=== a-meta 服务 ==="
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.12 'bash -s' <<'EOS' 2>&1
set -u
echo "  pd: $(pgrep -af pd-server | head -1 | cut -c1-60)"
echo "  tikv: $(pgrep -af tikv-server | head -1 | cut -c1-60)"
echo "  端口: pd=$(timeout 3 bash -c '</dev/tcp/127.0.0.1/2379' 2>/dev/null && echo up || echo down) tikv=$(timeout 3 bash -c '</dev/tcp/127.0.0.1/20160' 2>/dev/null && echo up || echo down)"
echo "  stores: $(curl -s --max-time 5 http://127.0.0.1:2379/pd/api/v1/stores | head -c 200)"
EOS
echo
echo "=== a-client 挂载点 ==="
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "  /mnt 下: $(ls /mnt 2>/dev/null | tr '\n' ' ')"
echo "  brewfs-c3/base 存在: $([ -d /mnt/brewfs-c3/base ] && echo y || echo n)"
echo "  juicefs-c3 存在: $([ -d /mnt/juicefs-c3 ] && echo y || echo n)"
echo "  juicefs-c2 存在: $([ -d /mnt/juicefs-c2 ] && echo y || echo n)"
echo "  配置: $(ls /etc/ax-c1/*.yaml 2>/dev/null | tr '\n' ' ')"
EOS
