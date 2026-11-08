#!/bin/bash
# 把私钥送到 client，使 client 能直接 ssh obj（磁盘护栏与 RustFS 重启需要）
set -u
K=/root/.ssh/ax-c1-20261106
CLIENT=10.20.2.161
ssh -o StrictHostKeyChecking=no -i $K root@$CLIENT 'mkdir -p /root/.ssh && chmod 700 /root/.ssh' 2>/dev/null
scp -q -o StrictHostKeyChecking=no -i $K $K root@$CLIENT:/root/.ssh/ax-c1-20261106 && echo "  key copied"
ssh -o StrictHostKeyChecking=no -i $K root@$CLIENT 'chmod 600 /root/.ssh/ax-c1-20261106; ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -i /root/.ssh/ax-c1-20261106 root@10.20.2.163 "echo obj-ssh-ok; df -BG --output=avail /srv/ax-data | tail -1"' 2>&1 | sed 's/^/  /'
