#!/bin/bash
# 归档 C1-v2 数据到 jump，再拉回本地；随后可删除 b-* 三台
set -u
K=/root/.ssh/ax-c1-20261106
echo "=== 打包 ==="
ssh -o StrictHostKeyChecking=no -i $K root@10.20.2.161 'cd /srv/ax-data && tar -czf /root/c1v2-data.tar.gz c1v2 && ls -lh /root/c1v2-data.tar.gz | cut -c25-50'
echo "=== 拉到 jump ==="
scp -q -o StrictHostKeyChecking=no -i $K root@10.20.2.161:/root/c1v2-data.tar.gz /root/c1v2-data.tar.gz && ls -lh /root/c1v2-data.tar.gz | cut -c25-50
