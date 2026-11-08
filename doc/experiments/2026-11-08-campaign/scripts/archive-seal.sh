#!/bin/bash
# 归档 build 机上的 seal 证据与构建日志到 jump
set -u
K=/root/.ssh/ax-c1-20261106
B=10.20.2.164
ssh -o StrictHostKeyChecking=no -i $K root@$B 'bash -s' <<'EOS' 2>&1 | sed 's/^/  /'
set -u
cd /srv/ax-data || exit 1
# 只留文本证据，去掉大文件
mkdir -p /root/archive
cp -a sealtest/meta.db /root/archive/ 2>/dev/null
cp -a sealtest/fork.json /root/archive/ 2>/dev/null
cp -a sealtest/data.yaml /root/archive/ 2>/dev/null
for f in sealtest/mount*.log sealtest/m-*.log; do [ -f "$f" ] && cp -a "$f" /root/archive/ 2>/dev/null; done
cp -a /root/build-seal3.log /root/archive/ 2>/dev/null
tar -czf /root/seal-evidence.tar.gz -C /root archive && ls -lh /root/seal-evidence.tar.gz | cut -c25-50
EOS
scp -q -o StrictHostKeyChecking=no -i $K root@$B:/root/seal-evidence.tar.gz /root/seal-evidence.tar.gz && ls -lh /root/seal-evidence.tar.gz | cut -c25-50
