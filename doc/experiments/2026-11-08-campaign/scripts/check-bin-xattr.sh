#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
B=/srv/ax-data/src/brewfs-new/target/release/brewfs
echo "=== 二进制是否含新 xattr 键前缀 ==="
strings -a $B 2>/dev/null | grep -c '^xattr/$' | sed 's/^/  "xattr/" 出现次数: /'
strings -a $B 2>/dev/null | grep -c 'invalid setxattr flags' | sed 's/^/  "invalid setxattr flags": /'
ls -l $B | awk '{print "  bin mtime: "$6, $7, $8}'
echo
echo "=== 新 bucket 挂载失败原因（前台 8 秒）==="
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
timeout 8 $B mount --privileged \
  --meta-backend tikv --meta-tikv-pd-endpoints http://10.20.0.12:2379 \
  --data-backend s3 --s3-bucket c1xattr-20261108 --s3-endpoint http://10.20.0.13:9000 \
  --s3-region us-east-1 --s3-force-path-style true /mnt/bfs-xattr2 2>&1 | tail -8 | cut -c1-150 | sed 's/^/  /'
EOS
