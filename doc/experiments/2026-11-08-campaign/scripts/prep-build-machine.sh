#!/bin/bash
# 构建机准备：格式化盘、装依赖、装 rust、取源码+seal 补丁、启动编译
set -u
K=/root/.ssh/ax-c1-20261106
B=10.20.2.164
echo "=== 可达性 ==="
timeout 25 ssh -o StrictHostKeyChecking=no -o ConnectTimeout=15 -i $K root@$B 'hostname; nproc; free -g | head -2 | tail -1' 2>&1 | sed 's/^/  /'
echo "=== 传源码 + 补丁 + 编脚本 ==="
scp -q -o StrictHostKeyChecking=no -i $K /root/brewfs-src.tar.gz root@$B:/root/ && echo "  src ok"
scp -q -o StrictHostKeyChecking=no -i $K /root/seal.patch root@$B:/root/ && echo "  patch ok"
ssh -o StrictHostKeyChecking=no -i $K root@$B 'bash -s' <<'EOS' 2>&1 | sed 's/^/  /'
set -u
echo "--- 磁盘 ---"
mkdir -p /srv/ax-data
if ! mountpoint -q /srv/ax-data; then
  blkid /dev/vdb >/dev/null 2>&1 || mkfs.ext4 -F -m 1 /dev/vdb >/dev/null 2>&1
  mount /dev/vdb /srv/ax-data && echo "  mounted"
  grep -q '/srv/ax-data' /etc/fstab || echo '/dev/vdb /srv/ax-data ext4 defaults,nofail 0 2' >> /etc/fstab
fi
df -h /srv/ax-data | tail -1
echo "--- 系统依赖 ---"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq >/dev/null 2>&1
apt-get install -y -qq build-essential pkg-config libssl-dev libfuse3-dev fuse3 clang curl git >/dev/null 2>&1 && echo "  deps ok"
echo "--- rust ---"
if ! command -v cargo >/dev/null; then
  curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable >/dev/null 2>&1 && echo "  rust installed"
fi
export PATH=$HOME/.cargo/bin:$PATH
cargo --version 2>/dev/null | sed 's/^/  /'
echo "--- 源码 ---"
mkdir -p /srv/ax-data/src && cd /srv/ax-data/src
rm -rf brewfs && tar -xzf /root/brewfs-src.tar.gz 2>/dev/null
ls -d /srv/ax-data/src/* | head -3
EOS
