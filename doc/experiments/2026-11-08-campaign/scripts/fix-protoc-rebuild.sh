#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -o StrictHostKeyChecking=no -i $K root@10.20.2.164 'bash -s' <<'EOS' 2>&1 | sed 's/^/  /'
set -u
export DEBIAN_FRONTEND=noninteractive
echo "--- 装 protoc ---"
apt-get install -y -qq protobuf-compiler >/dev/null 2>&1 && echo "  protoc installed" || echo "  protoc FAILED"
protoc --version 2>/dev/null | sed 's/^/  /'
echo "--- 重新编译 ---"
export PATH=/root/.cargo/bin:$PATH
cd /srv/ax-data/src || exit 1
setsid nohup /root/build-seal.sh </dev/null >/dev/null 2>&1 &
sleep 25
echo "  cargo=$(pgrep -fc 'cargo build' || echo 0) rustc=$(pgrep -fc rustc || echo 0)"
tail -2 /root/build-seal.log 2>/dev/null | cut -c1-110
EOS
