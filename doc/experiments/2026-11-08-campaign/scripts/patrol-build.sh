#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -o StrictHostKeyChecking=no -i $K root@10.20.2.164 'bash -s' <<'EOS' 2>&1
set -u
echo "  log: $(stat -c%s /root/build-seal.log 2>/dev/null || echo 0) B"
tail -2 /root/build-seal.log 2>/dev/null | cut -c1-110 | sed 's/^/  /'
echo "  cargo=$(pgrep -fc 'cargo build' || echo 0) rustc=$(pgrep -fc rustc || echo 0)"
free -g | head -2 | tail -1 | sed 's/^/  /'
df -h /srv/ax-data | tail -1 | sed 's/^/  /'
grep -c BUILD_RC /root/build-seal.log 2>/dev/null || echo "  未完成"
EOS
