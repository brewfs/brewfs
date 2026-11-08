#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== 1. 小文件/小写入探针（限时）==="
D=/mnt/juicefs-c1/c1-data/probe
mkdir -p $D 2>&1 | head -2
timeout 30 bash -c "echo hello > $D/a.txt" && echo "  write small OK: $(cat $D/a.txt 2>/dev/null)" || echo "  write small TIMEOUT/FAIL"
timeout 60 dd if=/dev/zero of=$D/b bs=1M count=64 conv=fsync 2>&1 | tail -1 | sed 's/^/  dd64M: /' || echo "  dd64M TIMEOUT"
rm -rf $D 2>/dev/null

echo
echo "=== 2. JuiceFS 进程与日志 ==="
pgrep -af 'juicefs mount' | head -3 | cut -c1-110 | sed 's/^/  /'
ls -t /var/log/juicefs* /root/.juicefs/*.log 2>/dev/null | head -2 | sed 's/^/  log: /'

echo
echo "=== 3. 内核 FUSE 错误 ==="
dmesg 2>/dev/null | tail -5 | cut -c1-140 | sed 's/^/  /'

echo
echo "=== 4. 该卷的对象桶用量（RustFS 侧不可直连，用 juicefs stats 替代）==="
timeout 10 /srv/ax-data/artifacts/bin/juicefs stats /mnt/juicefs-c1 2>&1 | head -8 | sed 's/^/  /'
EOS
