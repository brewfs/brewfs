#!/bin/bash
# 直接在工作区里生成 10 万 4 KiB 文件（不依赖 c3 挂载）
set -u
K=/root/.ssh/ax-c1-20261106
cat > /tmp/gen100k.py <<'PY'
import os, time
DST = "/mnt/c4ws/base"
NDIRS, NFILES, SIZE = 103, 976, 4096
payload = bytes((i % 251) for i in range(SIZE))
t0 = time.time()
n = 0
for d in range(NDIRS):
    dp = os.path.join(DST, f"d{d:04d}")
    os.makedirs(dp, exist_ok=True)
    for i in range(NFILES):
        with open(os.path.join(dp, f"f{i:07d}"), "wb") as f:
            f.write(payload)
        n += 1
    if d % 10 == 0:
        el = time.time() - t0
        print(f"  d{d:04d}: {n} files, {el:.0f}s, {n/max(el,1e-9):.0f} files/s", flush=True)
el = time.time() - t0
print(f"  DONE files={n} elapsed={el:.1f}s rate={n/max(el,1e-9):.0f} files/s", flush=True)
PY
scp -q -o StrictHostKeyChecking=no -i $K /tmp/gen100k.py root@10.20.0.11:/root/gen100k.py
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "  挂载状态: $(mountpoint -q /mnt/c4ws && echo y || echo n)"
mkdir -p /mnt/c4ws/base
setsid nohup python3 /root/gen100k.py > /srv/ax-data/c4ws/gen100k.log 2>&1 &
sleep 90
tail -5 /srv/ax-data/c4ws/gen100k.log | sed 's/^/  /'
echo "  当前文件数: $(find /mnt/c4ws/base -type f 2>/dev/null | wc -l)"
EOS
