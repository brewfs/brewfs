#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
B=/srv/ax-data/src/brewfs-new/target/release/brewfs
echo "=== mount --help 里 xattr 相关 ==="
$B mount --help 2>&1 | grep -i -B1 -A2 'xattr' | head -16 | cut -c1-140 | sed 's/^/  /'
echo "=== 是否有 --enable-xattr ==="
$B mount --help 2>&1 | grep -c 'enable-xattr' | sed 's/^/  count: /'
echo "=== VFS 层是否检查挂载选项 ==="
cd /srv/ax-data/src/brewfs-new || exit 1
grep -rn 'xattr' src/vfs/config.rs src/vfs/fs/mod.rs 2>/dev/null | head -8 | cut -c1-130 | sed 's/^/  /'
EOS
