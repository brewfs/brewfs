#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
cd /srv/ax-data/src/brewfs-new || exit 1
echo "=== capabilities 里 xattr 的设置 ==="
grep -n -A12 'fn capabilities' src/meta/stores/tikv/mod.rs | head -20 | sed 's/^/  /'
echo
echo "=== xattr 方法与 flag ==="
grep -n 'fn set_xattr\|fn get_xattr\|fn list_xattr\|fn remove_xattr\|xattr' src/meta/stores/tikv/mod.rs | head -20 | cut -c1-120 | sed 's/^/  /'
EOS
