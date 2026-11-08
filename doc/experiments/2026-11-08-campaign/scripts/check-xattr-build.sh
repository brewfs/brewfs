#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
cd /srv/ax-data/src/brewfs-new || exit 1
echo "  xattr 方法数: $(grep -c 'fn set_xattr\|fn get_xattr\|fn list_xattr\|fn remove_xattr' src/meta/stores/tikv/mod.rs)"
echo "  xattr_capable: $(grep -c 'xattr' src/meta/stores/tikv/mod.rs)"
echo "  cargo: $(pgrep -fc 'cargo build' || echo 0)  rustc: $(pgrep -fc rustc || echo 0)"
tail -3 /root/build-xattr.log 2>/dev/null | cut -c1-120 | sed 's/^/  /'
EOS
