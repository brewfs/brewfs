#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
cd /srv/ax-data/src/brewfs-new || exit 1
echo "  cargo: $(pgrep -fc 'cargo build' || echo 0)  rustc: $(pgrep -fc rustc || echo 0)"
echo "  BUILD_RC: $(grep -a BUILD_RC /root/build-xattr.log 2>/dev/null | tail -1 || echo 未完成)"
ls -lh target/release/brewfs 2>/dev/null | awk '{print "  bin: "$5, $6, $7, $8}' || echo "  bin: 未产出"
tail -3 /root/build-xattr.log 2>/dev/null | cut -c1-130 | sed 's/^/  /'
EOS
