#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
cd /srv/ax-data/src/brewfs-new || exit 1
echo "  cargo: $(pgrep -fc 'cargo build' || echo 0)  rustc: $(pgrep -fc rustc || echo 0)"
echo "  BUILD_RC: $(grep -a BUILD_RC /root/build-xattr2.log 2>/dev/null | tail -1 || echo 未完成)"
ls -l target/release/brewfs | awk '{print "  bin: "$6, $7, $8}'
strings -a target/release/brewfs 2>/dev/null | grep -c 'invalid setxattr flags' | sed 's/^/  binary 含新代码: /'
EOS
