#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
pkill -f c3-100k-v2.sh 2>/dev/null || true
pkill -f 'cp -a' 2>/dev/null || true
sleep 1
echo "  c3 script: $(pgrep -fc c3-100k-v2.sh || echo 0)"
echo "  磁盘: $(df -h /srv/ax-data | tail -1 | awk '{print $3"/"$2}')"
EOS
