#!/bin/bash
set -u
pkill -f build-xattr4.sh 2>/dev/null || true
pkill -f 'cargo build' 2>/dev/null || true
sleep 2
echo "  cargo: $(pgrep -fc 'cargo build' || echo 0)"
echo "  marker: $(cat /root/build-xattr4.marker 2>/dev/null || echo none)"
echo "  bin: $(ls -l /srv/ax-data/src/brewfs-new/target/release/brewfs 2>/dev/null | awk '{print $6, $7, $8}')"
echo "  磁盘: $(df -h /srv/ax-data | tail -1 | awk '{print $3"/"$2" ("$5")"}')"
