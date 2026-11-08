#!/bin/bash
set -u
echo "  log: $(stat -c%s /home/hxy/build-xattr-brewfs.log 2>/dev/null || echo 0) bytes"
tail -2 /home/hxy/build-xattr-brewfs.log 2>/dev/null | cut -c1-100 | sed 's/^/  /'
echo "  cargo=$(pgrep -fc 'cargo build' || echo 0) rustc=$(pgrep -fc rustc || echo 0)"
echo "  marker: $(cat /home/hxy/build-xattr-brewfs.marker 2>/dev/null || echo none)"
ls -lh /home/hxy/brewfs-main/target/release/brewfs 2>/dev/null | awk '{print "  bin: "$5, $6, $7, $8}' || echo "  bin: 未产出"
