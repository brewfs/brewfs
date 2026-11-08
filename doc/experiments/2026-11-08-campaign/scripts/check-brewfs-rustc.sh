#!/bin/bash
set -u
echo "=== rustc 详情 ==="
ps -eo pid,etime,pcpu,rss,args --sort=-pcpu | grep 'rustc --crate-name brewfs' | grep -v grep | head -2 | cut -c1-120 | sed 's/^/  /'
echo "=== 全部 cargo/rustc ==="
ps -eo pid,etime,pcpu,args | grep -E 'cargo build|rustc' | grep -v grep | head -4 | cut -c1-110 | sed 's/^/  /'
echo "=== 磁盘 ==="
df -h /home/hxy | tail -1 | sed 's/^/  /'
echo "=== target/release 更新 ==="
ls -lt /home/hxy/brewfs-main/target/release/deps 2>/dev/null | head -2 | awk '{print "  "$6" "$7" "$8}'
