#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== cargo/rustc 进程 ==="
ps -eo pid,etime,pcpu,rss,args --sort=-pcpu | grep -E 'cargo build|rustc --crate-name brewfs' | grep -v grep | head -4 | cut -c1-130 | sed 's/^/  /'
echo
echo "=== 是否在编旧代码（看 build 日志）==="
tail -4 /root/build-xattr.log 2>/dev/null | cut -c1-130 | sed 's/^/  /'
EOS
