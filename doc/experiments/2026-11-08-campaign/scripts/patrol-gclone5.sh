#!/bin/bash
# 看 release 编译是否在进行 / 是否有二进制产出 / bench 是否跑过
set -u
echo "=== release 目录 ==="
ls -lt /home/hxy/brewfs-main/target/release/ 2>/dev/null | head -5 | awk '{print "  "$6" "$7" "$8"  "$9}'
echo "  二进制: $(ls -lh /home/hxy/brewfs-main/target/release/brewfs 2>/dev/null | awk '{print $5, $6, $7, $8}')"
echo
echo "=== cargo/rustc 进程 ==="
pgrep -af 'cargo|rustc' 2>/dev/null | head -3 | cut -c1-110 | sed 's/^/  /' || echo "  无编译进程"
echo
echo "=== 进程/日志 ==="
echo "  codex: $(pgrep -fc 'codex exec' || echo 0)"
A=$(stat -c%s /home/hxy/gclone.log); sleep 15; B=$(stat -c%s /home/hxy/gclone.log)
echo "  日志: $A -> $B"
echo
echo "=== 日志尾 8 ==="
tail -8 /home/hxy/gclone.log 2>/dev/null | cut -c1-120 | sed 's/^/  /'
