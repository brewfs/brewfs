#!/bin/bash
# 抓编译/测试结果（看日志里的 cargo 输出）
set -u
echo "=== 日志中的编译/测试关键字 ==="
grep -aE 'cargo (build|test|check|clippy)|Compiling brewfs|error\[E[0-9]|warning: unused|test result:|running [0-9]+ tests' /home/hxy/gclone.log 2>/dev/null | tail -12 | cut -c1-130 | sed 's/^/  /'
echo
echo "=== 日志增长 ==="
A=$(stat -c%s /home/hxy/gclone.log); sleep 20; B=$(stat -c%s /home/hxy/gclone.log)
echo "  $A -> $B B"
echo
echo "=== 是否有 target 目录在动 ==="
ls -lt /home/hxy/brewfs-target 2>/dev/null | head -3 | awk '{print "  "$6" "$7" "$8"  "$9}'
ls -lt /home/hxy/brewfs-main/target 2>/dev/null | head -3 | awk '{print "  "$6" "$7" "$8"  "$9}'
echo
echo "=== CLONE-RESULT ==="
ls -l /home/hxy/CLONE-RESULT.md 2>/dev/null | sed 's/^/  /' || echo "  尚未生成"
