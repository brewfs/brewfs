#!/bin/bash
set -u
cd /home/hxy/brewfs-main || exit 1
echo "=== trace 记录怎么落盘/导出（找 emit/write/dump）==="
grep -rn 'RequestTraceRecord' src/ 2>/dev/null | grep -vE '^src/vfs/stats.rs' | head -8 | cut -c1-140 | sed 's/^/  /'
echo
echo "=== stats.rs 里的导出函数 ==="
grep -n 'pub fn .*trace\|fn .*dump\|fn .*snapshot\|fn .*export' src/vfs/stats.rs 2>/dev/null | head -12 | cut -c1-130 | sed 's/^/  /'
echo
echo "=== 是否有 HTTP 端点/命令行子命令导出 ==="
grep -rn 'request_trace\|trace_dump\|/debug/trace\|stats.*trace' src/cli src/gateway 2>/dev/null | head -8 | cut -c1-140 | sed 's/^/  /'
echo
echo "=== 分段枚举全名（十段）==="
awk '/enum RequestSegment/,/^}/' src/vfs/stats.rs 2>/dev/null | grep -oE '^\s+[A-Z][A-Za-z]+,' | tr -d ' ,' | head -20 | sed 's/^/  /'
