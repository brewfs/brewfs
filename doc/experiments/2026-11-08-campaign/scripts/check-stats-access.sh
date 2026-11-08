#!/bin/bash
set -u
cd /home/hxy/brewfs-main || exit 1
echo "=== tools/stats 能查什么 ==="
ls tools/stats/src 2>/dev/null | head -6 | sed 's/^/  /'
grep -rn 'request_observation\|segments\|queue_wait_ns' tools/stats/src/*.rs 2>/dev/null | head -8 | cut -c1-130 | sed 's/^/  /'
echo
echo "=== 是否有 stats socket / 文件输出 ==="
grep -rn 'stats.*sock\|unix.*stats\|stats_path\|BREWFS_STATS' src/ 2>/dev/null | head -8 | cut -c1-140 | sed 's/^/  /'
echo
echo "=== FsStatsSnapshot 的暴露方式（fuse 内 / gateway）==="
grep -rn 'fn stats\|/stats\|stats_handler' src/gateway src/cli 2>/dev/null | head -8 | cut -c1-130 | sed 's/^/  /'
echo
echo "=== 十段枚举报错（换个正则）==="
sed -n '/pub enum RequestSegment/,/^}/p' src/vfs/stats.rs 2>/dev/null | head -30 | sed 's/^/  /'
