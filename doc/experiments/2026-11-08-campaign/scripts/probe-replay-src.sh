#!/bin/bash
set -u
echo "=== 工具源码 ==="
ls /srv/ax-data/src/tools/full-fuse-replay/src/ 2>/dev/null | sed 's/^/  /'
wc -l /srv/ax-data/src/tools/full-fuse-replay/src/*.rs 2>/dev/null | tail -4 | sed 's/^/  /'
echo "=== xattr 相关代码位置 ==="
grep -rn 'getxattr\|GetXattr' /srv/ax-data/src/tools/full-fuse-replay/src/*.rs 2>/dev/null | head -8 | cut -c1-140 | sed 's/^/  /'
echo "=== 是否有 skip / ignore 选项 ==="
grep -rn 'skip\|ignore' /srv/ax-data/src/tools/full-fuse-replay/src/main.rs 2>/dev/null | head -6 | cut -c1-120 | sed 's/^/  /'
