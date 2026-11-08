#!/bin/bash
# 用 sed 给 systemd-run 加 --working-directory
set -u
F=/home/hxy/run-xattr-final3.sh
sed -i 's#--uid=hxy --gid=hxy \\#--uid=hxy --gid=hxy --working-directory=/home/hxy/brewfs-main \\#g' "$F"
echo "  匹配行数: $(grep -c 'working-directory=/home/hxy/brewfs-main' "$F")"
echo "--- 检查两处 systemd-run ---"
grep -n 'systemd-run' "$F" | sed 's/^/  /'
grep -n 'working-directory' "$F" | sed 's/^/  /'
