#!/bin/bash
set -u
cd /home/hxy/brewfs-main || exit 1
echo "=== 1. regress 编译是否结束（决定我能否跑测试）==="
echo "  rustc=$(pgrep -cx rustc || echo 0) cargo=$(pgrep -cx cargo || echo 0)"
ls /mnt/brewfs-data/home/hxy/regress-20261104/build/complete 2>/dev/null && echo "  build/complete 存在 ⇒ 编译完成" || echo "  build 未完成"
ls -l --time-style=+%H:%M /mnt/brewfs-data/home/hxy/regress-20261104/bin/ 2>/dev/null | sed 's/^/  /'
echo
echo "=== 2. 仓库里的 TiKV 编排 ==="
find . -maxdepth 4 -iname '*tikv*compose*' -o -maxdepth 4 -iname 'compose*tikv*' 2>/dev/null | head -5 | sed 's/^/  /'
ls docker/ 2>/dev/null | head -12 | sed 's/^/  /'
echo
echo "=== 3. 是否有现成的 tikv 启动脚本 ==="
timeout 60 rg -l --no-messages 'tikv-server|pd-server' --glob 'docker/**' --glob 'scripts/**' 2>/dev/null | head -8 | sed 's/^/  /'
echo
echo "=== 4. docker 可用性 ==="
docker --version 2>&1 | head -1 | sed 's/^/  /'
docker images --format '  {{.Repository}}:{{.Tag}}' 2>/dev/null | grep -iE 'tikv|pingcap' | head -5
echo
echo "=== 5. 全仓有多少 ignored 测试（判断 --include-ignored 风险）==="
timeout 60 grep -rn --include='*.rs' '#\[ignore' src/ tests/ 2>/dev/null | wc -l | sed 's/^/  ignore 标记数: /'
timeout 60 grep -rn --include='*.rs' '#\[ignore' src/ tests/ 2>/dev/null | grep -c 'TIKV' | sed 's/^/  其中需要 TIKV 环境: /'
