#!/bin/bash
set -u
cd /home/hxy/brewfs-main || exit 1
echo "=== 1. main 是否支持请求级分段计时 ==="
grep -rn 'BREWFS_REQUEST_TRACE' src/ 2>/dev/null | head -6 | cut -c1-130 | sed 's/^/  /'
echo "  出现次数: $(grep -rn 'BREWFS_REQUEST_TRACE' src/ 2>/dev/null | wc -l)"
echo
echo "=== 2. 是否有 observe / trace 模块 ==="
ls src/observability* src/observe* 2>/dev/null | sed 's/^/  /'
grep -rn 'metadata_rpc\|queue_wait\|dirty_merge' src/observability* 2>/dev/null | head -5 | cut -c1-120 | sed 's/^/  /'
echo
echo "=== 3. #388 状态（十段计时的来源 PR）==="
export PATH=/usr/local/bin:$PATH
gh pr view 388 -R brewfs/brewfs --json number,state,title,mergedAt --jq '"  #\(.number) state=\(.state) merged=\(.mergedAt // "未合") \(.title[0:46])"' 2>/dev/null
echo
echo "=== 4. 当前 main 分支 ==="
git log --oneline origin/main -1 | sed 's/^/  /'
