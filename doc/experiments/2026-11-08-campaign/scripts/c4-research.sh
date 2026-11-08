#!/bin/bash
# C4 研究：seal / packed manifest 路线（只读代码，不跑实验）
set -u
cd /home/hxy/brewfs-main || exit 1
echo "=== 1. native ingest 是否有 CLI 入口 ==="
grep -rn 'native_base::ingest\|ingest::' src/cli.rs src/config.rs 2>/dev/null | head -8 | cut -c1-140 | sed 's/^/  /'
echo
echo "=== 2. packed manifest 生成者 ==="
grep -rn 'pub fn .*manifest\|fn write_manifest\|manifest_key' src/native_base/ingest/*.rs 2>/dev/null | head -10 | cut -c1-140 | sed 's/^/  /'
echo
echo "=== 3. workspace 命令里与 packed 相关的子命令 ==="
grep -n 'InitPackedVolume\|init-packed-volume' -A18 src/config.rs 2>/dev/null | head -26 | cut -c1-140 | sed 's/^/  /'
echo
echo "=== 4. seal 的触发点（workspace_overlay/seal.rs 被谁调用）==="
grep -rn 'seal.rs\|mod seal\|SealEngine\|seal_workspace' src/workspace_overlay/*.rs src/workspace_overlay/**/*.rs 2>/dev/null | grep -v test | head -10 | cut -c1-140 | sed 's/^/  /'
