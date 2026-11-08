#!/bin/bash
# C4 研究（续）：seal 引擎的调用者、packed manifest 的生产者
set -u
cd /home/hxy/brewfs-main || exit 1
echo "=== 1. seal 模块的公开入口 ==="
grep -nE 'pub (async )?fn|pub struct|impl ' src/workspace_overlay/seal.rs 2>/dev/null | head -20 | cut -c1-130 | sed 's/^/  /'
echo
echo "=== 2. 谁调用 seal 模块里的函数 ==="
for fn in $(grep -oE 'pub (async )?fn [a-z_]+' src/workspace_overlay/seal.rs 2>/dev/null | awk '{print $NF}' | head -8); do
  hits=$(grep -rn "$fn(" src --include=*.rs 2>/dev/null | grep -v 'seal.rs' | grep -v test | head -3)
  [ -n "$hits" ] && { echo "  --- $fn ---"; echo "$hits" | cut -c1-130 | sed 's/^/    /'; }
done
echo
echo "=== 3. packed manifest 生产（wire005 / manifest key 写入）==="
grep -rn 'manifest_key\|write_manifest\|publish_manifest' src --include=*.rs 2>/dev/null | grep -v test | head -10 | cut -c1-140 | sed 's/^/  /'
echo
echo "=== 4. 是否有 ingest 二进制/子命令 ==="
ls -1 src/bin 2>/dev/null | sed 's/^/  bin: /'
grep -n 'Ingest\|ingest' src/config.rs 2>/dev/null | head -8 | cut -c1-130 | sed 's/^/  /'
