#!/bin/bash
set -u
echo "=== cargo 位置 ==="
for p in /root/.cargo/bin/cargo /home/hxy/.cargo/bin/cargo /usr/local/bin/cargo /home/hxy/shim/cargo; do
  [ -x "$p" ] && echo "  找到: $p"
done
command -v cargo 2>/dev/null | sed 's/^/  PATH cargo: /' || echo "  PATH 中无 cargo"
echo
echo "=== 源码状态 ==="
cd /srv/ax-data/src/brewfs || exit 1
ls -d .git 2>/dev/null && echo "  git 仓库" || echo "  非 git（campaign 拷贝）"
echo "  store.rs 行尾: $(file src/meta/store.rs | cut -c1-80)"
echo "  已应用的补丁残留: $(ls src/meta/store.rs.rej src/meta/stores/tikv/tests.rs.rej 2>/dev/null | tr '\n' ' ')"
echo
echo "=== 与 brewfs-main 的差异规模（抽查 3 个文件行数）==="
for f in src/meta/store.rs src/cli.rs src/meta/stores/tikv/mod.rs; do
  echo "  $f: $(wc -l < $f 2>/dev/null) 行"
done
