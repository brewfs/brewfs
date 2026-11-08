#!/bin/bash
set -u
echo "=== 1. 可用的加速组件 ==="
for b in sccache mold lld ld.lld clang gcc ccache; do
  P=$(command -v "$b" 2>/dev/null || true)
  printf "  %-10s %s\n" "$b" "${P:-未安装}"
done
echo
echo "=== 2. rustc 自带并行/增量相关 ==="
export PATH=/home/hxy/.cargo/bin:$PATH
rustc -vV 2>/dev/null | grep -E 'release|host' | sed 's/^/  /'
echo "  nightly 可用: $(rustc +nightly-2026-10-27 --version 2>/dev/null | head -1 || echo 无)"
echo
echo "=== 3. 当前 target 是否带增量缓存 ==="
T=/home/hxy/brewfs-pkv-target
for p in debug release; do
  N=$(find "$T/$p/incremental" -maxdepth 1 -type d 2>/dev/null | wc -l)
  echo "  $p/incremental 目录数: $N"
done
echo "  target 总大小: $(du -sm "$T" 2>/dev/null | awk '{print $1}') MB"
echo
echo "=== 4. 磁盘 inode/空间 ==="
df -h /mnt/brewfs-data | tail -1 | sed 's/^/  /'
df -i /mnt/brewfs-data | tail -1 | sed 's/^/  /'
echo
echo "=== 5. CPU 型号/核数 ==="
lscpu 2>/dev/null | grep -E 'Model name|^CPU\(s\)|Thread|Core' | sed 's/^/  /'
