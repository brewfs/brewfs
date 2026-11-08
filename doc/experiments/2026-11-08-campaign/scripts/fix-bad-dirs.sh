#!/bin/bash
# 修 3 个坏目录（d0132/d0133/d0371）：先确认哪些文件坏，再从源补拷并核 sha
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
SRC=/srv/ax-data/c3/source/small
DST=/mnt/brewfs-c3/base/small
echo "=== 1. 逐目录比对大小（源 vs 挂载）==="
for d in d0132 d0133 d0371; do
  s=$(find "$SRC/$d" -maxdepth 1 -type f 2>/dev/null | wc -l)
  m=$(find "$DST/$d" -maxdepth 1 -type f 2>/dev/null | wc -l)
  z=$(find "$DST/$d" -maxdepth 1 -type f -size 0 2>/dev/null | wc -l)
  echo "  $d: 源=$s 挂载=$m 其中0字节=$z"
done

echo
echo "=== 2. 找出挂载侧 0 字节文件 ==="
for d in d0132 d0133 d0371; do
  for f in $(find "$DST/$d" -maxdepth 1 -type f -size 0 2>/dev/null); do
    echo "  0字节: $f (源大小 $(stat -c%s "$SRC/$d/$(basename $f)" 2>/dev/null))"
  done
done

echo
echo "=== 3. 从源补拷这 3 个目录（强制覆盖）==="
for d in d0132 d0133 d0371; do
  cp -f "$SRC/$d"/* "$DST/$d"/ 2>/tmp/fix.err
  echo "  $d 补拷完成，错误 $(wc -l < /tmp/fix.err) 行"
done
sync 2>/dev/null || true

echo
echo "=== 4. 复核：0 字节文件数 ==="
for d in d0132 d0133 d0371; do
  echo "  $d: 0字节=$(find "$DST/$d" -maxdepth 1 -type f -size 0 2>/dev/null | wc -l) 总数=$(find "$DST/$d" -maxdepth 1 -type f 2>/dev/null | wc -l)"
done
EOS
