#!/bin/bash
set -u
export PATH=/home/hxy/.cargo/bin:/home/hxy/bin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export HOME=/home/hxy
D=/tmp/moldverify
cd "$D" || exit 1
rm -rf target
echo "=== 完整 rustc 链接参数（过滤关键项）==="
cargo build -v 2>&1 | tr ' ' '\n' | grep -nE 'linker|lld|mold|gcc|link-arg|link-self-contained' | head -20 | sed 's/^/  /'
echo
echo "=== rustc 默认 linker 是什么 ==="
rustc --print link-args 2>/dev/null | head -3 | sed 's/^/  /' || echo "  （不支持 --print link-args）"
echo
echo "=== rustc 是否内置 lld ==="
ls "$(rustc --print sysroot 2>/dev/null)/lib/rustlib/x86_64-unknown-linux-gnu/bin/" 2>/dev/null | head -6 | sed 's/^/  /'
echo
echo "=== 我们装的 mold 与 lld 版本 ==="
/usr/bin/ld.mold --version 2>/dev/null | head -1 | sed 's/^/  mold: /'
/usr/bin/ld.lld --version 2>/dev/null | head -1 | sed 's/^/  lld: /'
