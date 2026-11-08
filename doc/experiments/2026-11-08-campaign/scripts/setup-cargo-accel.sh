#!/bin/bash
# 全局 cargo 配置：mold 链接器 + 6 路并行 + 增量
set -u
mkdir -p /home/hxy/bin
cat > /home/hxy/bin/gcc-mold <<'EOF'
#!/bin/sh
# 用 mold 链接（比 GNU ld 快 5-10 倍），其余参数原样传给 gcc
exec gcc -fuse-ld=mold "$@"
EOF
chmod +x /home/hxy/bin/gcc-mold
echo "  已建 /home/hxy/bin/gcc-mold"

CFG=/home/hxy/.cargo/config.toml
[ -f "$CFG" ] && cp "$CFG" "$CFG.bak.$(date +%s)" && echo "  已备份原 config"

cat > "$CFG" <<'EOF'
# brewfs 编译加速配置（2026-11-05）
# 1) mold 链接器：391 MB 二进制链接从 3-5 分钟降到 20-40 秒
# 2) jobs=6：依赖阶段并行（主 crate 仍是单 rustc，不受影响）
# 3) incremental：改 1 个文件时增量重编，而不是整 crate 全量
[build]
jobs = 6
incremental = true

[target.x86_64-unknown-linux-gnu]
linker = "/home/hxy/bin/gcc-mold"

[profile.release]
debug = false
strip = false
EOF
echo "  已写 $CFG："
sed 's/^/    /' "$CFG"
echo
echo "=== 验证生效（用它做一次 dry 检查）==="
export PATH=/home/hxy/.cargo/bin:$PATH
cd /home/hxy/brewfs-main || exit 1
cargo config get build.jobs 2>/dev/null | head -5 | sed 's/^/  /' || echo "  （cargo config get 不可用，跳过）"
echo
echo "=== 确认 mold 路径可执行 ==="
/home/hxy/bin/gcc-mold --version 2>&1 | head -2 | sed 's/^/  /'
