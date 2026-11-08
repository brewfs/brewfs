#!/bin/bash
# 修正加速配置：撤掉无用的 mold linker，把真正的开关放进 shim
set -u
echo "=== 1. 撤掉 linker 覆盖（rustc 本来就用 rust-lld，避免白重编）==="
CFG=/home/hxy/.cargo/config.toml
cat > "$CFG" <<'EOF'
# brewfs 编译加速配置（2026-11-05，修正版）
# 注意：cargo 不接受 config 里的 [profile.*]，profile 必须走 Cargo.toml 或 CARGO_PROFILE_* 环境变量
# 本文件只放 target/build 级别的东西
[build]
jobs = 6
incremental = true
EOF
sed 's/^/  /' "$CFG"
echo
echo "=== 2. 改 shim：注入真正有效的环境变量 ==="
SHIM=/home/hxy/shim/cargo
cp "$SHIM" "$SHIM.bak.$(date +%s)"
# 备份后重写：保留原有 systemd-run 逻辑，只改 setenv 部分
cat > "$SHIM" <<'EOF'
#!/bin/bash
# cargo shim：所有 cargo 调用都进 cgroup（保护整机），并注入编译加速环境
NS=$(date +%s%N)
ARGS=()
for a in "$@"; do ARGS+=("$a"); done
exec sudo -n systemd-run \
  --unit="cargo-${NS}-$$" --collect --quiet --pipe \
  -p MemoryMax=24G -p MemorySwapMax=40G -p CPUQuota=700% -p IOWeight=50 \
  --setenv=CARGO_BUILD_JOBS=6 \
  --setenv=CARGO_INCREMENTAL=1 \
  --setenv=CARGO_PROFILE_RELEASE_DEBUG=0 \
  --setenv=CARGO_PROFILE_RELEASE_STRIP=false \
  --setenv=CARGO_PROFILE_TEST_DEBUG=0 \
  --setenv=CARGO_PROFILE_DEV_DEBUG=0 \
  --setenv=RUSTUP_HOME=/home/hxy/.rustup \
  --setenv=CARGO_HOME=/home/hxy/.cargo \
  --setenv=HOME=/home/hxy \
  --uid=hxy --gid=hxy --working-directory=/home/hxy/brewfs-main \
  /home/hxy/.cargo/bin/cargo "${ARGS[@]}"
EOF
chmod +x "$SHIM"
echo "  新 shim:"
sed 's/^/    /' "$SHIM" | head -28
echo
echo "=== 3. 冒烟测试（cargo --version，确认 shim 能跑）==="
export PATH=/home/hxy/shim:/home/hxy/.cargo/bin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
cargo --version 2>&1 | head -3 | sed 's/^/  /'
echo
echo "=== 4. 确认 cgroup 生效 ==="
systemctl list-units 'cargo-*' --no-pager --plain 2>/dev/null | head -3 | sed 's/^/  /'
