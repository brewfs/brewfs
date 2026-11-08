#!/bin/bash
# 修 rust 安装（hxy 的 /tmp 权限问题）+ 验证
set -u
echo "=== 1. 用 root 下载 rustup 到 /root，再以 hxy 运行 ==="
if [ ! -f /root/rustup.sh ]; then
  curl -sSf https://sh.rustup.rs -o /root/rustup.sh
fi
ls -l /root/rustup.sh | sed 's/^/  /'
chmod 644 /root/rustup.sh 2>/dev/null
cp /root/rustup.sh /home/hxy/rustup.sh
chown hxy:hxy /home/hxy/rustup.sh

echo
echo "=== 2. 以 hxy 安装 1.99.1（minimal）==="
sudo -n -u hxy bash -c '
export HOME=/home/hxy
export RUSTUP_HOME=/home/hxy/.rustup
export CARGO_HOME=/home/hxy/.cargo
cd /home/hxy
sh ./rustup.sh -y --profile minimal --default-toolchain 1.99.1 --no-modify-path 2>&1 | tail -6
' 2>&1 | sed 's/^/  /'

echo
echo "=== 3. 验证 ==="
sudo -n -u hxy bash -c 'export HOME=/home/hxy; /home/hxy/.cargo/bin/rustc --version; /home/hxy/.cargo/bin/cargo --version' 2>&1 | sed 's/^/  /'

echo
echo "=== 4. shim 与配置检查 ==="
ls -l /home/hxy/shim/cargo /home/hxy/.cargo/config.toml 2>/dev/null | sed 's/^/  /'
head -3 /home/hxy/shim/cargo 2>/dev/null | sed 's/^/  /'
