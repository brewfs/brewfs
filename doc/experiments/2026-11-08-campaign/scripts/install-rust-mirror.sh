#!/bin/bash
# 在 build 机装 rust（走阿里云镜像）+ cargo 镜像，然后验证能拉依赖
set -u
K=/root/.ssh/ax-c1-20261106
B=10.20.2.164
ssh -o StrictHostKeyChecking=no -i $K root@$B 'bash -s' <<'EOS' 2>&1 | sed 's/^/  /'
set -u
export DEBIAN_FRONTEND=noninteractive
echo "--- 用阿里云镜像装 rustup ---"
export RUSTUP_DIST_SERVER=https://mirrors.aliyun.com/rustup
export RUSTUP_UPDATE_ROOT=https://mirrors.aliyun.com/rustup/rustup
if ! command -v cargo >/dev/null 2>&1; then
  curl -sSf --max-time 60 https://mirrors.aliyun.com/rustup/rustup/dist/x86_64-unknown-linux-gnu/rustup-init -o /tmp/rustup-init && chmod +x /tmp/rustup-init && \
  /tmp/rustup-init -y --default-toolchain stable --profile minimal >/dev/null 2>&1 && echo "  rustup ok" || echo "  rustup FAILED"
fi
export PATH=$HOME/.cargo/bin:$PATH
cargo --version 2>/dev/null | sed 's/^/  /' || echo "  仍无 cargo"
echo "--- cargo 镜像（rsproxy）---"
mkdir -p $HOME/.cargo
cat > $HOME/.cargo/config.toml <<'TOML'
[source.crates-io]
replace-with = 'rsproxy-sparse'
[source.rsproxy-sparse]
registry = "sparse+https://rsproxy.cn/index/"
[registries.rsproxy]
index = "https://rsproxy.cn/crates.io-index"
[net]
git-fetch-with-cli = true
TOML
echo "  config written"
echo "--- 依赖系统库 ---"
apt-get install -y -qq libfuse3-dev fuse3 pkg-config libssl-dev clang cmake >/dev/null 2>&1 && echo "  libs ok"
echo "--- 解压源码 + 打 seal 补丁 ---"
cd /srv/ax-data/src || exit 1
if [ -d brewfs-main ]; then
  echo "  已存在源码目录"
else
  mkdir -p brewfs-main && tar -xzf /root/brewfs-src.tar.gz -C brewfs-main 2>/dev/null
  ls brewfs-main | head -3 | sed 's/^/  /'
fi
D=$(ls -d /srv/ax-data/src/brewfs-main/* 2>/dev/null | head -1)
echo "  源码根候选: $D"
find /srv/ax-data/src -maxdepth 3 -name Cargo.toml | head -3 | sed 's/^/  cargo.toml: /'
EOS
