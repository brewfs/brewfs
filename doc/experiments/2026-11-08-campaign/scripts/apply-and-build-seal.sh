#!/bin/bash
# 打补丁 + 启动 release 编译（build 机 8c32g）
set -u
K=/root/.ssh/ax-c1-20261106
B=10.20.2.164
ssh -o StrictHostKeyChecking=no -i $K root@$B 'bash -s' <<'EOS' 2>&1 | sed 's/^/  /'
set -u
export PATH=$HOME/.cargo/bin:$PATH
cd /srv/ax-data/src || exit 1
echo "--- 补丁检查 ---"
git apply --check /root/seal.patch 2>&1 | head -3 && echo "  check 通过" || echo "  check 有冲突"
echo "--- 应用 ---"
git apply /root/seal.patch 2>&1 | head -5 && echo "  applied" || echo "  已有改动或失败"
git status --short 2>/dev/null | head -6 | sed 's/^/  /'
echo "--- seal 子命令是否在源码 ---"
grep -c 'Seal' src/cli.rs 2>/dev/null | sed 's/^/  cli.rs Seal 出现次数: /'
echo "--- 启动编译（后台）---"
cat > /root/build-seal.sh <<'INNER'
#!/bin/bash
set -u
export HOME=/root
export PATH=/root/.cargo/bin:$PATH
cd /srv/ax-data/src || exit 1
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=256
cargo build --release --bin brewfs -j 6 \
  --features fuse-io-uring-runtime,workspace-overlay \
  > /root/build-seal.log 2>&1
echo "BUILD_RC=$?" >> /root/build-seal.log
ls -lh target/release/brewfs >> /root/build-seal.log 2>&1
INNER
chmod +x /root/build-seal.sh
pkill -f 'cargo build' 2>/dev/null || true
sleep 2
: > /root/build-seal.log
setsid nohup /root/build-seal.sh </dev/null >/dev/null 2>&1 &
sleep 30
echo "  cargo=$(pgrep -fc 'cargo build' || echo 0) rustc=$(pgrep -fc rustc || echo 0)"
tail -3 /root/build-seal.log 2>/dev/null | cut -c1-110 | sed 's/^/  /'
EOS
