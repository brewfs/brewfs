#!/bin/bash
# 加 swap、用 -j2 重编（依赖已缓存，cgu=256 保持不变以复用）
set -u
K=/root/.ssh/ax-c1-20261106
ssh -o StrictHostKeyChecking=no -i $K root@10.20.2.164 'bash -s' <<'EOS' 2>&1 | sed 's/^/  /'
set -u
echo "--- swap ---"
if ! swapon --show | grep -q swap; then
  if [ ! -f /srv/ax-data/swapfile ]; then
    fallocate -l 64G /srv/ax-data/swapfile && chmod 600 /srv/ax-data/swapfile && mkswap /srv/ax-data/swapfile >/dev/null 2>&1
  fi
  swapon /srv/ax-data/swapfile 2>/dev/null && echo "  swap on" || echo "  swap FAILED"
fi
free -g | sed 's/^/  /'
echo "--- 编译日志现状 ---"
grep -c . /root/build-seal.log 2>/dev/null || echo 0
tail -3 /root/build-seal.log 2>/dev/null | cut -c1-110
echo "--- 用 -j2 重编 ---"
export PATH=/root/.cargo/bin:$PATH
cd /srv/ax-data/src || exit 1
cat > /root/build-seal2.sh <<'INNER'
#!/bin/bash
set -u
export HOME=/root
export PATH=/root/.cargo/bin:$PATH
cd /srv/ax-data/src || exit 1
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=256
export CARGO_PROFILE_RELEASE_LTO=off
cargo build --release --bin brewfs -j 2 \
  --features fuse-io-uring-runtime,workspace-overlay \
  > /root/build-seal2.log 2>&1
echo "BUILD_RC=$?" >> /root/build-seal2.log
ls -lh target/release/brewfs >> /root/build-seal2.log 2>&1
INNER
chmod +x /root/build-seal2.sh
setsid nohup /root/build-seal2.sh </dev/null >/dev/null 2>&1 &
sleep 30
echo "  cargo=$(pgrep -fc 'cargo build' || echo 0) rustc=$(pgrep -fc rustc || echo 0)"
tail -3 /root/build-seal2.log 2>/dev/null | cut -c1-110
EOS
