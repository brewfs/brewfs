#!/bin/bash
# OOM 后修复：加 64 GB swap + 降低 codegen 内存，再重编
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== 1. 当前内存/swap ==="
free -g | sed 's/^/  /'
df -h /srv/ax-data | tail -1 | sed 's/^/  /'
echo
echo "=== 2. 建 64 GB swap ==="
if ! swapon --show | grep -q swapfile; then
  fallocate -l 64G /srv/ax-data/swapfile 2>/dev/null || dd if=/dev/zero of=/srv/ax-data/swapfile bs=1M count=65536 status=none
  chmod 600 /srv/ax-data/swapfile
  mkswap /srv/ax-data/swapfile >/dev/null 2>&1
  swapon /srv/ax-data/swapfile && echo "  swap 已启用"
else
  echo "  swap 已存在"
fi
free -g | sed 's/^/  /'
echo
echo "=== 3. 重编（-j8 + codegen-units=256 省内存）==="
cat > /root/build-new2.sh <<'INNER'
#!/bin/bash
set -u
cd /srv/ax-data/src/brewfs-new || exit 1
export PATH=/root/.cargo/bin:$PATH
export AWS_ACCESS_KEY_ID=$(cat /etc/ax-c1/rustfs-access)
export AWS_SECRET_ACCESS_KEY=$(cat /etc/ax-c1/rustfs-secret)
export AWS_DEFAULT_REGION=us-east-1
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=256
cargo build --release --bin brewfs -j 8 \
  --features fuse-io-uring-runtime,workspace-overlay,native-packed-base,rustfs-ec-block-size-hint \
  > /root/build-new2.log 2>&1
echo "BUILD_RC=$?" >> /root/build-new2.log
ls -lh target/release/brewfs >> /root/build-new2.log 2>&1
INNER
chmod +x /root/build-new2.sh
setsid nohup /root/build-new2.sh > /dev/null 2>&1 &
sleep 30
echo "  cargo: $(pgrep -fc 'cargo build' || echo 0)  rustc: $(pgrep -fc rustc || echo 0)"
free -g | sed 's/^/  /'
EOS
