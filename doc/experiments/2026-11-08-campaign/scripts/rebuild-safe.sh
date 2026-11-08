#!/bin/bash
# 重启后：恢复 swap + 安全参数重编（-j4, codegen-units=16）
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== 1. swap ==="
swapon --show | sed 's/^/  /' || echo "  无 swap"
if ! swapon --show | grep -q swapfile; then
  [ -f /srv/ax-data/swapfile ] || { fallocate -l 64G /srv/ax-data/swapfile; chmod 600 /srv/ax-data/swapfile; mkswap /srv/ax-data/swapfile >/dev/null 2>&1; }
  swapon /srv/ax-data/swapfile 2>/dev/null && echo "  swap 已启用" || echo "  swap 启用失败"
fi
free -g | sed 's/^/  /'
echo "=== 2. 重编 ==="
cd /srv/ax-data/src/brewfs-new || exit 1
pkill -f 'cargo build' 2>/dev/null || true
sleep 2
cat > /root/build-xattr3.sh <<'INNER'
#!/bin/bash
set -u
cd /srv/ax-data/src/brewfs-new || exit 1
export PATH=/root/.cargo/bin:$PATH
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16
export CARGO_PROFILE_RELEASE_DEBUG=0
/root/.cargo/bin/cargo build --release --bin brewfs -j 4 \
  --features fuse-io-uring-runtime,workspace-overlay,native-packed-base,rustfs-ec-block-size-hint \
  > /root/build-xattr3.log 2>&1
echo "BUILD_RC=$?" >> /root/build-xattr3.log
ls -l target/release/brewfs >> /root/build-xattr3.log 2>&1
strings -a target/release/brewfs 2>/dev/null | grep -c 'invalid setxattr flags' > /root/build-xattr3.marker 2>&1
INNER
chmod +x /root/build-xattr3.sh
setsid nohup /root/build-xattr3.sh </dev/null > /dev/null 2>&1 &
sleep 30
echo "  cargo: $(pgrep -fc 'cargo build' || echo 0)  rustc: $(pgrep -fc rustc || echo 0)"
tail -3 /root/build-xattr3.log 2>/dev/null | cut -c1-120 | sed 's/^/  /'
EOS
