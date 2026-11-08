#!/bin/bash
# 回到已验证参数（codegen-units=256，-j8），只重编 brewfs crate，避免依赖全量重编
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
pkill -f build-xattr3.sh 2>/dev/null || true
pkill -f 'cargo build' 2>/dev/null || true
sleep 3
cd /srv/ax-data/src/brewfs-new || exit 1
cat > /root/build-xattr4.sh <<'INNER'
#!/bin/bash
set -u
cd /srv/ax-data/src/brewfs-new || exit 1
export PATH=/root/.cargo/bin:$PATH
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=256
export CARGO_PROFILE_RELEASE_DEBUG=0
/root/.cargo/bin/cargo build --release --bin brewfs -j 8 \
  --features fuse-io-uring-runtime,workspace-overlay,native-packed-base,rustfs-ec-block-size-hint \
  > /root/build-xattr4.log 2>&1
echo "BUILD_RC=$?" >> /root/build-xattr4.log
ls -l target/release/brewfs >> /root/build-xattr4.log 2>&1
strings -a target/release/brewfs 2>/dev/null | grep -c 'invalid setxattr flags' > /root/build-xattr4.marker 2>&1
INNER
chmod +x /root/build-xattr4.sh
setsid nohup /root/build-xattr4.sh </dev/null > /dev/null 2>&1 &
sleep 30
echo "  cargo: $(pgrep -fc 'cargo build' || echo 0)  rustc: $(pgrep -fc rustc || echo 0)"
tail -3 /root/build-xattr4.log 2>/dev/null | cut -c1-120 | sed 's/^/  /'
EOS
