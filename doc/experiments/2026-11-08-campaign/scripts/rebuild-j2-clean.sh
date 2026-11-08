#!/bin/bash
# 修正：不要改 LTO（会触发依赖全量重编）；只保留 cgu=256，-j2
set -u
K=/root/.ssh/ax-c1-20261106
ssh -o StrictHostKeyChecking=no -i $K root@10.20.2.164 'bash -s' <<'EOS' 2>&1 | sed 's/^/  /'
set -u
pkill -9 -f build-seal2.sh 2>/dev/null || true
pkill -9 -f 'cargo build' 2>/dev/null || true
pkill -9 -f rustc 2>/dev/null || true
sleep 3
export PATH=/root/.cargo/bin:$PATH
cd /srv/ax-data/src || exit 1
cat > /root/build-seal3.sh <<'INNER'
#!/bin/bash
set -u
export HOME=/root
export PATH=/root/.cargo/bin:$PATH
cd /srv/ax-data/src || exit 1
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=256
cargo build --release --bin brewfs -j 2 \
  --features fuse-io-uring-runtime,workspace-overlay \
  > /root/build-seal3.log 2>&1
echo "BUILD_RC=$?" >> /root/build-seal3.log
ls -lh target/release/brewfs >> /root/build-seal3.log 2>&1
INNER
chmod +x /root/build-seal3.sh
: > /root/build-seal3.log
setsid nohup /root/build-seal3.sh </dev/null >/dev/null 2>&1 &
sleep 40
echo "  cargo=$(pgrep -fc 'cargo build' || echo 0) rustc=$(pgrep -fc rustc || echo 0)"
echo "  已编 crate 数: $(grep -c 'Compiling' /root/build-seal3.log 2>/dev/null || echo 0)"
tail -3 /root/build-seal3.log 2>/dev/null | cut -c1-110
EOS
