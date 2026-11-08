#!/bin/bash
set -u
cd /home/hxy/brewfs-main || exit 1
cat > /home/hxy/run-debug-build.sh <<'INNER'
#!/bin/bash
set -u
export HOME=/home/hxy
cd /home/hxy/brewfs-main || exit 1
export PATH=/home/hxy/.cargo/bin:$PATH
export CARGO_PROFILE_DEV_DEBUG=0
export CARGO_INCREMENTAL=0
cargo build --bin brewfs -j 4 \
  --features fuse-io-uring-runtime,workspace-overlay,native-packed-base,rustfs-ec-block-size-hint \
  > /home/hxy/build-seal-debug.log 2>&1
echo "BUILD_RC=$?" >> /home/hxy/build-seal-debug.log
ls -lh target/debug/brewfs >> /home/hxy/build-seal-debug.log 2>&1
INNER
chmod +x /home/hxy/run-debug-build.sh
tmux kill-session -t gbuild 2>/dev/null || true
tmux new-session -d -s gbuild -c /home/hxy/brewfs-main "/home/hxy/run-debug-build.sh"
sleep 20
echo "  tmux: $(tmux ls 2>/dev/null | tr '\n' ' ')"
echo "  log: $(stat -c%s /home/hxy/build-seal-debug.log 2>/dev/null || echo 0) bytes"
tail -2 /home/hxy/build-seal-debug.log 2>/dev/null | cut -c1-110 | sed 's/^/  /'
