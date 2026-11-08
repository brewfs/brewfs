#!/bin/bash
# 在 build 主机上建 hxy 用户 + rust 工具链 + shim（与 brewfs 一致的路径）
set -u
echo "=== 0. 现状 ==="
echo "  rustc: $(command -v rustc || echo 未装)"
echo "  ubuntu 用户: $(id ubuntu 2>/dev/null | cut -c1-60)"

echo
echo "=== 1. 建 hxy 用户（与 brewfs 同路径布局）==="
if ! id hxy >/dev/null 2>&1; then
  sudo -n useradd -m -s /bin/bash hxy
  echo "hxy ALL=(ALL) NOPASSWD:ALL" | sudo -n tee /etc/sudoers.d/hxy >/dev/null
  chmod 440 /etc/sudoers.d/hxy 2>/dev/null || sudo -n chmod 440 /etc/sudoers.d/hxy
fi
id hxy | sed 's/^/  /'

echo
echo "=== 2. 把 root 的 ssh key 给 hxy（便于从 brewfs 直连）==="
sudo -n mkdir -p /home/hxy/.ssh
sudo -n cp /root/.ssh/authorized_keys /home/hxy/.ssh/authorized_keys 2>/dev/null || true
sudo -n chown -R hxy:hxy /home/hxy/.ssh 2>/dev/null || true
sudo -n chmod 700 /home/hxy/.ssh; sudo -n chmod 600 /home/hxy/.ssh/authorized_keys 2>/dev/null || true
ls -l /home/hxy/.ssh/ 2>/dev/null | sed 's/^/  /'

echo
echo "=== 3. 为 hxy 装 rust 1.99.1 ==="
sudo -n -u hxy bash -lc '
set -e
export HOME=/home/hxy
if [ ! -x /home/hxy/.cargo/bin/rustup ]; then
  curl -sSf https://sh.rustup.rs -o /tmp/rustup.sh
  sh /tmp/rustup.sh -y --profile minimal --default-toolchain 1.99.1 >/dev/null 2>&1
fi
/home/hxy/.cargo/bin/rustc --version
/home/hxy/.cargo/bin/cargo --version
' 2>&1 | sed 's/^/  /'

echo
echo "=== 4. shim（cgroup + 加速参数，与 brewfs 同款）==="
sudo -n -u hxy mkdir -p /home/hxy/shim /home/hxy/bin
sudo -n tee /home/hxy/shim/cargo >/dev/null <<'EOS'
#!/bin/bash
# cargo shim（build 主机版）
set -u
NS=$(date +%s%N)
ENVARGS=()
for name in PATH RUSTFLAGS RUSTDOCFLAGS RUSTUP_TOOLCHAIN CARGO_HOME RUSTUP_HOME \
            CARGO_TARGET_DIR CARGO_BUILD_JOBS CARGO_INCREMENTAL \
            CARGO_PROFILE_RELEASE_DEBUG CARGO_PROFILE_TEST_DEBUG CARGO_PROFILE_DEV_DEBUG \
            RUST_BACKTRACE RUST_LOG TMPDIR HOME USER LANG; do
  if [ -n "${!name:-}" ]; then ENVARGS+=(--setenv="$name=${!name}"); fi
done
while IFS='=' read -r k v; do
  case "$k" in BREWFS_*|AWS_*|TIKV_*|REDIS_*|S3_*) ENVARGS+=(--setenv="$k=$v") ;; esac
done < <(env)
exec sudo -n systemd-run \
  --unit="cargo-${NS}-$$" --collect --quiet --pipe \
  -p MemoryMax=24G -p MemorySwapMax=40G -p CPUQuota=700% -p IOWeight=50 \
  "${ENVARGS[@]}" \
  --setenv=CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-6}" \
  --setenv=CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-1}" \
  --setenv=CARGO_PROFILE_RELEASE_DEBUG="${CARGO_PROFILE_RELEASE_DEBUG:-line-tables-only}" \
  --setenv=CARGO_PROFILE_TEST_DEBUG="${CARGO_PROFILE_TEST_DEBUG:-0}" \
  --setenv=CARGO_PROFILE_DEV_DEBUG="${CARGO_PROFILE_DEV_DEBUG:-0}" \
  --uid=hxy --gid=hxy --working-directory="$PWD" \
  /home/hxy/.cargo/bin/cargo "$@"
EOS
sudo -n chmod +x /home/hxy/shim/cargo
sudo -n chown -R hxy:hxy /home/hxy/shim /home/hxy/bin

echo
echo "=== 5. cargo config ==="
sudo -n -u hxy bash -c 'mkdir -p /home/hxy/.cargo && cat > /home/hxy/.cargo/config.toml <<EOF
[build]
jobs = 6
incremental = true
EOF'
cat /home/hxy/.cargo/config.toml | sed 's/^/  /'

echo
echo "=== 6. sshd OOM 保护（修正服务名）==="
SVC=$(systemctl list-unit-files 'ssh*.service' --no-pager 2>/dev/null | awk '/ssh/{print $1; exit}')
echo "  服务: ${SVC:-ssh.service}"
sudo -n mkdir -p /etc/systemd/system/${SVC:-ssh.service}.d
printf '[Service]\nOOMScoreAdjust=-1000\n' | sudo -n tee /etc/systemd/system/${SVC:-ssh.service}.d/oom.conf >/dev/null
sudo -n systemctl daemon-reload
sudo -n systemctl restart ${SVC:-ssh.service} 2>/dev/null || true
sleep 2
echo "  sshd oom_score_adj: $(cat /proc/$(pgrep -o sshd)/oom_score_adj 2>/dev/null)"

echo
echo "=== 7. 汇总 ==="
echo "  hxy rust: $(sudo -n -u hxy /home/hxy/.cargo/bin/rustc --version 2>&1)"
echo "  disk: $(df -h / | tail -1 | awk '{print $4}') avail"
