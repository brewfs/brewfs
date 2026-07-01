#!/usr/bin/env bash

brewfs_local_binary() {
  local repo_root="$1"
  if [[ -n "${BREWFS_BIN_LOCAL:-}" ]]; then
    printf '%s' "$BREWFS_BIN_LOCAL"
    return 0
  fi
  printf '%s' "${repo_root}/target/release/examples/${BREWFS_EXAMPLE}"
}

brewfs_build_local() {
  local repo_root="$1"
  log_info "Building BrewFS example: ${BREWFS_EXAMPLE}"
  (cd "${repo_root}" && cargo build -p brewfs --example "${BREWFS_EXAMPLE}" --release)
}

brewfs_etcd_urls_csv() {
  if [[ -n "${BREWFS_META_ETCD_URLS:-}" ]]; then
    printf '%s' "$BREWFS_META_ETCD_URLS"
    return 0
  fi

  if [[ -n "${META_NODES:-}" ]]; then
    local urls=()
    local node
    for node in $META_NODES; do
      urls+=("http://${node}:2379")
    done
    (IFS=','; printf '%s' "${urls[*]}")
    return 0
  fi

  die "BREWFS_META_ETCD_URLS or META_NODES must be set for etcd backend"
}

brewfs_render_config() {
  local backend="${BREWFS_META_BACKEND}"

  case "$backend" in
    etcd)
      local urls_csv
      urls_csv="$(brewfs_etcd_urls_csv)"
      local urls=()
      IFS=',' read -r -a urls <<< "$urls_csv"
      printf 'database:\n'
      printf '  type: etcd\n'
      printf '  urls:\n'
      local url
      for url in "${urls[@]}"; do
        printf '    - "%s"\n' "$url"
      done
      ;;
    redis)
      require_var BREWFS_META_URL
      printf 'database:\n'
      printf '  type: redis\n'
      printf '  url: "%s"\n' "$BREWFS_META_URL"
      ;;
    postgres)
      require_var BREWFS_META_URL
      printf 'database:\n'
      printf '  type: postgres\n'
      printf '  url: "%s"\n' "$BREWFS_META_URL"
      ;;
    sqlite)
      require_var BREWFS_META_URL
      printf 'database:\n'
      printf '  type: sqlite\n'
      printf '  url: "%s"\n' "$BREWFS_META_URL"
      ;;
    *)
      die "Unknown BREWFS_META_BACKEND: ${backend}"
      ;;
  esac
}

brewfs_prepare_node() {
  local node="$1"

  # Clean up stale mount points before preparing
  log_info "  Cleaning up stale mount point on ${node}"
  ssh_exec_sudo "$node" "umount -l '${BREWFS_MOUNT_DIR}' 2>/dev/null || fusermount -u '${BREWFS_MOUNT_DIR}' 2>/dev/null || true"
  ssh_exec_sudo "$node" "rm -rf '${BREWFS_MOUNT_DIR}' 2>/dev/null || true"

  ssh_exec_sudo "$node" "mkdir -p '${REMOTE_WORKDIR}/bin' '${REMOTE_WORKDIR}/pids' '${BREWFS_META_DIR}' '${BREWFS_DATA_DIR}' '${BREWFS_MOUNT_DIR}' '${BREWFS_LOG_DIR}'"
  ssh_exec_sudo "$node" "chown -R '${SSH_USER}':'${SSH_USER}' '${REMOTE_WORKDIR}' '${BREWFS_META_DIR}' '${BREWFS_DATA_DIR}' '${BREWFS_MOUNT_DIR}' '${BREWFS_LOG_DIR}'"

  # Enable user_allow_other in /etc/fuse.conf for allow_other mount option
  log_info "  Enabling user_allow_other in /etc/fuse.conf on ${node}"
  ssh_exec_sudo "$node" "grep -q '^user_allow_other' /etc/fuse.conf 2>/dev/null || echo 'user_allow_other' >> /etc/fuse.conf"
}

brewfs_deploy_binary() {
  local node="$1"
  local bin_local="$2"
  local bin_remote="${REMOTE_WORKDIR}/bin/brewfs-demo"

  local local_md5
  local_md5=$(md5sum "$bin_local" 2>/dev/null | awk '{print $1}')

  if [[ -z "$local_md5" ]]; then
    log_warn "Failed to compute local MD5, uploading without check"
    scp_to "$bin_local" "$node" "$bin_remote"
    ssh_exec "$node" "chmod +x '$bin_remote'"
    BREWFS_BIN_REMOTE="$bin_remote"
    return 0
  fi

  # Check if remote file exists and has the same MD5
  if ssh_run "$node" "test -f '$bin_remote' && md5sum '$bin_remote' 2>/dev/null | awk '{print \$1}' | grep -q '$local_md5'"; then
    log_info "Binary already exists on ${node} (MD5: ${local_md5}), skipping upload"
    BREWFS_BIN_REMOTE="$bin_remote"
    return 0
  fi

  # Need to upload
  log_info "Uploading binary to ${node}..."
  scp_to "$bin_local" "$node" "$bin_remote"
  ssh_exec "$node" "chmod +x '$bin_remote'"
  BREWFS_BIN_REMOTE="$bin_remote"
}

brewfs_deploy_config() {
  local node="$1"
  local cfg_remote="${BREWFS_CONFIG_REMOTE:-${REMOTE_WORKDIR}/brewfs.yml}"

  local cfg_basename
  cfg_basename="$(basename "$cfg_remote")"
  local tmp_dir
  tmp_dir="$(mktemp -d)"
  local tmp_cfg="${tmp_dir}/${cfg_basename}"
  brewfs_render_config > "$tmp_cfg"
  scp_to "$tmp_cfg" "$node" "$cfg_remote"
  rm -rf "$tmp_dir"

  BREWFS_CONFIG_REMOTE="$cfg_remote"
}

brewfs_start_node() {
  local node="$1"
  local bin_remote="${BREWFS_BIN_REMOTE}"
  local cfg_remote="${BREWFS_CONFIG_REMOTE}"
  local log_file="${BREWFS_LOG_DIR}/brewfs-${node}.log"
  local pid_file="${REMOTE_WORKDIR}/pids/brewfs-${node}.pid"

  local cmd
  cmd="'${bin_remote}' --config '${cfg_remote}' --mount '${BREWFS_MOUNT_DIR}' --storage '${BREWFS_DATA_DIR}'"

  ssh_exec "$node" "nohup bash -lc \"${cmd}\" >'${log_file}' 2>&1 & echo \$! > '${pid_file}'"
}

brewfs_wait_mount() {
  local node="$1"
  local mount_dir="$BREWFS_MOUNT_DIR"
  local retries="${MOUNT_WAIT_RETRIES:-30}"

  ssh_run "$node" "for i in \$(seq 1 ${retries}); do if command -v mountpoint >/dev/null 2>&1; then mountpoint -q '${mount_dir}' && exit 0; else mount | grep -q ' ${mount_dir} ' && exit 0; fi; sleep 1; done; exit 1"
}

brewfs_stop_node() {
  local node="$1"
  local pid_file="${REMOTE_WORKDIR}/pids/brewfs-${node}.pid"

  ssh_exec "$node" "if [[ -f '${pid_file}' ]]; then kill \$(cat '${pid_file}') >/dev/null 2>&1 || true; fi"
}

brewfs_unmount_node() {
  local node="$1"
  # First, try to unmount if mounted
  ssh_exec_sudo "$node" "if mountpoint -q '${BREWFS_MOUNT_DIR}'; then fusermount -u '${BREWFS_MOUNT_DIR}' || umount -f '${BREWFS_MOUNT_DIR}'; fi"
  # Then, clean up any leftover files in the mount directory
  ssh_exec_sudo "$node" "rm -rf '${BREWFS_MOUNT_DIR}'/* 2>/dev/null || true"
}
