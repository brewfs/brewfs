#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(realpath "${SCRIPT_DIR}/../../../..")"

source "${SCRIPT_DIR}/lib/common.sh"

usage() {
  cat <<USAGE
Usage: ${0##*/} [options] <action>

Actions:
  prepare           Prepare local/remote directories
  deploy-brewfs   Build (optional) and deploy BrewFS to client nodes
  test-brewfs     Run workloads on BrewFS mount
  collect           Pull remote results to local results dir
  cleanup           Stop mounts and background processes
  all               Run prepare + deploy + test + collect

Options:
  -c, --config PATH   Path to cluster.env (default: SCRIPT_DIR/cluster.env)
  -h, --help          Show this help
USAGE
}

ACTION="all"
CONFIG_PATH=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -c|--config)
      CONFIG_PATH="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      ACTION="$1"
      shift
      ;;
  esac
done

if [[ -z "$CONFIG_PATH" ]]; then
  CONFIG_PATH="${SCRIPT_DIR}/cluster.env"
fi

if [[ ! -f "$CONFIG_PATH" ]]; then
  die "Config not found: ${CONFIG_PATH} (copy cluster.env.example to cluster.env)"
fi

set -a
# shellcheck disable=SC1090
source "$CONFIG_PATH"
set +a

source "${SCRIPT_DIR}/lib/ssh.sh"
source "${SCRIPT_DIR}/lib/brewfs.sh"
source "${SCRIPT_DIR}/lib/tests.sh"

CLUSTER_NODES="${CLUSTER_NODES:-}"
CLIENT_NODES="${CLIENT_NODES:-$CLUSTER_NODES}"
META_NODES="${META_NODES:-$CLUSTER_NODES}"
PRIMARY_CLIENT="${PRIMARY_CLIENT:-${CLIENT_NODES%% *}}"

SSH_PORT="${SSH_PORT:-22}"
REMOTE_SUDO="${REMOTE_SUDO:-sudo -n}"
REMOTE_WORKDIR="${REMOTE_WORKDIR:-/tmp/brewfs-dist}"
REMOTE_RESULTS_DIR="${REMOTE_RESULTS_DIR:-/tmp/brewfs-results}"
RESULTS_DIR="${RESULTS_DIR:-${SCRIPT_DIR}/results}"

BREWFS_BUILD="${BREWFS_BUILD:-0}"
BREWFS_EXAMPLE="${BREWFS_EXAMPLE:-persistence_demo}"
BREWFS_META_BACKEND="${BREWFS_META_BACKEND:-etcd}"
BREWFS_CONFIG_REMOTE="${BREWFS_CONFIG_REMOTE:-${REMOTE_WORKDIR}/brewfs.yml}"
BREWFS_MOUNT_DIR="${BREWFS_MOUNT_DIR:-/mnt/brewfs}"
BREWFS_META_DIR="${BREWFS_META_DIR:-/var/lib/brewfs/meta}"
BREWFS_DATA_DIR="${BREWFS_DATA_DIR:-/var/lib/brewfs/data}"
BREWFS_LOG_DIR="${BREWFS_LOG_DIR:-/var/log/brewfs}"

ETCD_DATA_DIR="${ETCD_DATA_DIR:-/var/lib/etcd}"
ETCD_LOG_FILE="${ETCD_LOG_FILE:-/tmp/etcd.log}"
if [[ -z "${ETCD_DATA_DIR// }" ]]; then
  ETCD_DATA_DIR="/var/lib/etcd"
fi
if [[ -z "${ETCD_LOG_FILE// }" ]]; then
  ETCD_LOG_FILE="/tmp/etcd.log"
fi

RUN_ID="${RUN_ID:-$(now_stamp)}"
export RUN_ID

FIO_RUNTIME="${FIO_RUNTIME:-60}"
FIO_SIZE="${FIO_SIZE:-20G}"
FIO_NUMJOBS="${FIO_NUMJOBS:-4}"
FIO_IODEPTH="${FIO_IODEPTH:-16}"
FIO_BS_SEQ="${FIO_BS_SEQ:-1M}"
FIO_BS_RAND="${FIO_BS_RAND:-4K}"
FIO_RWMIXREAD="${FIO_RWMIXREAD:-70}"
FIO_DIRECT="${FIO_DIRECT:-0}"

MDTEST_NFILES="${MDTEST_NFILES:-10000}"
MDTEST_NPROCS="${MDTEST_NPROCS:-4}"

require_var SSH_USER
require_var CLUSTER_NODES
require_var PRIMARY_CLIENT

check_dependencies() {
  log_info "Checking dependencies..."

  # Check SSH connectivity and sudo
  for node in $CLIENT_NODES; do
    if ! ssh_exec "$node" "echo 'SSH OK'" >/dev/null 2>&1; then
      die "SSH connection failed to ${node}"
    fi
    if ! ssh_exec_sudo "$node" "true" >/dev/null 2>&1; then
      die "Passwordless sudo not configured on ${node} (set REMOTE_SUDO='sudo' if using password)"
    fi
  done
  log_success "SSH and sudo: OK"

  # Check/install fio if enabled
  if [[ "${RUN_FIO:-0}" == "1" ]]; then
    if ! ssh_run "$PRIMARY_CLIENT" "command -v fio >/dev/null 2>&1"; then
      log_warn "fio not found, installing on all CLIENT_NODES..."
      for node in $CLIENT_NODES; do
        log_info "  Installing fio on ${node}..."
        ssh_exec_sudo "$node" "apt-get update && apt-get install -y fio"
      done
    fi
    log_success "fio: OK"
  fi

  # Check mdtest if enabled
  if [[ "${RUN_MDTEST:-0}" == "1" ]]; then
    if ! ssh_run "$PRIMARY_CLIENT" "command -v mdtest >/dev/null 2>&1"; then
      log_error "mdtest not found. Please install mdtest manually or set RUN_MDTEST=0"
      die "Required dependency missing: mdtest"
    fi
    log_success "mdtest: OK"
  fi
}

prepare() {
  log_info "Preparing environment"

  check_dependencies

  # Create remote work directories (not RUN_ID specific)
  for node in $CLIENT_NODES; do
    ssh_exec "$node" "mkdir -p '${REMOTE_WORKDIR}'"
  done
  log_success "Environment prepared"
}

deploy_brewfs() {
  if [[ "${BREWFS_BUILD}" == "1" ]]; then
    brewfs_build_local "${REPO_ROOT}"
  fi

  local bin_local
  bin_local="$(brewfs_local_binary "${REPO_ROOT}")"
  if [[ ! -f "$bin_local" ]]; then
    die "BrewFS binary not found: ${bin_local}"
  fi

  for node in $CLIENT_NODES; do
    log_info "Deploy BrewFS to ${node}"
    brewfs_prepare_node "$node"
    brewfs_deploy_binary "$node" "$bin_local"
    brewfs_deploy_config "$node"
    brewfs_start_node "$node"
  done

  for node in $CLIENT_NODES; do
    log_info "Wait for BrewFS mount on ${node}"
    brewfs_wait_mount "$node"
  done
  log_success "BrewFS deployed"
}

test_brewfs() {
  log_info "Run workloads on BrewFS"

  # Check if BrewFS is mounted on primary client
  if ! ssh_run "$PRIMARY_CLIENT" "mountpoint -q '${BREWFS_MOUNT_DIR}'"; then
    log_error "BrewFS is not mounted on ${PRIMARY_CLIENT}. Please run 'deploy-brewfs' first."
    return 1
  fi

  # Create remote test result directory with current RUN_ID
  log_info "Creating test result directory: ${RUN_ID}"
  for node in $CLIENT_NODES; do
    ssh_exec "$node" "mkdir -p '${REMOTE_RESULTS_DIR}/${RUN_ID}'"
  done

  # Run tests
  run_tests_for_fs "brewfs" "${BREWFS_MOUNT_DIR}"

  # Display completion message with RUN_ID
  echo ""
  echo "========================================"
  log_success "BrewFS tests completed!"
  echo "RUN_ID: ${RUN_ID}"
  echo ""
  echo "To collect results, run:"
  echo "  ./run-distributed-tests.sh collect"
  echo "========================================"
  echo ""
}

collect_results() {
  # Auto-detect: find the latest directory based on enabled tests
  local remote_run_id
  local test_pattern=""

  # Build search pattern based on enabled tests
  if [[ "${RUN_FIO:-0}" == "1" ]]; then
    test_pattern="${test_pattern}|fio"
  fi
  if [[ "${RUN_MDTEST:-0}" == "1" ]]; then
    test_pattern="${test_pattern}|mdtest"
  fi
  if [[ "${RUN_IOZONE:-0}" == "1" ]]; then
    test_pattern="${test_pattern}|iozone"
  fi
  if [[ "${RUN_XFSTESTS:-0}" == "1" ]]; then
    test_pattern="${test_pattern}|xfstests"
  fi

  # Remove leading pipe
  test_pattern="${test_pattern#|}"

  # If no tests enabled, fall back to latest directory
  if [[ -z "$test_pattern" ]]; then
    log_warn "No tests enabled in config, using latest directory"
    remote_run_id=$(ssh_run "$PRIMARY_CLIENT" "ls -t '${REMOTE_RESULTS_DIR}' 2>/dev/null | head -1" | tr -d '\n\r')
  else
    remote_run_id=$(ssh_run "$PRIMARY_CLIENT" "
      for dir in \$(ls -t '${REMOTE_RESULTS_DIR}' 2>/dev/null); do
        if find '${REMOTE_RESULTS_DIR}'/\${dir}/brewfs/ -type d 2>/dev/null | egrep -q '(${test_pattern})'; then
          echo \"\${dir}\"
          break
        fi
      done
    " | tr -d '\n\r')
  fi

  if [[ -z "$remote_run_id" ]]; then
    log_error "No test results found on ${PRIMARY_CLIENT}"
    log_info "Available directories:"
    ssh_run "$PRIMARY_CLIENT" "ls -lt '${REMOTE_RESULTS_DIR}'" 2>/dev/null || true
    return 1
  fi

  log_info "Auto-detected run-id: ${remote_run_id}"

  local local_dir="${RESULTS_DIR}/${remote_run_id}"
  ensure_dir "$local_dir"

  # Copy cluster config
  cp "${CONFIG_PATH}" "${local_dir}/cluster.env"

  # Collect test results from primary client based on enabled tests
  log_info "Collecting test results from ${PRIMARY_CLIENT}"
  ensure_dir "${local_dir}/brewfs"

  if [[ "${RUN_FIO:-0}" == "1" ]]; then
    scp_from_dir "$PRIMARY_CLIENT" "${REMOTE_RESULTS_DIR}/${remote_run_id}/brewfs/fio" "${local_dir}/brewfs/" 2>/dev/null || log_warn "Failed to pull fio results from ${PRIMARY_CLIENT}"
  fi
  if [[ "${RUN_MDTEST:-0}" == "1" ]]; then
    scp_from_dir "$PRIMARY_CLIENT" "${REMOTE_RESULTS_DIR}/${remote_run_id}/brewfs/mdtest" "${local_dir}/brewfs/" 2>/dev/null || log_warn "Failed to pull mdtest results from ${PRIMARY_CLIENT}"
  fi
  if [[ "${RUN_IOZONE:-0}" == "1" ]]; then
    scp_from_dir "$PRIMARY_CLIENT" "${REMOTE_RESULTS_DIR}/${remote_run_id}/brewfs/iozone" "${local_dir}/brewfs/" 2>/dev/null || log_warn "Failed to pull iozone results from ${PRIMARY_CLIENT}"
  fi
  if [[ "${RUN_XFSTESTS:-0}" == "1" ]]; then
    scp_from_dir "$PRIMARY_CLIENT" "${REMOTE_RESULTS_DIR}/${remote_run_id}/brewfs/xfstests" "${local_dir}/brewfs/" 2>/dev/null || log_warn "Failed to pull xfstests results from ${PRIMARY_CLIENT}"
  fi

  # Collect logs directly from all nodes
  ensure_dir "${local_dir}/logs"
  for node in $CLIENT_NODES $META_NODES; do
    log_info "  Collecting logs from ${node}"

    # Collect BrewFS logs
    if [[ -n "${BREWFS_LOG_DIR:-}" ]]; then
      # Get list of log files from remote
      local log_files
      log_files=$(ssh_run "$node" "ls '${BREWFS_LOG_DIR}'/brewfs-*.log 2>/dev/null" || true)
      if [[ -n "$log_files" ]]; then
        while IFS= read -r log_file; do
          [[ -z "$log_file" ]] && continue
          local basename
          basename=$(basename "$log_file")
          # Copy to REMOTE_WORKDIR with proper permissions, then scp to local
          ssh_run "$node" "cp -a '${log_file}' '${REMOTE_WORKDIR}/brewfs-log-${basename}' && chown '${SSH_USER}' '${REMOTE_WORKDIR}/brewfs-log-${basename}'"
          scp_from "$node" "${REMOTE_WORKDIR}/brewfs-log-${basename}" "${local_dir}/logs/${basename}" 2>/dev/null || true
          ssh_run "$node" "rm -f '${REMOTE_WORKDIR}/brewfs-log-${basename}'" 2>/dev/null || true
        done <<< "$log_files"
      fi
    fi

    # Collect etcd logs
    if [[ "${BREWFS_META_BACKEND}" == "etcd" ]] && [[ -n "${ETCD_LOG_FILE:-}" ]]; then
      if ssh_run "$node" "[[ -f '${ETCD_LOG_FILE}' ]]"; then
        ssh_run "$node" "cp -a '${ETCD_LOG_FILE}' '${REMOTE_WORKDIR}/etcd.log' && chown '${SSH_USER}' '${REMOTE_WORKDIR}/etcd.log'" 2>/dev/null || true
        scp_from "$node" "${REMOTE_WORKDIR}/etcd.log" "${local_dir}/logs/etcd-${node}.log" 2>/dev/null || true
        ssh_run "$node" "rm -f '${REMOTE_WORKDIR}/etcd.log'" 2>/dev/null || true
      fi
    fi
  done

  log_success "Results collected into ${local_dir}"
}

cleanup() {
  for node in $CLIENT_NODES; do
    log_info "Stop BrewFS on ${node}"
    brewfs_stop_node "$node"
  done

  # Clean up data if CLEANUP_DATA is set
  if [[ "${CLEANUP_DATA:-0}" == "1" ]]; then
    log_info "Cleaning up files in BrewFS mount directories..."

    # Delete files inside /mnt/brewfs before unmounting
    for node in $CLIENT_NODES; do
      log_info "  Cleaning ${BREWFS_MOUNT_DIR} on ${node}"
      ssh_exec_sudo "$node" "find '${BREWFS_MOUNT_DIR}' -mindepth 1 -delete 2>/dev/null || true"
    done
  fi

  for node in $CLIENT_NODES; do
    brewfs_unmount_node "$node"
  done

  # Clean up data if CLEANUP_DATA is set
  if [[ "${CLEANUP_DATA:-0}" == "1" ]]; then
    log_info "Cleaning up BrewFS data..."

    # Clean etcd metadata
    if [[ -n "${META_NODES:-}" ]]; then
      log_info "  Cleaning etcd metadata on META_NODES..."
      for node in $META_NODES; do
        ssh_exec "$node" "etcdctl del --prefix 'brewfs' >/dev/null 2>&1 || true"
        ssh_exec "$node" "etcdctl del --prefix '/1/' >/dev/null 2>&1 || true"
        ssh_exec "$node" "etcdctl del --prefix '/brewfs' >/dev/null 2>&1 || true"
      done
    fi

    # Clean data directories on data nodes
    if [[ -n "${DATA_NODES:-}" ]]; then
      log_info "  Cleaning data directories on DATA_NODES..."
      for node in $DATA_NODES; do
        ssh_exec_sudo "$node" "rm -rf '${BREWFS_DATA_DIR}'/* 2>/dev/null || true"
        ssh_exec_sudo "$node" "rm -rf '${BREWFS_META_DIR}'/* 2>/dev/null || true"
      done
    fi

    # Clean data directories on client nodes
    log_info "  Cleaning data directories on CLIENT_NODES..."
    for node in $CLIENT_NODES; do
      ssh_exec_sudo "$node" "rm -rf '${BREWFS_DATA_DIR}'/* 2>/dev/null || true"
      ssh_exec_sudo "$node" "rm -rf '${BREWFS_META_DIR}'/* 2>/dev/null || true"
    done

    # Clean remote test results
    log_info "  Cleaning remote test results..."
    for node in $CLIENT_NODES; do
      ssh_exec_sudo "$node" "rm -rf '${REMOTE_RESULTS_DIR}'/* 2>/dev/null || true"
    done

    # Clean BrewFS logs
    if [[ -n "${BREWFS_LOG_DIR:-}" ]]; then
      log_info "  Cleaning BrewFS logs..."
      for node in $CLIENT_NODES $META_NODES; do
        ssh_exec_sudo "$node" "rm -f '${BREWFS_LOG_DIR}'/brewfs-*.log 2>/dev/null || true"
      done
    fi

    # Clean etcd logs
    if [[ -n "${ETCD_LOG_FILE:-}" ]]; then
      log_info "  Cleaning etcd logs..."
      for node in $META_NODES; do
        ssh_exec_sudo "$node" "rm -f '${ETCD_LOG_FILE}' 2>/dev/null || true"
      done
    fi

    log_success "Data cleanup complete"
  fi

  log_success "Cleanup complete"
}

case "$ACTION" in
  prepare)
    prepare
    ;;
  deploy-brewfs)
    prepare
    deploy_brewfs
    ;;
  test-brewfs)
    prepare
    test_brewfs
    ;;
  collect)
    collect_results
    ;;
  cleanup)
    cleanup
    ;;
  all)
    prepare
    deploy_brewfs
    test_brewfs
    collect_results
    ;;
  *)
    usage
    exit 1
    ;;
esac
