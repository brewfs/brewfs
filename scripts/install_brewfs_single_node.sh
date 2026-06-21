#!/usr/bin/env bash
#
# BrewFS single-node installer.
#
# This script installs and maintains a local BrewFS stack managed by systemd:
# Redis for metadata, RustFS for S3-compatible object storage, and BrewFS as
# the mounted filesystem. BrewFS release binaries are resolved from the same
# R2 layout used by the release workflow: brewfs/releases/<version>/brewfs-linux-<arch>.

set -euo pipefail

BREWFS_SERVICE="brewfs.service"
RUSTFS_SERVICE="brewfs-rustfs.service"
REDIS_SERVICE="brewfs-redis.service"

SYSTEMD_DIR="${SYSTEMD_DIR:-/etc/systemd/system}"
DEFAULT_DIR="${DEFAULT_DIR:-/etc/default}"
CONFIG_DIR="${CONFIG_DIR:-/etc/brewfs}"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"
STATE_DIR="${STATE_DIR:-/var/lib/brewfs}"
LOG_DIR="${LOG_DIR:-/var/log/brewfs}"

BREWFS_BIN="${BREWFS_BIN:-$INSTALL_DIR/brewfs}"
RUSTFS_BIN="${RUSTFS_BIN:-$INSTALL_DIR/rustfs}"
REDIS_BIN="${REDIS_BIN:-}"

BREWFS_RELEASE_REPO="${BREWFS_RELEASE_REPO:-brewfs/brewfs}"
BREWFS_BASE_URL="${BREWFS_BASE_URL:-https://download.brewfs.ai/brewfs/releases}"
BREWFS_VERSION="${BREWFS_VERSION:-}"
BREWFS_REQUIRE_CHECKSUM="${BREWFS_REQUIRE_CHECKSUM:-0}"
BREWFS_ALLOW_FALLBACK="${BREWFS_ALLOW_FALLBACK:-0}"
DEFAULT_BREWFS_VERSION="${DEFAULT_BREWFS_VERSION:-v0.1.0}"
BREWFS_DOWNLOAD_URL="${BREWFS_DOWNLOAD_URL:-}"
RUSTFS_DOWNLOAD_URL="${RUSTFS_DOWNLOAD_URL:-}"
REDIS_DOWNLOAD_URL="${REDIS_DOWNLOAD_URL:-}"

MOUNT_POINT="${MOUNT_POINT:-/mnt/brewfs}"
BREWFS_BUCKET="${BREWFS_BUCKET:-brewfs-data}"
BREWFS_REGION="${BREWFS_REGION:-us-east-1}"
BREWFS_CACHE_DIR="${BREWFS_CACHE_DIR:-$STATE_DIR/cache}"
BREWFS_CONFIG_FILE="${BREWFS_CONFIG_FILE:-$CONFIG_DIR/mount.yaml}"
AWS_CONFIG_FILE="${AWS_CONFIG_FILE:-$CONFIG_DIR/aws-config}"
BREWFS_ENV_FILE="$DEFAULT_DIR/brewfs"
RUSTFS_ENV_FILE="$DEFAULT_DIR/brewfs-rustfs"
REDIS_ENV_FILE="$DEFAULT_DIR/brewfs-redis"
REDIS_CONFIG_FILE="$CONFIG_DIR/redis.conf"

REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-6379}"
RUSTFS_HOST="${RUSTFS_HOST:-127.0.0.1}"
RUSTFS_PORT="${RUSTFS_PORT:-9000}"
RUSTFS_CONSOLE_PORT="${RUSTFS_CONSOLE_PORT:-9001}"
RUSTFS_ACCESS_KEY="${RUSTFS_ACCESS_KEY:-rustfsadmin}"
RUSTFS_SECRET_KEY="${RUSTFS_SECRET_KEY:-rustfsadmin}"

BREWFS_USER="${BREWFS_USER:-root}"
BREWFS_GROUP="${BREWFS_GROUP:-root}"

DOWNLOAD_CMD=""
PORT_CMD=""
OS=""
ARCH=""
OS_RAW=""
ARCH_RAW=""

err() {
  echo "[ERROR] $*" >&2
  exit 1
}

info() {
  echo "[INFO] $*"
}

warn() {
  echo "[WARN] $*" >&2
}

usage() {
  cat <<'EOF'
Usage: install_brewfs_single_node.sh [OPTIONS] [install|upgrade|uninstall|status|restart]

Options:
  -v, --version <VERSION>      Install BrewFS release version, e.g. v0.1.0.
  -h, --help                   Show this help message.

Environment overrides:
  BREWFS_VERSION=""            BrewFS release version. Defaults to latest GitHub release.
  BREWFS_BASE_URL=""           Release mirror base URL.
  BREWFS_RELEASE_REPO=brewfs/brewfs
                               GitHub repo used for latest-version detection.
  BREWFS_REQUIRE_CHECKSUM=0    Set to 1 to fail if <binary>.sha256 is missing.
  BREWFS_ALLOW_FALLBACK=0      Set to 1 to use DEFAULT_BREWFS_VERSION if latest
                               release detection is unreachable.
  DEFAULT_BREWFS_VERSION=v0.1.0
                               Opt-in fallback version.
  BREWFS_DOWNLOAD_URL=""       Explicit BrewFS binary archive or executable URL.
  RUSTFS_DOWNLOAD_URL=""      RustFS binary archive or executable URL.
  REDIS_DOWNLOAD_URL=""       Redis binary archive or executable URL.
  MOUNT_POINT=/mnt/brewfs
  STATE_DIR=/var/lib/brewfs
  LOG_DIR=/var/log/brewfs
  BREWFS_BUCKET=brewfs-data
  REDIS_PORT=6379
  RUSTFS_PORT=9000
  RUSTFS_CONSOLE_PORT=9001
  RUSTFS_ACCESS_KEY=rustfsadmin
  RUSTFS_SECRET_KEY=rustfsadmin

Notes:
  - Redis is maintained as brewfs-redis.service. If redis-server already exists
    on the host, this script uses it instead of downloading Redis.
  - If aws CLI is installed, brewfs.service creates/checks the RustFS bucket
    before mounting. The generated AWS config forces path-style S3 access.
  - If BREWFS_DOWNLOAD_URL is empty and /usr/local/bin/brewfs is missing, this
    script downloads brewfs-${OS}-${ARCH} from:
      ${BREWFS_BASE_URL}/${BREWFS_VERSION}/brewfs-${OS}-${ARCH}
    For example:
      https://download.brewfs.ai/brewfs/releases/v0.1.1/brewfs-darwin-arm64
  - Put an existing RustFS binary at /usr/local/bin/rustfs, or set
    RUSTFS_DOWNLOAD_URL.
  - Uninstall removes services and config files, but keeps data and logs.
EOF
}

need_root() {
  if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
    err "This script must be run as root. Please use sudo or switch to root."
  fi
}

find_command() {
  local var_name="$1"
  shift
  local cmd
  for cmd in "$@"; do
    if command -v "$cmd" >/dev/null 2>&1; then
      printf -v "$var_name" '%s' "$cmd"
      return 0
    fi
  done
  return 1
}

normalize_version() {
  local version="$1"
  case "$version" in
    v*|"") printf '%s' "$version" ;;
    *) printf 'v%s' "$version" ;;
  esac
}

detect_release_platform() {
  OS_RAW="$(uname -s)"
  case "$OS_RAW" in
    Linux) OS="linux" ;;
    *) err "Unsupported operating system: $OS_RAW. BrewFS release binaries currently target Linux." ;;
  esac

  ARCH_RAW="$(uname -m)"
  case "$ARCH_RAW" in
    x86_64|amd64) ARCH="amd64" ;;
    aarch64|arm64) ARCH="arm64" ;;
    *) err "Unsupported architecture: $ARCH_RAW. BrewFS release binaries target amd64 and arm64." ;;
  esac
}

fetch_latest_brewfs_version() {
  local api_url="https://api.github.com/repos/${BREWFS_RELEASE_REPO}/releases/latest"
  if [[ "$DOWNLOAD_CMD" == "curl" ]]; then
    curl -fsSL --connect-timeout 5 --max-time 10 "$api_url" 2>/dev/null \
      | grep '"tag_name":' | head -n1 \
      | sed 's/.*"tag_name": "\([^"]*\)".*/\1/'
  else
    wget -q --timeout=10 --tries=1 -O- "$api_url" 2>/dev/null \
      | grep '"tag_name":' | head -n1 \
      | sed 's/.*"tag_name": "\([^"]*\)".*/\1/'
  fi
}

resolve_brewfs_release_version() {
  if [[ -z "$BREWFS_VERSION" ]]; then
    BREWFS_VERSION="$(fetch_latest_brewfs_version || true)"
    if [[ -z "$BREWFS_VERSION" ]]; then
      if [[ "$BREWFS_ALLOW_FALLBACK" == "1" ]]; then
        BREWFS_VERSION="$DEFAULT_BREWFS_VERSION"
        warn "Could not detect latest BrewFS release; falling back to $BREWFS_VERSION."
      else
        err "Could not determine latest BrewFS release. Pass --version, set BREWFS_VERSION, or set BREWFS_ALLOW_FALLBACK=1."
      fi
    fi
  fi

  BREWFS_VERSION="$(normalize_version "$BREWFS_VERSION")"
}

preflight() {
  need_root

  local required=(systemctl mktemp find grep chmod mkdir install sed)
  local missing=()
  local cmd
  for cmd in "${required[@]}"; do
    command -v "$cmd" >/dev/null 2>&1 || missing+=("$cmd")
  done
  [[ "${#missing[@]}" -gt 0 ]] && err "Missing required commands: ${missing[*]}"

  find_command DOWNLOAD_CMD curl wget || true
  find_command PORT_CMD ss lsof netstat || true

  [[ "$(uname -s)" == "Linux" ]] || err "This installer supports Linux only."
  detect_release_platform
  if [[ ! -d /run/systemd/system ]]; then
    warn "systemd runtime directory was not found; service installation may fail."
  fi
}

port_in_use() {
  local port="$1"
  case "$PORT_CMD" in
    ss) ss -ltn "sport = :$port" | grep -q ":$port" ;;
    lsof) lsof -i ":$port" >/dev/null 2>&1 ;;
    netstat) netstat -ltn | grep -q ":$port[[:space:]]" ;;
    "") return 1 ;;
    *) return 1 ;;
  esac
}

check_ports() {
  local unit="$1"
  shift
  local port
  for port in "$@"; do
    if systemctl is-active --quiet "$unit" 2>/dev/null; then
      continue
    fi
    if port_in_use "$port"; then
      err "Port $port is already in use. Set another port or stop the conflicting service."
    fi
  done
}

download_to_tmp() {
  local url="$1"
  local output="$2"

  [[ -n "$DOWNLOAD_CMD" ]] || err "No downloader found. Install curl or wget."
  if [[ "$DOWNLOAD_CMD" == "curl" ]]; then
    curl -fsSL --connect-timeout 10 --max-time 300 -o "$output" "$url"
  else
    wget -q --timeout=30 --tries=3 -O "$output" "$url"
  fi
}

sha256_of() {
  local file="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file" 2>/dev/null | awk '{print $1; exit}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$file" 2>/dev/null | awk '{print $1; exit}'
  elif command -v openssl >/dev/null 2>&1; then
    openssl dgst -sha256 "$file" 2>/dev/null | awk '{print $NF; exit}'
  fi
}

verify_checksum() {
  local file="$1"
  local url="$2"
  local sum_url="${url}.sha256"
  local sum_file

  sum_file="$(mktemp)"
  if ! download_to_tmp "$sum_url" "$sum_file" 2>/dev/null; then
    rm -f "$sum_file"
    if [[ "$BREWFS_REQUIRE_CHECKSUM" == "1" ]]; then
      err "No checksum published at $sum_url."
    fi
    warn "Checksum not published at $sum_url; skipping verification."
    return 0
  fi

  local expected
  expected="$(awk '{print $1; exit}' "$sum_file" 2>/dev/null || true)"
  rm -f "$sum_file"
  [[ -n "$expected" ]] || err "Checksum file is empty or malformed: $sum_url"

  local actual
  actual="$(sha256_of "$file" || true)"
  if [[ -z "$actual" ]]; then
    if [[ "$BREWFS_REQUIRE_CHECKSUM" == "1" ]]; then
      err "No sha256 tool found. Install sha256sum, shasum, or openssl."
    fi
    warn "No sha256 tool found; skipping checksum verification."
    return 0
  fi

  [[ "$expected" == "$actual" ]] || err "Checksum mismatch for $url. Expected $expected, got $actual."
  info "Checksum verified for $(basename "$file")."
}

install_executable_from_url() {
  local name="$1"
  local url="$2"
  local destination="$3"

  info "Downloading $name from $url"
  local tmp_dir
  tmp_dir="$(mktemp -d)"
  local payload="$tmp_dir/$name"
  download_to_tmp "$url" "$payload"
  [[ -s "$payload" ]] || err "$name download is empty: $url"
  verify_checksum "$payload" "$url"
  chmod +x "$payload"
  install -m 0755 "$payload" "$destination"
  rm -rf "$tmp_dir"
  info "$name installed to $destination"
}

install_binary_from_url() {
  local name="$1"
  local url="$2"
  local destination="$3"

  [[ -n "$url" ]] || return 1

  info "Downloading $name from $url"
  local tmp_dir
  tmp_dir="$(mktemp -d)"
  trap 'rm -rf "$tmp_dir"' RETURN

  local payload="$tmp_dir/$name.download"
  download_to_tmp "$url" "$payload"

  case "$url" in
    *.tar.gz|*.tgz)
      command -v tar >/dev/null 2>&1 || err "tar is required for $url"
      tar -xzf "$payload" -C "$tmp_dir"
      ;;
    *.zip)
      command -v unzip >/dev/null 2>&1 || err "unzip is required for $url"
      unzip -q "$payload" -d "$tmp_dir"
      ;;
    *)
      chmod +x "$payload"
      install -m 0755 "$payload" "$destination"
      info "$name installed to $destination"
      return 0
      ;;
  esac

  local found
  found="$(find "$tmp_dir" -type f -name "$name" -perm /111 | head -n1 || true)"
  [[ -n "$found" ]] || found="$(find "$tmp_dir" -type f -name "$name" | head -n1 || true)"
  [[ -n "$found" ]] || err "$name binary not found in downloaded archive."
  install -m 0755 "$found" "$destination"
  info "$name installed to $destination"
}

resolve_redis_binary() {
  if [[ -n "$REDIS_BIN" ]]; then
    [[ -x "$REDIS_BIN" ]] || err "REDIS_BIN is not executable: $REDIS_BIN"
    return 0
  fi

  if command -v redis-server >/dev/null 2>&1; then
    REDIS_BIN="$(command -v redis-server)"
    return 0
  fi

  if install_binary_from_url "redis-server" "$REDIS_DOWNLOAD_URL" "$INSTALL_DIR/redis-server"; then
    REDIS_BIN="$INSTALL_DIR/redis-server"
    return 0
  fi

  err "redis-server not found and REDIS_DOWNLOAD_URL is empty. Install Redis or set REDIS_DOWNLOAD_URL."
}

install_brewfs_binary() {
  local force_download="${1:-0}"

  if [[ "$force_download" != "1" && -x "$BREWFS_BIN" && -z "$BREWFS_DOWNLOAD_URL" && -z "$BREWFS_VERSION" ]]; then
    info "Using existing BrewFS binary: $BREWFS_BIN"
    return 0
  fi

  if [[ -z "$BREWFS_DOWNLOAD_URL" ]]; then
    [[ -n "$DOWNLOAD_CMD" ]] || err "No downloader found. Install curl or wget."
    resolve_brewfs_release_version
    BREWFS_DOWNLOAD_URL="${BREWFS_BASE_URL}/${BREWFS_VERSION}/brewfs-${OS}-${ARCH}"
  fi

  case "$BREWFS_DOWNLOAD_URL" in
    *.tar.gz|*.tgz|*.zip)
      install_binary_from_url "brewfs" "$BREWFS_DOWNLOAD_URL" "$BREWFS_BIN"
      ;;
    *)
      install_executable_from_url "brewfs" "$BREWFS_DOWNLOAD_URL" "$BREWFS_BIN"
      ;;
  esac

  if [[ -x "$BREWFS_BIN" ]]; then
    info "BrewFS release source: $BREWFS_DOWNLOAD_URL"
    return 0
  fi

  err "BrewFS binary installation failed: $BREWFS_BIN"
}

install_rustfs_binary() {
  if [[ -x "$RUSTFS_BIN" ]]; then
    info "Using existing RustFS binary: $RUSTFS_BIN"
    return 0
  fi

  if install_binary_from_url "rustfs" "$RUSTFS_DOWNLOAD_URL" "$RUSTFS_BIN"; then
    return 0
  fi

  err "RustFS binary not found at $RUSTFS_BIN and RUSTFS_DOWNLOAD_URL is empty."
}

create_directories() {
  install -d -m 0755 "$CONFIG_DIR" "$DEFAULT_DIR" "$SYSTEMD_DIR" "$LOG_DIR"
  install -d -m 0755 "$STATE_DIR" "$STATE_DIR/redis" "$STATE_DIR/rustfs" "$BREWFS_CACHE_DIR" "$MOUNT_POINT"
}

write_redis_config() {
  cat >"$REDIS_CONFIG_FILE" <<EOF
bind $REDIS_HOST
port $REDIS_PORT
dir $STATE_DIR/redis
appendonly yes
appendfilename "appendonly.aof"
save 900 1
save 300 10
save 60 10000
protected-mode yes
daemonize no
supervised no
loglevel notice
logfile ""
EOF
}

write_env_files() {
  cat >"$AWS_CONFIG_FILE" <<EOF
[default]
s3 =
  addressing_style = path
EOF

  cat >"$REDIS_ENV_FILE" <<EOF
REDIS_BIN="$REDIS_BIN"
REDIS_CONFIG_FILE="$REDIS_CONFIG_FILE"
EOF

  cat >"$RUSTFS_ENV_FILE" <<EOF
RUSTFS_ACCESS_KEY="$RUSTFS_ACCESS_KEY"
RUSTFS_SECRET_KEY="$RUSTFS_SECRET_KEY"
RUSTFS_VOLUMES="$STATE_DIR/rustfs"
RUSTFS_ADDRESS=":$RUSTFS_PORT"
RUSTFS_CONSOLE_ADDRESS=":$RUSTFS_CONSOLE_PORT"
RUSTFS_CONSOLE_ENABLE=true
RUSTFS_OBS_LOGGER_LEVEL=error
RUSTFS_OBS_LOG_DIRECTORY="$LOG_DIR/"
EOF

  cat >"$BREWFS_ENV_FILE" <<EOF
BREWFS_BIN="$BREWFS_BIN"
BREWFS_CONFIG_FILE="$BREWFS_CONFIG_FILE"
AWS_ACCESS_KEY_ID="$RUSTFS_ACCESS_KEY"
AWS_SECRET_ACCESS_KEY="$RUSTFS_SECRET_KEY"
AWS_REGION="$BREWFS_REGION"
AWS_DEFAULT_REGION="$BREWFS_REGION"
AWS_CONFIG_FILE="$AWS_CONFIG_FILE"
AWS_EC2_METADATA_DISABLED=true
RUST_LOG=info
EOF
}

write_brewfs_config() {
  cat >"$BREWFS_CONFIG_FILE" <<EOF
mount_point: $MOUNT_POINT
data:
  backend: s3
  s3:
    bucket: $BREWFS_BUCKET
    endpoint: http://$RUSTFS_HOST:$RUSTFS_PORT
    region: $BREWFS_REGION
    force_path_style: true
    disable_payload_checksum: true
meta:
  backend: redis
  redis:
    url: redis://$REDIS_HOST:$REDIS_PORT/0
  open_file_cache_ttl_ms: 30000
  open_file_cache_capacity: 65536
cache:
  root: $BREWFS_CACHE_DIR
  writeback_mode: commit_before_upload
  writeback_persist_sync: false
  prefetch_enabled: true
fuse:
  workers: 1
  max_background: 512
  privileged: true
EOF
}

write_systemd_units() {
  cat >"$SYSTEMD_DIR/$REDIS_SERVICE" <<EOF
[Unit]
Description=BrewFS Redis Metadata Server
Documentation=https://github.com/redis/redis
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=$REDIS_ENV_FILE
ExecStart=$REDIS_BIN $REDIS_CONFIG_FILE
WorkingDirectory=$STATE_DIR/redis
User=$BREWFS_USER
Group=$BREWFS_GROUP
Restart=always
RestartSec=3s
LimitNOFILE=1048576
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ProtectHome=true
ReadWritePaths=$STATE_DIR/redis $LOG_DIR

[Install]
WantedBy=multi-user.target
EOF

  cat >"$SYSTEMD_DIR/$RUSTFS_SERVICE" <<EOF
[Unit]
Description=BrewFS RustFS Object Storage Server
Documentation=https://rustfs.com/docs/
After=network-online.target
Wants=network-online.target

[Service]
Type=notify
NotifyAccess=main
EnvironmentFile=$RUSTFS_ENV_FILE
ExecStart=$RUSTFS_BIN \${RUSTFS_VOLUMES}
WorkingDirectory=$STATE_DIR/rustfs
User=$BREWFS_USER
Group=$BREWFS_GROUP
Restart=always
RestartSec=5s
LimitNOFILE=1048576
TasksMax=infinity
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true
ReadWritePaths=$STATE_DIR/rustfs $LOG_DIR
StandardOutput=append:$LOG_DIR/rustfs.log
StandardError=append:$LOG_DIR/rustfs-err.log

[Install]
WantedBy=multi-user.target
EOF

  cat >"$SYSTEMD_DIR/$BREWFS_SERVICE" <<EOF
[Unit]
Description=BrewFS Single Node Mount
Documentation=https://github.com/slayerfs/brewfs
After=network-online.target $REDIS_SERVICE $RUSTFS_SERVICE
Wants=network-online.target $REDIS_SERVICE $RUSTFS_SERVICE
Requires=$REDIS_SERVICE $RUSTFS_SERVICE

[Service]
Type=simple
EnvironmentFile=$BREWFS_ENV_FILE
ExecStartPre=/bin/sh -c 'if command -v aws >/dev/null 2>&1; then timeout 180 sh -ec '"'"'until aws --endpoint-url http://$RUSTFS_HOST:$RUSTFS_PORT s3api create-bucket --bucket $BREWFS_BUCKET >/dev/null 2>&1 || aws --endpoint-url http://$RUSTFS_HOST:$RUSTFS_PORT s3api head-bucket --bucket $BREWFS_BUCKET >/dev/null 2>&1; do sleep 2; done'"'"'; else echo "aws CLI not found; skipping RustFS bucket initialization"; fi'
ExecStart=$BREWFS_BIN mount --config $BREWFS_CONFIG_FILE
ExecStop=/bin/sh -c 'if command -v fusermount3 >/dev/null 2>&1; then fusermount3 -u $MOUNT_POINT; else umount $MOUNT_POINT; fi'
WorkingDirectory=$STATE_DIR
User=$BREWFS_USER
Group=$BREWFS_GROUP
Restart=always
RestartSec=5s
LimitNOFILE=1048576
TasksMax=infinity
NoNewPrivileges=true
PrivateTmp=true
ReadWritePaths=$STATE_DIR $MOUNT_POINT $LOG_DIR
StandardOutput=append:$LOG_DIR/brewfs.log
StandardError=append:$LOG_DIR/brewfs-err.log

[Install]
WantedBy=multi-user.target
EOF
}

daemon_reload() {
  systemctl daemon-reload
}

enable_and_start() {
  systemctl enable "$REDIS_SERVICE" "$RUSTFS_SERVICE" "$BREWFS_SERVICE"
  systemctl restart "$REDIS_SERVICE"
  systemctl restart "$RUSTFS_SERVICE"
  systemctl restart "$BREWFS_SERVICE"
}

install_stack() {
  preflight
  create_directories
  resolve_redis_binary
  install_rustfs_binary
  install_brewfs_binary
  check_ports "$REDIS_SERVICE" "$REDIS_PORT"
  check_ports "$RUSTFS_SERVICE" "$RUSTFS_PORT" "$RUSTFS_CONSOLE_PORT"
  write_redis_config
  write_env_files
  write_brewfs_config
  write_systemd_units
  daemon_reload
  enable_and_start

  info "BrewFS single-node stack installed."
  info "Mount point: $MOUNT_POINT"
  info "Redis: redis://$REDIS_HOST:$REDIS_PORT/0"
  info "RustFS endpoint: http://$RUSTFS_HOST:$RUSTFS_PORT"
  info "BrewFS config: $BREWFS_CONFIG_FILE"
}

upgrade_stack() {
  preflight
  create_directories
  resolve_redis_binary
  install_binary_from_url "redis-server" "$REDIS_DOWNLOAD_URL" "$INSTALL_DIR/redis-server" || true
  install_binary_from_url "rustfs" "$RUSTFS_DOWNLOAD_URL" "$RUSTFS_BIN" || true
  install_brewfs_binary 1
  write_redis_config
  write_env_files
  write_brewfs_config
  write_systemd_units
  daemon_reload
  systemctl restart "$REDIS_SERVICE" "$RUSTFS_SERVICE" "$BREWFS_SERVICE"
  info "BrewFS single-node stack upgraded/restarted."
}

restart_stack() {
  preflight
  systemctl restart "$REDIS_SERVICE" "$RUSTFS_SERVICE" "$BREWFS_SERVICE"
  status_stack
}

status_stack() {
  systemctl --no-pager --full status "$REDIS_SERVICE" "$RUSTFS_SERVICE" "$BREWFS_SERVICE"
}

uninstall_stack() {
  preflight
  warn "This removes BrewFS systemd units and config files. Data and logs are kept."
  read -r -p "Continue uninstall? [y/N]: " confirmation
  case "$confirmation" in
    y|Y|yes|YES)
      ;;
    *)
      info "Uninstall cancelled."
      return 0
      ;;
  esac

  systemctl stop "$BREWFS_SERVICE" "$RUSTFS_SERVICE" "$REDIS_SERVICE" 2>/dev/null || true
  systemctl disable "$BREWFS_SERVICE" "$RUSTFS_SERVICE" "$REDIS_SERVICE" 2>/dev/null || true

  rm -f "$SYSTEMD_DIR/$BREWFS_SERVICE" \
        "$SYSTEMD_DIR/$RUSTFS_SERVICE" \
        "$SYSTEMD_DIR/$REDIS_SERVICE" \
        "$BREWFS_ENV_FILE" \
        "$RUSTFS_ENV_FILE" \
        "$REDIS_ENV_FILE" \
        "$BREWFS_CONFIG_FILE" \
        "$REDIS_CONFIG_FILE" \
        "$AWS_CONFIG_FILE"

  daemon_reload
  systemctl reset-failed "$BREWFS_SERVICE" "$RUSTFS_SERVICE" "$REDIS_SERVICE" 2>/dev/null || true
  info "Uninstalled service units and config files."
  info "Kept data directory: $STATE_DIR"
  info "Kept log directory: $LOG_DIR"
}

main() {
  local action="install"
  local action_set=0

  while [[ $# -gt 0 ]]; do
    case "$1" in
      -v|--version)
        [[ $# -ge 2 ]] || err "Missing argument for $1."
        BREWFS_VERSION="$2"
        shift 2
        ;;
      -h|--help|help)
        usage
        exit 0
        ;;
      install|upgrade|uninstall|status|restart)
        [[ "$action_set" -eq 0 ]] || err "Only one action can be specified."
        action="$1"
        action_set=1
        shift
        ;;
      *)
        usage
        err "Unknown option or action: $1"
        ;;
    esac
  done

  case "$action" in
    install)
      install_stack
      ;;
    upgrade)
      upgrade_stack
      ;;
    uninstall)
      uninstall_stack
      ;;
    status)
      status_stack
      ;;
    restart)
      restart_stack
      ;;
    *)
      usage
      err "Unknown action: $action"
      ;;
  esac
}

main "$@"
