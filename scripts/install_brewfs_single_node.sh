#!/usr/bin/env bash
#
# BrewFS single-node installer.
#
# This script installs and maintains a local BrewFS stack managed by systemd:
# Redis for metadata, RustFS for S3-compatible object storage, and BrewFS as
# the mounted filesystem. BrewFS release binaries are resolved from the public
# R2 download layout: brewfs/releases/<version>/brewfs-linux-<arch>.

set -euo pipefail

BREWFS_SERVICE="brewfs.service"
RUSTFS_SERVICE="brewfs-rustfs.service"
REDIS_SERVICE="brewfs-redis.service"

SYSTEMD_DIR="${SYSTEMD_DIR:-/etc/systemd/system}"
DEFAULT_DIR="${DEFAULT_DIR:-/etc/default}"
CONFIG_DIR="${CONFIG_DIR:-/etc/brewfs}"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"
LIBEXEC_DIR="${LIBEXEC_DIR:-/usr/local/libexec/brewfs}"
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
DEFAULT_BREWFS_VERSION="${DEFAULT_BREWFS_VERSION:-v0.1.2}"
BREWFS_DOWNLOAD_URL="${BREWFS_DOWNLOAD_URL:-}"
BREWFS_INSTALL_PACKAGES="${BREWFS_INSTALL_PACKAGES:-1}"

RUSTFS_RELEASE_REPO="${RUSTFS_RELEASE_REPO:-rustfs/rustfs}"
RUSTFS_VERSION="${RUSTFS_VERSION:-}"
DEFAULT_RUSTFS_VERSION="${DEFAULT_RUSTFS_VERSION:-1.0.0-beta.8}"
RUSTFS_REQUIRE_CHECKSUM="${RUSTFS_REQUIRE_CHECKSUM:-0}"
RUSTFS_INSTALLER_URL="${RUSTFS_INSTALLER_URL:-https://rustfs.com/install_rustfs.sh}"
RUSTFS_DIST_BASE_URL="${RUSTFS_DIST_BASE_URL:-https://dl.rustfs.com/artifacts/rustfs/release}"
RUSTFS_FLAVOR="${RUSTFS_FLAVOR:-musl}"
RUSTFS_DOWNLOAD_URL="${RUSTFS_DOWNLOAD_URL:-}"
REDIS_REQUIRE_CHECKSUM="${REDIS_REQUIRE_CHECKSUM:-0}"
REDIS_DOWNLOAD_URL="${REDIS_DOWNLOAD_URL:-}"

MOUNT_POINT="${MOUNT_POINT:-/mnt/brewfs}"
BREWFS_BUCKET="${BREWFS_BUCKET:-brewfs-data}"
BREWFS_REGION="${BREWFS_REGION:-us-east-1}"
BREWFS_CACHE_DIR="${BREWFS_CACHE_DIR:-$STATE_DIR/cache}"
BREWFS_CONFIG_FILE="${BREWFS_CONFIG_FILE:-$CONFIG_DIR/mount.yaml}"
AWS_CONFIG_FILE="${AWS_CONFIG_FILE:-$CONFIG_DIR/aws-config}"
BUCKET_INIT_SCRIPT="${BUCKET_INIT_SCRIPT:-$LIBEXEC_DIR/init-rustfs-bucket}"
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

BREWFS_TUNING_PROFILE="${BREWFS_TUNING_PROFILE:-balanced}"
BREWFS_META_OPEN_FILE_CACHE_TTL_MS="${BREWFS_META_OPEN_FILE_CACHE_TTL_MS:-}"
BREWFS_META_OPEN_FILE_CACHE_CAPACITY="${BREWFS_META_OPEN_FILE_CACHE_CAPACITY:-}"
BREWFS_S3_PART_SIZE="${BREWFS_S3_PART_SIZE:-}"
BREWFS_S3_MAX_CONCURRENCY="${BREWFS_S3_MAX_CONCURRENCY:-}"
BREWFS_S3_FORCE_PATH_STYLE="${BREWFS_S3_FORCE_PATH_STYLE:-true}"
BREWFS_S3_DISABLE_PAYLOAD_CHECKSUM="${BREWFS_S3_DISABLE_PAYLOAD_CHECKSUM:-true}"
BREWFS_FUSE_WORKERS="${BREWFS_FUSE_WORKERS:-}"
BREWFS_FUSE_MAX_BACKGROUND="${BREWFS_FUSE_MAX_BACKGROUND:-}"
BREWFS_FUSE_PRIVILEGED="${BREWFS_FUSE_PRIVILEGED:-true}"
BREWFS_READ_MEMORY_BYTES="${BREWFS_READ_MEMORY_BYTES:-}"
BREWFS_READ_SSD_BYTES="${BREWFS_READ_SSD_BYTES:-}"
BREWFS_WRITE_MEMORY_BYTES="${BREWFS_WRITE_MEMORY_BYTES:-}"
BREWFS_WRITE_SSD_BYTES="${BREWFS_WRITE_SSD_BYTES:-}"
BREWFS_DIRTY_SLICE_TARGET_SIZE="${BREWFS_DIRTY_SLICE_TARGET_SIZE:-}"
BREWFS_DIRTY_SLICE_MAX_AGE_MS="${BREWFS_DIRTY_SLICE_MAX_AGE_MS:-}"
BREWFS_UPLOAD_CONCURRENCY="${BREWFS_UPLOAD_CONCURRENCY:-}"
BREWFS_PREFETCH_ENABLED="${BREWFS_PREFETCH_ENABLED:-}"
BREWFS_PREFETCH_MAX_BYTES="${BREWFS_PREFETCH_MAX_BYTES:-}"
BREWFS_PREFETCH_CONCURRENCY="${BREWFS_PREFETCH_CONCURRENCY:-}"
BREWFS_RANGE_BACKGROUND_PREFETCH="${BREWFS_RANGE_BACKGROUND_PREFETCH:-}"
BREWFS_POPULATE_WRITE_CACHE_AFTER_UPLOAD="${BREWFS_POPULATE_WRITE_CACHE_AFTER_UPLOAD:-}"
BREWFS_PERSIST_WRITE_CACHE_AFTER_UPLOAD="${BREWFS_PERSIST_WRITE_CACHE_AFTER_UPLOAD:-}"
BREWFS_MEMORY_BUDGET_BYTES="${BREWFS_MEMORY_BUDGET_BYTES:-}"
BREWFS_COMPRESSION="${BREWFS_COMPRESSION:-}"
BREWFS_VERIFY_CACHE_CHECKSUM="${BREWFS_VERIFY_CACHE_CHECKSUM:-}"
BREWFS_WRITEBACK_MODE="${BREWFS_WRITEBACK_MODE:-}"
BREWFS_WRITEBACK_PERSIST_SYNC="${BREWFS_WRITEBACK_PERSIST_SYNC:-}"
BREWFS_WRITEBACK_REQUIRE_STAGE_BEFORE_COMMIT="${BREWFS_WRITEBACK_REQUIRE_STAGE_BEFORE_COMMIT:-}"
BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES="${BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES:-}"
BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES="${BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES:-}"
BREWFS_CACHE_TTL_MS="${BREWFS_CACHE_TTL_MS:-1000}"
BREWFS_LOG_LEVEL="${BREWFS_LOG_LEVEL:-${RUST_LOG:-brewfs=info}}"

BREWFS_USER="${BREWFS_USER:-root}"
BREWFS_GROUP="${BREWFS_GROUP:-root}"

DOWNLOAD_CMD=""
PORT_CMD=""
OS=""
ARCH=""
OS_RAW=""
ARCH_RAW=""
APT_UPDATED=0

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
  BREWFS_VERSION=""            BrewFS release version. Defaults to the newest tag
                               with a downloadable release artifact.
  BREWFS_BASE_URL=""           Release mirror base URL.
  BREWFS_RELEASE_REPO=brewfs/brewfs
                               GitHub repo used for release/tag detection.
  BREWFS_REQUIRE_CHECKSUM=0    Set to 1 to fail if <binary>.sha256 is missing.
  BREWFS_ALLOW_FALLBACK=0      Set to 1 to use DEFAULT_BREWFS_VERSION if latest
                               release detection is unreachable.
  DEFAULT_BREWFS_VERSION=v0.1.2
                               Opt-in fallback version.
  BREWFS_DOWNLOAD_URL=""       Explicit BrewFS binary archive or executable URL.
  BREWFS_INSTALL_PACKAGES=1    Install missing OS packages with apt-get when possible.
  RUSTFS_VERSION=""            Optional RustFS GitHub release version.
  RUSTFS_RELEASE_REPO=rustfs/rustfs
  RUSTFS_INSTALLER_URL=https://rustfs.com/install_rustfs.sh
                               Official interactive RustFS installer reference.
  RUSTFS_DIST_BASE_URL=https://dl.rustfs.com/artifacts/rustfs/release
                               RustFS binary mirror used by the official installer.
  RUSTFS_FLAVOR=musl           musl or gnu. Defaults to the official installer default.
  RUSTFS_REQUIRE_CHECKSUM=0    Set to 1 to require <archive>.sha256 for RustFS.
  RUSTFS_DOWNLOAD_URL=""       RustFS binary archive or executable URL.
  REDIS_REQUIRE_CHECKSUM=0     Set to 1 to require <archive>.sha256 for Redis URL.
  REDIS_DOWNLOAD_URL=""       Redis binary archive or executable URL.
  BREWFS_TUNING_PROFILE=balanced
                               compat, balanced, or throughput.
  BREWFS_FUSE_WORKERS=""       Override generated fuse.workers.
  BREWFS_S3_MAX_CONCURRENCY="" Override generated data.s3.max_concurrency.
  BREWFS_UPLOAD_CONCURRENCY="" Override generated cache.upload_concurrency.
  BREWFS_MEMORY_BUDGET_BYTES="" Override generated cache.memory_budget_bytes.
  MOUNT_POINT=/mnt/brewfs
  STATE_DIR=/var/lib/brewfs
  LOG_DIR=/var/log/brewfs
  BREWFS_BUCKET=brewfs-data
  REDIS_PORT=6379
  RUSTFS_PORT=9000
  RUSTFS_CONSOLE_PORT=9001
  RUSTFS_ACCESS_KEY=rustfsadmin
  RUSTFS_SECRET_KEY=rustfsadmin
  BREWFS_S3_PART_SIZE=16777216
  BREWFS_S3_MAX_CONCURRENCY=32
  BREWFS_S3_FORCE_PATH_STYLE=true
  BREWFS_S3_DISABLE_PAYLOAD_CHECKSUM=true
  BREWFS_META_OPEN_FILE_CACHE_TTL_MS=30000
  BREWFS_META_OPEN_FILE_CACHE_CAPACITY=65536
  BREWFS_WRITEBACK_MODE=commit_before_upload
                               Use upload_before_commit for safer crash behavior.
  BREWFS_WRITEBACK_PERSIST_SYNC=false
  BREWFS_PREFETCH_ENABLED=true
  BREWFS_CACHE_TTL_MS=1000     FUSE attr/entry TTL in milliseconds.
  BREWFS_FUSE_WORKERS=1
  BREWFS_FUSE_MAX_BACKGROUND=512
  BREWFS_FUSE_PRIVILEGED=true
  BREWFS_LOG_LEVEL=brewfs=info

Notes:
  - Redis is maintained as brewfs-redis.service. If redis-server already exists
    on the host, this script uses it instead of downloading Redis.
  - If aws CLI is installed, brewfs.service creates/checks the RustFS bucket
    before mounting. The generated AWS config forces path-style S3 access.
  - If BREWFS_DOWNLOAD_URL is empty, this script downloads the newest BrewFS
    release/tag that has a published brewfs-${OS}-${ARCH} artifact at:
      ${BREWFS_BASE_URL}/${BREWFS_VERSION}/brewfs-${OS}-${ARCH}
    For example:
      https://download.brewfs.ai/brewfs/releases/v0.1.2/brewfs-linux-amd64
  - If RUSTFS_DOWNLOAD_URL is empty, this script uses the RustFS official
    installer source ($RUSTFS_INSTALLER_URL) as the reference and downloads the
    matching latest Linux archive from $RUSTFS_DIST_BASE_URL.
  - MOUNT_POINT must be empty and not already mounted.
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

apt_install_once() {
  [[ "$BREWFS_INSTALL_PACKAGES" == "1" ]] || return 1
  command -v apt-get >/dev/null 2>&1 || return 1

  if [[ "$APT_UPDATED" -eq 0 ]]; then
    info "Refreshing apt package metadata."
    DEBIAN_FRONTEND=noninteractive apt-get update -y
    APT_UPDATED=1
  fi

  info "Installing missing packages: $*"
  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "$@"
}

ensure_command() {
  local cmd="$1"
  shift

  if command -v "$cmd" >/dev/null 2>&1; then
    return 0
  fi

  if [[ "$#" -gt 0 ]] && apt_install_once "$@"; then
    command -v "$cmd" >/dev/null 2>&1 && return 0
  fi

  return 1
}

download_stdout() {
  local url="$1"

  [[ -n "$DOWNLOAD_CMD" ]] || err "No downloader found. Install curl or wget."
  if [[ "$DOWNLOAD_CMD" == "curl" ]]; then
    curl -fsSL --connect-timeout 10 --max-time 30 "$url"
  else
    wget -q --timeout=30 --tries=2 -O- "$url"
  fi
}

url_exists() {
  local url="$1"

  [[ -n "$DOWNLOAD_CMD" ]] || return 1
  if [[ "$DOWNLOAD_CMD" == "curl" ]]; then
    curl -fsIL --connect-timeout 5 --max-time 15 -o /dev/null "$url" >/dev/null 2>&1
  else
    wget -q --spider --timeout=15 --tries=1 "$url" >/dev/null 2>&1
  fi
}

set_default() {
  local var_name="$1"
  local default_value="$2"

  if [[ -z "${!var_name:-}" ]]; then
    printf -v "$var_name" '%s' "$default_value"
  fi
}

cpu_count() {
  getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || printf '2\n'
}

bounded_workers() {
  local requested="$1"
  local minimum="$2"
  local maximum="$3"

  if (( requested < minimum )); then
    printf '%s' "$minimum"
  elif (( requested > maximum )); then
    printf '%s' "$maximum"
  else
    printf '%s' "$requested"
  fi
}

apply_tuning_profile() {
  local cpus workers
  cpus="$(cpu_count)"

  set_default BREWFS_META_OPEN_FILE_CACHE_TTL_MS 30000
  set_default BREWFS_META_OPEN_FILE_CACHE_CAPACITY 65536
  set_default BREWFS_S3_PART_SIZE 16777216
  set_default BREWFS_FUSE_MAX_BACKGROUND 512
  set_default BREWFS_WRITEBACK_MODE commit_before_upload
  set_default BREWFS_WRITEBACK_PERSIST_SYNC false
  set_default BREWFS_PREFETCH_ENABLED true
  set_default BREWFS_RANGE_BACKGROUND_PREFETCH true
  set_default BREWFS_POPULATE_WRITE_CACHE_AFTER_UPLOAD true
  set_default BREWFS_PERSIST_WRITE_CACHE_AFTER_UPLOAD false
  set_default BREWFS_COMPRESSION lz4
  set_default BREWFS_VERIFY_CACHE_CHECKSUM full

  case "$BREWFS_TUNING_PROFILE" in
    compat)
      set_default BREWFS_S3_MAX_CONCURRENCY 32
      set_default BREWFS_FUSE_WORKERS 1
      set_default BREWFS_READ_MEMORY_BYTES 4294967296
      set_default BREWFS_READ_SSD_BYTES 21474836480
      set_default BREWFS_WRITE_MEMORY_BYTES 402653184
      set_default BREWFS_WRITE_SSD_BYTES 21474836480
      set_default BREWFS_DIRTY_SLICE_TARGET_SIZE 33554432
      set_default BREWFS_DIRTY_SLICE_MAX_AGE_MS 2000
      set_default BREWFS_UPLOAD_CONCURRENCY 10
      set_default BREWFS_PREFETCH_MAX_BYTES 67108864
      set_default BREWFS_PREFETCH_CONCURRENCY 64
      set_default BREWFS_MEMORY_BUDGET_BYTES 1342177280
      set_default BREWFS_WRITEBACK_REQUIRE_STAGE_BEFORE_COMMIT true
      set_default BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES 0
      set_default BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES 0
      ;;
    balanced)
      workers="$(bounded_workers "$cpus" 2 4)"
      set_default BREWFS_S3_MAX_CONCURRENCY 16
      set_default BREWFS_FUSE_WORKERS "$workers"
      set_default BREWFS_READ_MEMORY_BYTES 1073741824
      set_default BREWFS_READ_SSD_BYTES 21474836480
      set_default BREWFS_WRITE_MEMORY_BYTES 1073741824
      set_default BREWFS_WRITE_SSD_BYTES 21474836480
      set_default BREWFS_DIRTY_SLICE_TARGET_SIZE 67108864
      set_default BREWFS_DIRTY_SLICE_MAX_AGE_MS 1500
      set_default BREWFS_UPLOAD_CONCURRENCY 16
      set_default BREWFS_PREFETCH_MAX_BYTES 67108864
      set_default BREWFS_PREFETCH_CONCURRENCY 32
      set_default BREWFS_MEMORY_BUDGET_BYTES 2147483648
      set_default BREWFS_WRITEBACK_REQUIRE_STAGE_BEFORE_COMMIT true
      set_default BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES 536870912
      set_default BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES 1073741824
      ;;
    throughput)
      workers="$(bounded_workers "$cpus" 2 6)"
      set_default BREWFS_S3_MAX_CONCURRENCY 16
      set_default BREWFS_FUSE_WORKERS "$workers"
      set_default BREWFS_READ_MEMORY_BYTES 2147483648
      set_default BREWFS_READ_SSD_BYTES 21474836480
      set_default BREWFS_WRITE_MEMORY_BYTES 2147483648
      set_default BREWFS_WRITE_SSD_BYTES 21474836480
      set_default BREWFS_DIRTY_SLICE_TARGET_SIZE 67108864
      set_default BREWFS_DIRTY_SLICE_MAX_AGE_MS 2000
      set_default BREWFS_UPLOAD_CONCURRENCY 32
      set_default BREWFS_PREFETCH_MAX_BYTES 134217728
      set_default BREWFS_PREFETCH_CONCURRENCY 64
      set_default BREWFS_MEMORY_BUDGET_BYTES 4294967296
      set_default BREWFS_WRITEBACK_REQUIRE_STAGE_BEFORE_COMMIT false
      set_default BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES 1073741824
      set_default BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES 2147483648
      ;;
    *)
      err "Unsupported BREWFS_TUNING_PROFILE=$BREWFS_TUNING_PROFILE (expected compat, balanced, or throughput)."
      ;;
  esac
}

normalize_version() {
  local version="$1"
  case "$version" in
    v*|"") printf '%s' "$version" ;;
    *) printf 'v%s' "$version" ;;
  esac
}

normalize_bool_var() {
  local name="$1"
  local value="${!name}"
  case "${value,,}" in
    1|true|yes|on) printf -v "$name" '%s' "true" ;;
    0|false|no|off) printf -v "$name" '%s' "false" ;;
    *) err "$name must be a boolean value: true/false, 1/0, yes/no, or on/off." ;;
  esac
}

normalize_writeback_mode() {
  local normalized
  normalized="${BREWFS_WRITEBACK_MODE,,}"
  normalized="${normalized//-/_}"
  case "$normalized" in
    upload_before_commit|upload_first|safe|default)
      BREWFS_WRITEBACK_MODE="upload_before_commit"
      ;;
    commit_before_upload|commit_first|writeback|s3_writeback)
      BREWFS_WRITEBACK_MODE="commit_before_upload"
      ;;
    *)
      err "BREWFS_WRITEBACK_MODE must be upload_before_commit or commit_before_upload."
      ;;
  esac
}

require_uint_var() {
  local name="$1"
  local min="$2"
  local value="${!name}"
  if ! [[ "$value" =~ ^[0-9]+$ ]] || (( value < min )); then
    err "$name must be an integer >= $min."
  fi
}

validate_runtime_config() {
  normalize_bool_var BREWFS_S3_FORCE_PATH_STYLE
  normalize_bool_var BREWFS_S3_DISABLE_PAYLOAD_CHECKSUM
  normalize_bool_var BREWFS_WRITEBACK_PERSIST_SYNC
  normalize_bool_var BREWFS_PREFETCH_ENABLED
  normalize_bool_var BREWFS_FUSE_PRIVILEGED
  normalize_writeback_mode

  require_uint_var BREWFS_S3_PART_SIZE 1
  require_uint_var BREWFS_S3_MAX_CONCURRENCY 1
  require_uint_var BREWFS_META_OPEN_FILE_CACHE_TTL_MS 0
  require_uint_var BREWFS_META_OPEN_FILE_CACHE_CAPACITY 1
  require_uint_var BREWFS_CACHE_TTL_MS 0
  require_uint_var BREWFS_FUSE_WORKERS 0
  require_uint_var BREWFS_FUSE_MAX_BACKGROUND 1
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
  download_stdout "$api_url" 2>/dev/null \
    | grep '"tag_name":' | head -n1 \
    | sed 's/.*"tag_name": "\([^"]*\)".*/\1/'
}

fetch_brewfs_tag_versions() {
  local api_url="https://api.github.com/repos/${BREWFS_RELEASE_REPO}/tags"
  download_stdout "$api_url" 2>/dev/null \
    | grep '"name":' \
    | sed 's/.*"name": "\([^"]*\)".*/\1/' \
    | grep '^v' || true
}

brewfs_artifact_url_for_version() {
  local version="$1"
  printf '%s/%s/brewfs-%s-%s' "$BREWFS_BASE_URL" "$version" "$OS" "$ARCH"
}

resolve_brewfs_release_version() {
  if [[ -z "$BREWFS_VERSION" ]]; then
    local candidates
    candidates="$(
      fetch_latest_brewfs_version || true
      fetch_brewfs_tag_versions || true
      if [[ "$BREWFS_ALLOW_FALLBACK" == "1" ]]; then
        printf '%s\n' "$DEFAULT_BREWFS_VERSION"
      fi
    )"

    local candidate normalized url seen=" "
    while IFS= read -r candidate; do
      [[ -n "$candidate" ]] || continue
      normalized="$(normalize_version "$candidate")"
      case "$seen" in
        *" $normalized "*) continue ;;
      esac
      seen="${seen}${normalized} "
      url="$(brewfs_artifact_url_for_version "$normalized")"
      if url_exists "$url"; then
        BREWFS_VERSION="$normalized"
        info "Selected BrewFS release $BREWFS_VERSION from $url"
        break
      fi
      warn "Skipping BrewFS release $normalized because $url is not downloadable."
    done <<<"$candidates"

    if [[ -z "$BREWFS_VERSION" ]]; then
      err "Could not determine a downloadable BrewFS release. Pass --version or set BREWFS_DOWNLOAD_URL."
    fi
  fi

  BREWFS_VERSION="$(normalize_version "$BREWFS_VERSION")"
}

fetch_latest_rustfs_version() {
  local api_url="https://api.github.com/repos/${RUSTFS_RELEASE_REPO}/releases/latest"
  download_stdout "$api_url" 2>/dev/null \
    | grep '"tag_name":' | head -n1 \
    | sed 's/.*"tag_name": "\([^"]*\)".*/\1/'
}

fetch_rustfs_release_versions() {
  local api_url="https://api.github.com/repos/${RUSTFS_RELEASE_REPO}/releases"
  download_stdout "$api_url" 2>/dev/null \
    | grep '"tag_name":' \
    | sed 's/.*"tag_name": "\([^"]*\)".*/\1/' || true
}

rustfs_arch() {
  case "$ARCH_RAW" in
    x86_64|amd64) printf 'x86_64' ;;
    aarch64|arm64) printf 'aarch64' ;;
    *) err "Unsupported RustFS architecture: $ARCH_RAW" ;;
  esac
}

rustfs_artifact_url_for_version() {
  local version="$1"
  local asset_version="${version#v}"
  printf 'https://github.com/%s/releases/download/%s/rustfs-linux-%s-gnu-v%s.zip' \
    "$RUSTFS_RELEASE_REPO" "$version" "$(rustfs_arch)" "$asset_version"
}

rustfs_flavor() {
  case "$RUSTFS_FLAVOR" in
    musl|gnu) printf '%s' "$RUSTFS_FLAVOR" ;;
    *) err "Unsupported RUSTFS_FLAVOR=$RUSTFS_FLAVOR (expected musl or gnu)." ;;
  esac
}

rustfs_latest_artifact_url() {
  printf '%s/rustfs-linux-%s-%s-latest.zip' \
    "$RUSTFS_DIST_BASE_URL" "$(rustfs_arch)" "$(rustfs_flavor)"
}

resolve_rustfs_download_url() {
  [[ -z "$RUSTFS_DOWNLOAD_URL" ]] || return 0

  if [[ -n "$RUSTFS_VERSION" ]]; then
    RUSTFS_VERSION="${RUSTFS_VERSION#v}"
    RUSTFS_DOWNLOAD_URL="$(rustfs_artifact_url_for_version "$RUSTFS_VERSION")"
    return 0
  fi

  RUSTFS_DOWNLOAD_URL="$(rustfs_latest_artifact_url)"
  if url_exists "$RUSTFS_DOWNLOAD_URL"; then
    info "Selected RustFS official binary from $RUSTFS_DOWNLOAD_URL"
    return 0
  fi

  warn "RustFS official binary is not downloadable at $RUSTFS_DOWNLOAD_URL; falling back to GitHub release discovery."

  if [[ -z "$RUSTFS_VERSION" ]]; then
    local candidates
    candidates="$(
      fetch_latest_rustfs_version || true
      fetch_rustfs_release_versions || true
      printf '%s\n' "$DEFAULT_RUSTFS_VERSION"
    )"

    local candidate normalized url seen=" "
    while IFS= read -r candidate; do
      [[ -n "$candidate" ]] || continue
      normalized="${candidate#v}"
      case "$seen" in
        *" $normalized "*) continue ;;
      esac
      seen="${seen}${normalized} "
      url="$(rustfs_artifact_url_for_version "$normalized")"
      if url_exists "$url"; then
        RUSTFS_VERSION="$normalized"
        RUSTFS_DOWNLOAD_URL="$url"
        info "Selected RustFS release $RUSTFS_VERSION from $RUSTFS_DOWNLOAD_URL"
        return 0
      fi
      warn "Skipping RustFS release $normalized because $url is not downloadable."
    done <<<"$candidates"

    err "Could not determine a downloadable RustFS release. Set RUSTFS_DOWNLOAD_URL."
  fi
}

preflight() {
  need_root
  apply_tuning_profile
  validate_runtime_config

  local required=(systemctl mktemp find grep chmod mkdir install sed awk)
  local missing=()
  local cmd
  for cmd in "${required[@]}"; do
    command -v "$cmd" >/dev/null 2>&1 || missing+=("$cmd")
  done
  [[ "${#missing[@]}" -gt 0 ]] && err "Missing required commands: ${missing[*]}"

  if ! find_command DOWNLOAD_CMD curl wget; then
    ensure_command curl curl ca-certificates || ensure_command wget wget ca-certificates || true
    find_command DOWNLOAD_CMD curl wget || true
  fi

  find_command DOWNLOAD_CMD curl wget || true
  find_command PORT_CMD ss lsof netstat || true

  [[ "$(uname -s)" == "Linux" ]] || err "This installer supports Linux only."
  detect_release_platform
  ensure_command python3 python3 || true
  if [[ ! -d /run/systemd/system ]]; then
    warn "systemd runtime directory was not found; service installation may fail."
  fi
}

check_mount_point_available() {
  if command -v mountpoint >/dev/null 2>&1 && mountpoint -q "$MOUNT_POINT"; then
    err "Mount point is already mounted: $MOUNT_POINT"
  fi

  if find "$MOUNT_POINT" -mindepth 1 -maxdepth 1 -print -quit | grep -q .; then
    err "Mount point is not empty: $MOUNT_POINT. Set MOUNT_POINT to an empty directory."
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
  local require_checksum="${3:-$BREWFS_REQUIRE_CHECKSUM}"
  local sum_url="${url}.sha256"
  local sum_file

  sum_file="$(mktemp)"
  if ! download_to_tmp "$sum_url" "$sum_file" 2>/dev/null; then
    rm -f "$sum_file"
    if [[ "$require_checksum" == "1" ]]; then
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
    if [[ "$require_checksum" == "1" ]]; then
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
  local require_checksum="${4:-0}"

  [[ -n "$url" ]] || return 1

  info "Downloading $name from $url"
  local tmp_dir
  tmp_dir="$(mktemp -d)"

  local payload="$tmp_dir/$name.download"
  download_to_tmp "$url" "$payload"
  [[ -s "$payload" ]] || err "$name download is empty: $url"
  verify_checksum "$payload" "$url" "$require_checksum"

  case "$url" in
    *.tar.gz|*.tgz)
      ensure_command tar tar || err "tar is required for $url"
      tar -xzf "$payload" -C "$tmp_dir"
      ;;
    *.zip)
      ensure_command unzip unzip || err "unzip is required for $url"
      unzip -q "$payload" -d "$tmp_dir"
      ;;
    *)
      chmod +x "$payload"
      install -m 0755 "$payload" "$destination"
      rm -rf "$tmp_dir"
      info "$name installed to $destination"
      return 0
      ;;
  esac

  local found
  found="$(find "$tmp_dir" -type f -name "$name" -perm /111 | head -n1 || true)"
  [[ -n "$found" ]] || found="$(find "$tmp_dir" -type f -name "$name" | head -n1 || true)"
  [[ -n "$found" ]] || err "$name binary not found in downloaded archive."
  install -m 0755 "$found" "$destination"
  rm -rf "$tmp_dir"
  info "$name installed to $destination"
}

stop_distribution_redis_service() {
  local unit
  for unit in redis-server.service redis.service; do
    if systemctl list-unit-files "$unit" --no-pager 2>/dev/null | grep -q "$unit"; then
      systemctl stop "$unit" 2>/dev/null || true
      systemctl disable "$unit" 2>/dev/null || true
    fi
  done
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

  if apt_install_once redis-server; then
    stop_distribution_redis_service
    if command -v redis-server >/dev/null 2>&1; then
      REDIS_BIN="$(command -v redis-server)"
      return 0
    fi
  fi

  if install_binary_from_url "redis-server" "$REDIS_DOWNLOAD_URL" "$INSTALL_DIR/redis-server" "$REDIS_REQUIRE_CHECKSUM"; then
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
      install_binary_from_url "brewfs" "$BREWFS_DOWNLOAD_URL" "$BREWFS_BIN" "$BREWFS_REQUIRE_CHECKSUM"
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

  resolve_rustfs_download_url
  if install_binary_from_url "rustfs" "$RUSTFS_DOWNLOAD_URL" "$RUSTFS_BIN" "$RUSTFS_REQUIRE_CHECKSUM"; then
    return 0
  fi

  err "RustFS binary not found at $RUSTFS_BIN and could not be installed."
}

create_directories() {
  install -d -m 0755 "$CONFIG_DIR" "$DEFAULT_DIR" "$SYSTEMD_DIR" "$INSTALL_DIR" "$LIBEXEC_DIR" "$LOG_DIR"
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
BREWFS_BUCKET="$BREWFS_BUCKET"
RUSTFS_ENDPOINT="http://$RUSTFS_HOST:$RUSTFS_PORT"
BREWFS_CACHE_TTL_MS="$BREWFS_CACHE_TTL_MS"
RUST_LOG="$BREWFS_LOG_LEVEL"
EOF
}

write_brewfs_config() {
  apply_tuning_profile

  cat >"$BREWFS_CONFIG_FILE" <<EOF
mount_point: $MOUNT_POINT
data:
  backend: s3
  s3:
    bucket: $BREWFS_BUCKET
    endpoint: http://$RUSTFS_HOST:$RUSTFS_PORT
    region: $BREWFS_REGION
    part_size: $BREWFS_S3_PART_SIZE
    max_concurrency: $BREWFS_S3_MAX_CONCURRENCY
    force_path_style: $BREWFS_S3_FORCE_PATH_STYLE
    disable_payload_checksum: $BREWFS_S3_DISABLE_PAYLOAD_CHECKSUM
meta:
  backend: redis
  redis:
    url: redis://$REDIS_HOST:$REDIS_PORT/0
  open_file_cache_ttl_ms: $BREWFS_META_OPEN_FILE_CACHE_TTL_MS
  open_file_cache_capacity: $BREWFS_META_OPEN_FILE_CACHE_CAPACITY
cache:
  root: $BREWFS_CACHE_DIR
  read_memory_bytes: $BREWFS_READ_MEMORY_BYTES
  read_ssd_bytes: $BREWFS_READ_SSD_BYTES
  write_memory_bytes: $BREWFS_WRITE_MEMORY_BYTES
  write_ssd_bytes: $BREWFS_WRITE_SSD_BYTES
  dirty_slice_target_size: $BREWFS_DIRTY_SLICE_TARGET_SIZE
  dirty_slice_max_age_ms: $BREWFS_DIRTY_SLICE_MAX_AGE_MS
  upload_concurrency: $BREWFS_UPLOAD_CONCURRENCY
  prefetch_enabled: $BREWFS_PREFETCH_ENABLED
  prefetch_max_bytes: $BREWFS_PREFETCH_MAX_BYTES
  prefetch_concurrency: $BREWFS_PREFETCH_CONCURRENCY
  range_background_prefetch: $BREWFS_RANGE_BACKGROUND_PREFETCH
  populate_write_cache_after_upload: $BREWFS_POPULATE_WRITE_CACHE_AFTER_UPLOAD
  persist_write_cache_after_upload: $BREWFS_PERSIST_WRITE_CACHE_AFTER_UPLOAD
  memory_budget_bytes: $BREWFS_MEMORY_BUDGET_BYTES
  compression: $BREWFS_COMPRESSION
  verify_cache_checksum: $BREWFS_VERIFY_CACHE_CHECKSUM
  writeback_mode: $BREWFS_WRITEBACK_MODE
  writeback_persist_sync: $BREWFS_WRITEBACK_PERSIST_SYNC
  writeback_require_stage_before_commit: $BREWFS_WRITEBACK_REQUIRE_STAGE_BEFORE_COMMIT
  writeback_recent_pending_soft_bytes: $BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES
  writeback_recent_pending_hard_bytes: $BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES
fuse:
  workers: $BREWFS_FUSE_WORKERS
  max_background: $BREWFS_FUSE_MAX_BACKGROUND
  privileged: $BREWFS_FUSE_PRIVILEGED
EOF
}

write_bucket_init_script() {
  cat >"$BUCKET_INIT_SCRIPT" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

bucket="${BREWFS_BUCKET:?BREWFS_BUCKET is required}"
endpoint="${RUSTFS_ENDPOINT:?RUSTFS_ENDPOINT is required}"
region="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
access_key="${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID is required}"
secret_key="${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY is required}"

if command -v aws >/dev/null 2>&1; then
  deadline=$((SECONDS + 180))
  until aws --endpoint-url "$endpoint" s3api create-bucket --bucket "$bucket" >/dev/null 2>&1 \
    || aws --endpoint-url "$endpoint" s3api head-bucket --bucket "$bucket" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      echo "Timed out waiting for RustFS bucket $bucket at $endpoint" >&2
      exit 1
    fi
    sleep 2
  done
  exit 0
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "Neither aws nor python3 is available; cannot create/check RustFS bucket." >&2
  exit 1
fi

python3 - "$endpoint" "$bucket" "$region" "$access_key" "$secret_key" <<'PY'
import datetime
import hashlib
import hmac
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

endpoint, bucket, region, access_key, secret_key = sys.argv[1:6]
parsed = urllib.parse.urlsplit(endpoint)
if not parsed.scheme or not parsed.netloc:
    raise SystemExit(f"invalid RUSTFS_ENDPOINT: {endpoint}")

base_path = parsed.path.rstrip("/")
bucket_path = f"{base_path}/{bucket}" if base_path else f"/{bucket}"
canonical_uri = urllib.parse.quote(bucket_path, safe="/~")
url = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, bucket_path, "", ""))
payload_hash = hashlib.sha256(b"").hexdigest()


def signing_key(secret: str, date_stamp: str) -> bytes:
    def sign(key: bytes, message: str) -> bytes:
        return hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()

    key_date = sign(("AWS4" + secret).encode("utf-8"), date_stamp)
    key_region = sign(key_date, region)
    key_service = sign(key_region, "s3")
    return sign(key_service, "aws4_request")


def signed_headers(method):
    now = datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")
    host = parsed.netloc
    headers = {
        "host": host,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }
    signed_header_names = ";".join(sorted(headers))
    canonical_headers = "".join(f"{name}:{headers[name]}\n" for name in sorted(headers))
    canonical_request = "\n".join(
        [
            method,
            canonical_uri,
            "",
            canonical_headers,
            signed_header_names,
            payload_hash,
        ]
    )
    credential_scope = f"{date_stamp}/{region}/s3/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        ]
    )
    signature = hmac.new(
        signing_key(secret_key, date_stamp),
        string_to_sign.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    headers["Authorization"] = (
        "AWS4-HMAC-SHA256 "
        f"Credential={access_key}/{credential_scope}, "
        f"SignedHeaders={signed_header_names}, "
        f"Signature={signature}"
    )
    return headers


def request(method):
    data = b"" if method == "PUT" else None
    req = urllib.request.Request(url, data=data, headers=signed_headers(method), method=method)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()
    except urllib.error.URLError as exc:
        return None, str(exc).encode("utf-8", "replace")


last = (None, b"")
for _ in range(90):
    code, body = request("HEAD")
    if code in (200, 204):
        print(f"RustFS bucket {bucket} already exists.")
        raise SystemExit(0)

    code, body = request("PUT")
    if code in (200, 201, 204, 409):
        print(f"RustFS bucket {bucket} is ready.")
        raise SystemExit(0)

    last = (code, body)
    time.sleep(2)

code, body = last
detail = body.decode("utf-8", "replace")[:500] if body else ""
raise SystemExit(f"timed out creating RustFS bucket {bucket} at {endpoint}: status={code} {detail}")
PY
EOF
  chmod 0755 "$BUCKET_INIT_SCRIPT"
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
Type=simple
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
Documentation=https://github.com/brewfs/brewfs
After=network-online.target $REDIS_SERVICE $RUSTFS_SERVICE
Wants=network-online.target $REDIS_SERVICE $RUSTFS_SERVICE
Requires=$REDIS_SERVICE $RUSTFS_SERVICE

[Service]
Type=simple
EnvironmentFile=$BREWFS_ENV_FILE
ExecStartPre=$BUCKET_INIT_SCRIPT
ExecStart=$BREWFS_BIN mount --config $BREWFS_CONFIG_FILE
ExecStop=-/bin/sh -c 'if command -v fusermount3 >/dev/null 2>&1; then fusermount3 -u $MOUNT_POINT; else umount $MOUNT_POINT; fi'
WorkingDirectory=$STATE_DIR
User=$BREWFS_USER
Group=$BREWFS_GROUP
Restart=always
RestartSec=5s
LimitNOFILE=1048576
TasksMax=infinity
NoNewPrivileges=true
StandardOutput=append:$LOG_DIR/brewfs.log
StandardError=append:$LOG_DIR/brewfs-err.log

[Install]
WantedBy=multi-user.target
EOF
}

daemon_reload() {
  systemctl daemon-reload
}

stop_existing_brewfs_mount() {
  if systemctl is-active --quiet "$BREWFS_SERVICE" 2>/dev/null; then
    info "Stopping existing $BREWFS_SERVICE before rewriting its systemd unit."
    systemctl stop "$BREWFS_SERVICE"
  fi
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
  check_mount_point_available
  resolve_redis_binary
  install_rustfs_binary
  install_brewfs_binary
  check_ports "$REDIS_SERVICE" "$REDIS_PORT"
  check_ports "$RUSTFS_SERVICE" "$RUSTFS_PORT" "$RUSTFS_CONSOLE_PORT"
  stop_existing_brewfs_mount
  write_redis_config
  write_env_files
  write_brewfs_config
  write_bucket_init_script
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
  install_binary_from_url "redis-server" "$REDIS_DOWNLOAD_URL" "$INSTALL_DIR/redis-server" "$REDIS_REQUIRE_CHECKSUM" || true
  install_binary_from_url "rustfs" "$RUSTFS_DOWNLOAD_URL" "$RUSTFS_BIN" "$RUSTFS_REQUIRE_CHECKSUM" || true
  install_brewfs_binary 1
  stop_existing_brewfs_mount
  write_redis_config
  write_env_files
  write_brewfs_config
  write_bucket_init_script
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
        "$AWS_CONFIG_FILE" \
        "$BUCKET_INIT_SCRIPT"

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
