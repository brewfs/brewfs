#!/usr/bin/env bash

set -euo pipefail

write_data_section() {
  local backend="${BREWFS_DATA_BACKEND:-local-fs}"

  case "$backend" in
    local-fs)
      cat <<EOF
data:
  backend: local-fs
  localfs:
    data_dir: ${BREWFS_DATA_DIR:-${BREWFS_HOME:-/var/lib/brewfs}/data}
EOF
      ;;
    s3)
      : "${BREWFS_S3_BUCKET:?BREWFS_S3_BUCKET is required when BREWFS_DATA_BACKEND=s3}"
      cat <<EOF
data:
  backend: s3
  s3:
    bucket: ${BREWFS_S3_BUCKET}
    region: ${BREWFS_S3_REGION:-us-east-1}
    part_size: ${BREWFS_S3_PART_SIZE:-16777216}
    max_concurrency: ${BREWFS_S3_MAX_CONCURRENCY:-8}
    force_path_style: ${BREWFS_S3_FORCE_PATH_STYLE:-false}
EOF
      if [[ -n "${BREWFS_S3_ENDPOINT:-}" ]]; then
        echo "    endpoint: ${BREWFS_S3_ENDPOINT}"
      fi
      ;;
    *)
      echo "unsupported BREWFS_DATA_BACKEND: $backend" >&2
      exit 1
      ;;
  esac
}

write_meta_section() {
  local home_dir="${BREWFS_HOME:-/var/lib/brewfs}"
  local backend="${BREWFS_META_BACKEND:-sqlite}"
  local sqlite_path="${BREWFS_SQLITE_PATH:-${home_dir}/metadata.db}"
  local meta_url="${BREWFS_META_URL:-}"

  case "$backend" in
    sqlite)
      mkdir -p "$(dirname "$sqlite_path")"
      if [[ -z "$meta_url" ]]; then
        meta_url="sqlite://${sqlite_path}?mode=rwc"
      fi
      cat <<EOF
meta:
  backend: sqlx
  sqlx:
    url: "$meta_url"
EOF
      ;;
    sqlx|postgres)
      : "${meta_url:?BREWFS_META_URL is required when BREWFS_META_BACKEND=${backend}}"
      cat <<EOF
meta:
  backend: sqlx
  sqlx:
    url: "$meta_url"
EOF
      ;;
    redis)
      : "${meta_url:?BREWFS_META_URL is required when BREWFS_META_BACKEND=redis}"
      cat <<EOF
meta:
  backend: redis
  redis:
    url: "$meta_url"
EOF
      ;;
    etcd)
      local etcd_urls="${BREWFS_META_ETCD_URLS:-http://etcd:2379}"
      cat <<EOF
meta:
  backend: etcd
  etcd:
    urls:
EOF
      local old_ifs="$IFS"
      IFS=','
      for url in $etcd_urls; do
        echo "      - \"${url}\""
      done
      IFS="$old_ifs"
      ;;
    *)
      echo "unsupported BREWFS_META_BACKEND: $backend" >&2
      exit 1
      ;;
  esac
  if [[ -n "${BREWFS_METADATA_OPEN_CACHE_TTL_MS:-}" ]]; then
    echo "  open_file_cache_ttl_ms: ${BREWFS_METADATA_OPEN_CACHE_TTL_MS}"
  fi
  if [[ -n "${BREWFS_METADATA_OPEN_CACHE_CAPACITY:-}" ]]; then
    echo "  open_file_cache_capacity: ${BREWFS_METADATA_OPEN_CACHE_CAPACITY}"
  fi
}

write_config() {
  local mount_point="${BREWFS_MOUNT_POINT:-/mnt/brewfs}"
  local config_path="${BREWFS_CONFIG_PATH:-/run/brewfs/config.yaml}"

  mkdir -p "$(dirname "$config_path")" "$mount_point"
  if [[ "${BREWFS_DATA_BACKEND:-local-fs}" == "local-fs" ]]; then
    mkdir -p "${BREWFS_DATA_DIR:-${BREWFS_HOME:-/var/lib/brewfs}/data}"
  fi

  {
    echo "mount_point: $mount_point"
    echo
    write_data_section
    echo
    write_meta_section
    echo
    cat <<EOF
layout:
  chunk_size: ${BREWFS_CHUNK_SIZE:-67108864}
  block_size: ${BREWFS_BLOCK_SIZE:-4194304}
EOF
  } >"$config_path"
}

prepare_fuse() {
  mkdir -p /etc
  if ! grep -q '^user_allow_other$' /etc/fuse.conf 2>/dev/null; then
    echo 'user_allow_other' >> /etc/fuse.conf
  fi
}

main() {
    if [[ $# -gt 0 ]]; then
        exec "$@"
    fi

  prepare_fuse
  write_config

  exec /usr/local/bin/brewfs mount --privileged \
    --config "${BREWFS_CONFIG_PATH:-/run/brewfs/config.yaml}" \
    "${BREWFS_MOUNT_POINT:-/mnt/brewfs}"
}

main "$@"
