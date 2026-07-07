#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
release_workflow="$repo_root/.github/workflows/release.yml"
installer="$repo_root/scripts/install_brewfs_single_node.sh"
readme="$repo_root/README.md"
readme_cn="$repo_root/README_CN.md"
config_doc="$repo_root/doc/operations/configuration.md"

assert_file() {
  local path="$1"
  [[ -f "$path" ]] || {
    echo "missing file: $path" >&2
    exit 1
  }
}

assert_contains() {
  local path="$1"
  local needle="$2"
  grep -Fq -- "$needle" "$path" || {
    echo "expected '$path' to contain: $needle" >&2
    exit 1
  }
}

assert_file "$release_workflow"
assert_contains "$release_workflow" "tags:"
assert_contains "$release_workflow" "'v*'"
assert_contains "$release_workflow" "fail-fast: false"
assert_contains "$release_workflow" "component: [brewfs]"
assert_contains "$release_workflow" "x86_64-unknown-linux-gnu"
assert_contains "$release_workflow" "aarch64-unknown-linux-gnu"
assert_contains "$release_workflow" "aarch64-apple-darwin"
assert_contains "$release_workflow" "--features fuse-tokio-runtime"
assert_contains "$release_workflow" "brew install protobuf"
assert_contains "$release_workflow" "ubuntu-24.04-arm"
assert_contains "$release_workflow" "BREWFS_RELEASE_R2_PREFIX: brewfs/releases"
assert_contains "$release_workflow" "r2:\${release_prefix}/\${{ github.ref_name }}/"

assert_file "$installer"
assert_contains "$installer" "BREWFS_VERSION"
assert_contains "$installer" "BREWFS_BASE_URL=\"\${BREWFS_BASE_URL:-https://download.brewfs.ai/brewfs/releases}\""
assert_contains "$installer" "BREWFS_REQUIRE_CHECKSUM"
assert_contains "$installer" "BREWFS_ALLOW_FALLBACK"
assert_contains "$installer" "BREWFS_INSTALL_PACKAGES"
assert_contains "$installer" "BREWFS_TUNING_PROFILE"
assert_contains "$installer" "apply_tuning_profile"
assert_contains "$installer" "throughput)"
assert_contains "$installer" "RUSTFS_RELEASE_REPO=\"\${RUSTFS_RELEASE_REPO:-rustfs/rustfs}\""
assert_contains "$installer" "RUSTFS_INSTALLER_URL=\"\${RUSTFS_INSTALLER_URL:-https://rustfs.com/install_rustfs.sh}\""
assert_contains "$installer" "RUSTFS_DIST_BASE_URL=\"\${RUSTFS_DIST_BASE_URL:-https://dl.rustfs.com/artifacts/rustfs/release}\""
assert_contains "$installer" "rustfs_latest_artifact_url"
assert_contains "$installer" "RUSTFS_REQUIRE_CHECKSUM"
assert_contains "$installer" "REDIS_REQUIRE_CHECKSUM"
assert_contains "$installer" "resolve_rustfs_download_url"
assert_contains "$installer" "write_bucket_init_script"
assert_contains "$installer" "check_mount_point_available"
assert_contains "$installer" "ExecStartPre=\$BUCKET_INIT_SCRIPT"
assert_contains "$installer" "brewfs-\${OS}-\${ARCH}"
assert_contains "$installer" "max_concurrency: \$BREWFS_S3_MAX_CONCURRENCY"
assert_contains "$installer" "upload_concurrency: \$BREWFS_UPLOAD_CONCURRENCY"
assert_contains "$installer" "memory_budget_bytes: \$BREWFS_MEMORY_BUDGET_BYTES"
assert_contains "$installer" "workers: \$BREWFS_FUSE_WORKERS"
assert_contains "$installer" "https://download.brewfs.ai/brewfs/releases/v0.0.1/brewfs-linux-amd64"
assert_contains "$installer" "--version <VERSION>"

assert_file "$readme"
assert_contains "$readme" "https://raw.githubusercontent.com/brewfs/brewfs/main/scripts/install_brewfs_single_node.sh"
assert_contains "$readme" "BREWFS_TUNING_PROFILE=balanced"
assert_contains "$readme" "brewfs.service"

assert_file "$readme_cn"
assert_contains "$readme_cn" "https://raw.githubusercontent.com/brewfs/brewfs/main/scripts/install_brewfs_single_node.sh"
assert_contains "$readme_cn" "BREWFS_TUNING_PROFILE=balanced"
assert_contains "$readme_cn" "brewfs.service"

assert_file "$config_doc"
assert_contains "$config_doc" "## Single-node installer"
assert_contains "$config_doc" "BREWFS_TUNING_PROFILE"
assert_contains "$config_doc" "scripts/install_brewfs_single_node.sh | sudo bash -s -- install"
