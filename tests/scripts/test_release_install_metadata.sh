#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
release_workflow="$repo_root/.github/workflows/release.yml"
installer="$repo_root/scripts/install_brewfs_single_node.sh"

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
assert_contains "$release_workflow" "component: [brewfs]"
assert_contains "$release_workflow" "x86_64-unknown-linux-gnu"
assert_contains "$release_workflow" "aarch64-unknown-linux-gnu"
assert_contains "$release_workflow" "aarch64-apple-darwin"
assert_contains "$release_workflow" "ubuntu-24.04-arm"
assert_contains "$release_workflow" "brewfs/releases/\${{ github.ref_name }}/"

assert_file "$installer"
assert_contains "$installer" "BREWFS_VERSION"
assert_contains "$installer" "BREWFS_BASE_URL=\"\${BREWFS_BASE_URL:-https://download.brewfs.ai/brewfs/releases}\""
assert_contains "$installer" "BREWFS_REQUIRE_CHECKSUM"
assert_contains "$installer" "BREWFS_ALLOW_FALLBACK"
assert_contains "$installer" "brewfs-\${OS}-\${ARCH}"
assert_contains "$installer" "https://download.brewfs.ai/brewfs/releases/v0.1.1/brewfs-darwin-arm64"
assert_contains "$installer" "--version <VERSION>"
