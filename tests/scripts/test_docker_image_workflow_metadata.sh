#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
workflow="$repo_root/.github/workflows/docker-images.yml"
runtime_dockerfile="$repo_root/docker/Dockerfile.runtime"
operator_dockerfile="$repo_root/operator/brewfs-operator/Dockerfile"
operator_dockerignore="$repo_root/operator/brewfs-operator/.dockerignore"

assert_file() {
  local path="$1"
  [[ -f "$path" ]] || {
    echo "Expected file to exist: $path" >&2
    exit 1
  }
}

assert_contains() {
  local path="$1"
  local needle="$2"
  grep -Fq -- "$needle" "$path" || {
    echo "Expected '$path' to contain: $needle" >&2
    exit 1
  }
}

assert_not_contains() {
  local path="$1"
  local needle="$2"
  if grep -Fq -- "$needle" "$path"; then
    echo "Expected '$path' not to contain: $needle" >&2
    exit 1
  fi
}

assert_file "$workflow"
assert_file "$runtime_dockerfile"
assert_file "$operator_dockerfile"
assert_file "$operator_dockerignore"

assert_contains "$workflow" "actions-rust-lang/setup-rust-toolchain@v1"
assert_contains "$workflow" "Build BrewFS runtime binary"
assert_contains "$workflow" "docker/build_brewfs_host_binary.sh"
assert_contains "$workflow" "Build BrewFS operator binary"
assert_contains "$workflow" "cargo build --locked --release --manifest-path operator/brewfs-operator/Cargo.toml"

assert_contains "$runtime_dockerfile" "COPY target/release/brewfs /usr/local/bin/brewfs"
assert_not_contains "$runtime_dockerfile" "FROM rust:"
assert_not_contains "$runtime_dockerfile" "cargo build"
assert_not_contains "$runtime_dockerfile" "COPY --from=builder"

assert_contains "$operator_dockerfile" "COPY target/release/brewfs-operator /usr/local/bin/brewfs-operator"
assert_not_contains "$operator_dockerfile" "FROM rust:"
assert_not_contains "$operator_dockerfile" "cargo build"
assert_not_contains "$operator_dockerfile" "COPY --from=builder"

assert_contains "$operator_dockerignore" "!target/"
assert_contains "$operator_dockerignore" "!target/release/"
assert_contains "$operator_dockerignore" "!target/release/brewfs-operator"
