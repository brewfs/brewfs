#!/usr/bin/env bash
# PR07O explicit variant accounting and close-evidence certificate evidence.
#
#   CLN-022  an explicit repack variant succeeds only as an addition: the new
#            space is booked separately from the untouched source footprint and
#            every old official pack stays in the permanent set
#   CLN-023  a repack that was not published leaves only its own unretained
#            build outputs delete-eligible; retained source objects never are
#   CLN-025  after I(d) is frozen the close builds an independent
#            control-evidence inventory and certificate: C(d) is quota-accounted,
#            every evidence object is a container (data cannot masquerade as
#            evidence), there is no self-hash and no recursive inventory, and a
#            rejected C(d) leaves the domain DRAINING with no certificate
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

export PATH="/home/luxian/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/luxian/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

echo "=== PR07O: format check ==="
cargo fmt --all --check

echo "=== PR07O: explicit variant accounting and abandoned outputs (CLN-022/CLN-023) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::lifecycle::variant

echo "=== PR07O: close evidence inventory and certificate (CLN-025) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::lifecycle::cleanup::tests::close_evidence

echo "=== PR07O variant accounting and close evidence: ALL PASSED ==="
