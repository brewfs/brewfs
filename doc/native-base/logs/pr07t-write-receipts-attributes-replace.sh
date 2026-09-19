#!/usr/bin/env bash
# PR07T: a durable upload whose KV step fails, a metadata-only chmod, and a
# temp-file rename over an existing destination.
#
#   WRITE-006 an upload whose commit transaction fails is folded into a
#             protected orphan receipt: the objects stay out of the cleanable
#             set, a resolved receipt drops out of it, and a receipt is only
#             readable/validatable as its own bytes
#   WRITE-010 a chmod on a GiB file changes only the attribute row and uploads
#             only its control receipts container: no extent, binding,
#             placement or data object is created, a bare permission mask is
#             refused at admission, and a data mutation cannot silently
#             overwrite concurrent attributes
#   WRITE-012 a rename over an existing destination publishes the source's
#             complete extent set (holes included), removes every destination
#             extent, attests every carried block in its receipts, and is
#             refused if it would leave any destination byte on the old
#             content or report itself as a patch-shaped change
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

export PATH="/home/luxian/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/luxian/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

echo "=== PR07T: format check ==="
cargo fmt --all --check

echo "=== PR07T: a KV failure after a successful upload protects the receipt (WRITE-006) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::write::tests::orphan_protection::a_kv_failure_after_a_successful_upload_protects_the_receipt

echo "=== PR07T: chmod on a large file is metadata-only (WRITE-010) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::write::tests::memory_backend::chmod_on_a_large_file_changes_metadata_and_uploads_no_data

echo "=== PR07T: a rename-over publishes the complete file (WRITE-012) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::write::tests::memory_backend::rename_over_publishes_the_complete_file_and_never_a_patch

echo "=== PR07T: replacement coverage rule (WRITE-012) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::write::replace::tests

echo "=== PR07T receipts, attributes and replacement: ALL PASSED ==="
