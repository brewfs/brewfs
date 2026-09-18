#!/usr/bin/env bash
# PR07Q frozen index seams: attribute relocation, ordinal prune, cookie replay.
#
#   IDX-003  moving canonical inode attributes from the namespace row into an
#            external ValueRef is a byte-exact move: the resolved canonical
#            bytes and therefore the revision digest are unchanged; a
#            shortened, non-canonical or foreign payload is refused instead of
#            being repaired or resolved from somewhere else
#   IDX-004  a repack leaves a sparse ordinal map and historical Objects rows;
#            pruning deletes exactly this pack's dead keys, keeps pinned rows
#            even when they are not live, never claims or deletes another
#            pack's rows, and refuses to retain an ordinal the old map does not
#            already map inside the pack
#   IDX-005  an issued readdir cookie survives RAM eviction of the entry spool
#            because it anchors on the last returned entry; a full cookie table
#            fails before a cookie is issued, and a stale or replaced anchor
#            fails instead of resuming at a guessed position
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

export PATH="/home/luxian/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/luxian/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

echo "=== PR07Q: format check ==="
cargo fmt --all --check

echo "=== PR07Q: inline -> external attribute relocation (IDX-003) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::frozen::tests::attribute_relocation

echo "=== PR07Q: repack ordinal prune (IDX-004) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::frozen::tests::inventory_prune

echo "=== PR07Q: readdir cookie eviction and replay (IDX-005) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::frozen::tests::issued_directory_cookies

echo "=== PR07Q index relocation, prune and cookie replay: ALL PASSED ==="
