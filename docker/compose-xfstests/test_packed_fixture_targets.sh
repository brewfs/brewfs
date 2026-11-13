#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(realpath "$SCRIPT_DIR/../..")"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

cd "$REPO_DIR"
cargo metadata --no-deps --format-version 1 >"$tmpdir/metadata.json"
python3 - "$tmpdir/metadata.json" <<'PY'
import json
import pathlib
import sys

metadata = json.loads(pathlib.Path(sys.argv[1]).read_text())
package = next(pkg for pkg in metadata["packages"] if pkg["name"] == "brewfs")
targets = {target["name"]: target for target in package["targets"] if "bin" in target["kind"]}
assert set(targets) == {"brewfs", "packed_snapshot_fixture"}, sorted(targets)
assert "workspace" in targets["packed_snapshot_fixture"]["required-features"]
assert pathlib.Path(targets["brewfs"]["src_path"]).name == "main.rs"
print("Only BrewFS and the v3 packed fixture are exposed as package binaries")
PY
