#!/usr/bin/env python3
"""Compare actual framed-object captures and their preparation copy counters."""

import hashlib
import json
from pathlib import Path
import sys


def captures(directory):
    root = Path(directory)
    rows = json.loads((root / "measurements.json").read_text())
    result = {}
    for row in rows:
        name = row["case"]
        data = (root / f"{name}.object").read_bytes()
        assert name not in result, f"duplicate case: {name}"
        assert len(data) == row["object_bytes"], name
        assert hashlib.sha256(data).hexdigest() == row["sha256"], name
        assert data[:4].hex() == row["header_hex"], name
        result[name] = (row, data)
    assert result, "no captures"
    return result


def main():
    if len(sys.argv) != 3:
        raise SystemExit(f"usage: {sys.argv[0]} BEFORE_DIR AFTER_DIR")
    before = captures(sys.argv[1])
    after = captures(sys.argv[2])
    assert before.keys() == after.keys(), "capture cases differ"
    output = []
    for name, (old, old_bytes) in before.items():
        new, new_bytes = after[name]
        for field in ("payload_bytes", "zero_prefix_bytes", "fragments", "write_hot_bytes"):
            assert old[field] == new[field], f"{name}: {field} differs"
        # Compare every byte, including the header; hash equality alone is not
        # the acceptance check. Hashes make the recorded evidence identifiable.
        assert old_bytes == new_bytes, f"{name}: persisted object bytes differ"
        output.append({
            "case": name,
            "byte_identical": True,
            "compared_bytes": len(old_bytes),
            "sha256": new["sha256"],
            "payload_bytes_copied_before": old["payload_bytes_copied"],
            "payload_bytes_copied_after": new["payload_bytes_copied"],
            "header_bytes_written_before": old["header_bytes_written"],
            "header_bytes_written_after": new["header_bytes_written"],
        })
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
