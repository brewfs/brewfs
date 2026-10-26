#!/usr/bin/env python3
"""Summarize 3 read-policy trials and audit actual RustFS xl.meta EC geometry.

Usage: python summarize_ec_sparse_read.py results.json copied-data-directory
Requires msgpack. Copy the fixture's /data before removing its compose volume.
"""
import hashlib
import json
from pathlib import Path
import statistics
import sys

import msgpack


def decode_layout(path):
    data = path.read_bytes()
    # XL2 major 1, minor 3: an outer msgpack byte string contains versioned
    # metadata (header version, metadata version, count, header/body pairs).
    if data[:8] != b"XL2 \x01\x00\x03\x00":
        raise ValueError(f"unsupported xl.meta framing: {path}")
    outer = msgpack.Unpacker(raw=False, strict_map_key=False)
    outer.feed(data[8:])
    inner = msgpack.Unpacker(raw=False, strict_map_key=False)
    inner.feed(outer.unpack())
    header_version, metadata_version, count = (inner.unpack() for _ in range(3))
    if (header_version, metadata_version, count) != (3, 3, 1):
        raise ValueError(f"unexpected versioned metadata: {path}")
    inner.unpack()  # Version header, not the object layout.
    version = msgpack.unpackb(inner.unpack(), raw=False, strict_map_key=False)
    obj = version["V2Obj"]
    return {
        "path": str(path),
        "sha256": hashlib.sha256(data).hexdigest(),
        "EcBSize": obj["EcBSize"],
        "EcM": obj["EcM"],
        "EcN": obj["EcN"],
        "Size": obj["Size"],
    }


def summarize(measurements, data_root):
    rows = json.loads(measurements.read_text())
    groups = {}
    for row in rows:
        groups.setdefault((row["case"], row["policy"]), []).append(row)
    metrics = [
        "http_body_bytes_per_miss", "http_gets_per_miss", "miss_cache_hits",
        "repeat_cache_hits", "repeat_gets", "repeat_http_body_bytes", "logical_mib_s", "p95_ms", "p99_ms",
    ]
    medians = []
    for (case, policy), trials in sorted(groups.items()):
        assert sorted(row["trial"] for row in trials) == [1, 2, 3]
        assert all(row["samples"] == 64 for row in trials)
        medians.append({
            "case": case, "policy": policy, "trials": 3, "samples_per_trial": 64,
            **{metric: statistics.median(row[metric] for row in trials) for metric in metrics},
        })
    layouts = []
    for disk in range(1, 5):
        for obj in range(8101, 8105):
            path = data_root / f"disk{disk}/ec-sparse-evidence/chunks-v2/{obj}/0/xl.meta"
            decoded = decode_layout(path)
            decoded["path"] = str(path.relative_to(data_root))
            assert (decoded["EcM"], decoded["EcN"]) == (2, 2)
            decoded["read_arms"] = ["BackendDefault", "Sparse"]
            layouts.append(decoded)
    return {"medians": medians, "persisted_layouts": layouts}


if __name__ == "__main__":
    print(json.dumps(summarize(Path(sys.argv[1]), Path(sys.argv[2])), indent=2))
