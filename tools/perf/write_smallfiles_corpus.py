#!/usr/bin/env python3
"""Write the deterministic small-file corpus through a mounted filesystem.

The tree, names, sizes and bytes match packed_snapshot_fixture and the
expectations of smallfiles_scan.py, so a native (flat) volume can be read with
exactly the same trace as a packed lower.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from smallfiles_scan import expected_payload_chunk, expected_pattern, expected_size  # noqa: E402


def leaf_path(leaf_index: int, levels: int, fanout: int) -> pathlib.Path:
    parts = []
    value = leaf_index
    for _ in range(levels):
        parts.append(f"d{value % fanout:03}")
        value //= fanout
    return pathlib.Path(*reversed(parts))


def write_leaf(root: pathlib.Path, leaf: int, args: argparse.Namespace) -> int:
    directory = root / leaf_path(leaf, args.dir_levels, args.dirs_per_level)
    directory.mkdir(parents=True, exist_ok=True)
    written = 0
    for file_index in range(args.files_per_dir):
        number = leaf * args.files_per_dir + file_index
        size = expected_size(number, args.min_size, args.max_size)
        data = expected_payload_chunk(expected_pattern(number), 0, size)
        path = directory / f"f{file_index:06}"
        with open(path, "xb", buffering=0) as stream:
            stream.write(data)
            os.fsync(stream.fileno())
        written += size
    return written


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=pathlib.Path, required=True)
    parser.add_argument("--dir-levels", type=int, default=2)
    parser.add_argument("--dirs-per-level", type=int, default=10)
    parser.add_argument("--files-per-dir", type=int, required=True)
    parser.add_argument("--min-size", type=int, required=True)
    parser.add_argument("--max-size", type=int, required=True)
    parser.add_argument("--workers", type=int, default=16)
    args = parser.parse_args()
    leaves = args.dirs_per_level ** args.dir_levels
    with concurrent.futures.ThreadPoolExecutor(args.workers) as pool:
        total = sum(pool.map(lambda leaf: write_leaf(args.root, leaf, args), range(leaves)))
    print(f"wrote files={leaves * args.files_per_dir} bytes={total}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
