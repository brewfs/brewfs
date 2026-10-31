#!/usr/bin/env python3
"""Account for verified content consumed from packed-container HTTP ranges.

The caller verifies every file's contents and counts each consumed byte once
per mount/pass. HTTP rows come from the capture analyzer, not requested ranges.
The ratio uses GC05 response body bytes, including metadata and inline payload;
index responses are reported in total_received_bytes. Pure readdir consumes
zero file-content bytes; assess its latency and directory-entry bytes separately.
"""

import argparse
import json
from pathlib import Path


def summarize(rows, useful_content_bytes, operations, provenance):
    if useful_content_bytes < 0 or operations <= 0:
        raise ValueError("content bytes must be nonnegative and operations positive")
    lengths = []
    bundle_bytes = 0
    bundle_gets = 0
    for row in rows:
        length = str(row.get("response_length", ""))
        if not length.isdecimal() or not 200 <= row.get("status", 0) < 300:
            raise ValueError("a GET lacks a successful measured response length")
        if "wire_elapsed_ms" not in row:
            raise ValueError("a response body tail is outside the capture")
        length = int(length)
        lengths.append(length)
        if "/objects/1/" in row["key"]:
            bundle_bytes += length
            bundle_gets += 1
    build = provenance["build_provenance"]
    all_inline = build["frame_count"] == 0 and build["inline_dentries"] > 0
    return {
        "operations": operations,
        "gets": len(rows),
        "get_per_operation": len(rows) / operations,
        "total_received_bytes": sum(lengths),
        "received_bytes_per_operation": sum(lengths) / operations,
        "bundle_gets": bundle_gets,
        "bundle_received_bytes": bundle_bytes,
        "useful_content_bytes": useful_content_bytes,
        "all_content_inline": all_inline,
        "bundle_useful_content_ratio": (
            useful_content_bytes / bundle_bytes if all_inline and bundle_bytes else None
        ),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requests", required=True, type=Path)
    parser.add_argument("--provenance", required=True, type=Path)
    parser.add_argument("--useful-content-bytes", required=True, type=int)
    parser.add_argument("--operations", required=True, type=int)
    args = parser.parse_args()
    result = summarize(
        json.loads(args.requests.read_text()), args.useful_content_bytes,
        args.operations, json.loads(args.provenance.read_text()),
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
