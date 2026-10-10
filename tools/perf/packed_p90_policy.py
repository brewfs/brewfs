#!/usr/bin/env python3
"""Build and validate a frozen packed-v3 p90 frame-selection policy."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
from collections import Counter
from typing import Any

SCHEMA = "packed-v3-p90-policy-v1"
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_MAX_REQUEST_BYTES = 1 << 63


class P90PolicyError(ValueError):
    """Raised when a training trace or frozen policy is not trustworthy."""


def _canonical(value: dict[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def _digest(value: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _trace_content_digest(trace: dict[str, Any]) -> str:
    payload = {key: value for key, value in trace.items() if key != "trace_sha256"}
    return _digest(payload)


def _read(path: pathlib.Path) -> Any:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise P90PolicyError(f"invalid JSON artifact {path.name}: {error}") from error


def _trace_fields(trace: Any) -> tuple[str, str, str, list[int]]:
    if not isinstance(trace, dict):
        raise P90PolicyError("training trace must be a JSON object")
    trace_sha256 = trace.get("trace_sha256")
    if not isinstance(trace_sha256, str) or not _HEX64.fullmatch(trace_sha256.lower()):
        raise P90PolicyError("training trace requires a SHA-256 identity")
    source = trace.get("source")
    if not isinstance(source, str) or not source.strip() or any(c.isspace() for c in source):
        raise P90PolicyError("training trace source is missing or malformed")
    captured_at = trace.get("captured_at_utc")
    if not isinstance(captured_at, str) or not captured_at.strip() or any(c.isspace() for c in captured_at):
        raise P90PolicyError("training trace collection time is missing or malformed")
    samples = trace.get("requested_ranges_bytes")
    if not isinstance(samples, list) or not samples:
        raise P90PolicyError("training trace has no requested ranges")
    if len(samples) > 10_000_000:
        raise P90PolicyError("training trace exceeds the bounded sample limit")
    normalized: list[int] = []
    for value in samples:
        if isinstance(value, bool) or not isinstance(value, int) or not 0 < value <= _MAX_REQUEST_BYTES:
            raise P90PolicyError("training trace contains an invalid requested range")
        normalized.append(value)
    return trace_sha256.lower(), source.strip(), captured_at.strip(), normalized


def _payload_from_trace(trace: Any) -> dict[str, Any]:
    trace_sha256, source, captured_at, samples = _trace_fields(trace)
    histogram = [
        {"range_bytes": value, "count": count}
        for value, count in sorted(Counter(samples).items())
    ]
    rank = (90 * len(samples) + 99) // 100
    p90_bytes = sorted(samples)[rank - 1]
    return {
        "schema": SCHEMA,
        "trace_sha256": trace_sha256,
        "source": source,
        "captured_at_utc": captured_at,
        "sample_count": len(samples),
        "histogram": histogram,
        "p90_bytes": p90_bytes,
    }


def build_policy(trace: dict[str, Any]) -> dict[str, Any]:
    """Return a signed-by-digest policy derived solely from a training trace."""
    payload = _payload_from_trace(trace)
    return {**payload, "policy_sha256": _digest(payload)}


def validate_policy(policy: Any, *, expected_trace_sha256: str | None = None) -> dict[str, Any]:
    if not isinstance(policy, dict) or policy.get("schema") != SCHEMA:
        raise P90PolicyError("unsupported p90 policy schema")
    required = (
        "schema",
        "trace_sha256",
        "source",
        "captured_at_utc",
        "sample_count",
        "histogram",
        "p90_bytes",
        "policy_sha256",
    )
    if any(name not in policy for name in required):
        raise P90PolicyError("p90 policy is missing provenance")
    digest = policy.get("policy_sha256")
    if not isinstance(digest, str) or not _HEX64.fullmatch(digest.lower()):
        raise P90PolicyError("p90 policy digest is malformed")
    payload = {name: policy[name] for name in required if name != "policy_sha256"}
    if _digest(payload) != digest.lower():
        raise P90PolicyError("p90 policy digest does not match its contents")
    trace_sha256 = policy["trace_sha256"]
    source = policy["source"]
    captured_at = policy["captured_at_utc"]
    if not isinstance(trace_sha256, str) or not _HEX64.fullmatch(trace_sha256.lower()):
        raise P90PolicyError("p90 policy trace digest is malformed")
    if not isinstance(source, str) or not source.strip() or any(c.isspace() for c in source):
        raise P90PolicyError("p90 policy source is missing or malformed")
    if not isinstance(captured_at, str) or not captured_at.strip() or any(c.isspace() for c in captured_at):
        raise P90PolicyError("p90 policy collection time is missing or malformed")
    trace_sha256 = trace_sha256.lower()
    if expected_trace_sha256 is not None and trace_sha256 != expected_trace_sha256.lower():
        raise P90PolicyError("p90 policy trace does not match the manifest")
    sample_count = policy["sample_count"]
    histogram = policy["histogram"]
    if isinstance(sample_count, bool) or not isinstance(sample_count, int) or sample_count <= 0:
        raise P90PolicyError("p90 policy sample_count is invalid")
    if not isinstance(histogram, list) or not histogram:
        raise P90PolicyError("p90 policy histogram is empty")
    total = 0
    rank = (90 * sample_count + 99) // 100
    p90_from_histogram = None
    previous = 0
    for entry in histogram:
        if not isinstance(entry, dict) or set(entry) != {"range_bytes", "count"}:
            raise P90PolicyError("p90 policy histogram entry is malformed")
        value, count = entry["range_bytes"], entry["count"]
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value <= previous
            or value > _MAX_REQUEST_BYTES
        ):
            raise P90PolicyError("p90 policy histogram is not strictly ordered")
        if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
            raise P90PolicyError("p90 policy histogram count is invalid")
        total += count
        if total > 10_000_000:
            raise P90PolicyError("p90 policy exceeds the bounded sample limit")
        if p90_from_histogram is None and total >= rank:
            p90_from_histogram = value
        previous = value
    if total != sample_count:
        raise P90PolicyError("p90 policy histogram total disagrees with sample_count")
    if not isinstance(policy["p90_bytes"], int) or policy["p90_bytes"] != p90_from_histogram:
        raise P90PolicyError("p90 policy value does not match its histogram")
    return policy


def build_from_file(trace_path: pathlib.Path, policy_path: pathlib.Path) -> dict[str, Any]:
    trace = _read(trace_path)
    declared = trace.get("trace_sha256") if isinstance(trace, dict) else None
    actual = _trace_content_digest(trace) if isinstance(trace, dict) else ""
    if declared != actual:
        raise P90PolicyError("training trace SHA-256 does not match its file")
    policy = build_policy(trace)
    policy_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = policy_path.with_name(policy_path.name + ".tmp")
    temporary.write_text(json.dumps(policy, sort_keys=True, indent=2) + "\n")
    temporary.replace(policy_path)
    return policy


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--training-trace", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    try:
        policy = build_from_file(args.training_trace, args.output)
        validate_policy(policy)
    except P90PolicyError as error:
        print(f"p90 policy validation failed: {error}")
        return 1
    print(json.dumps({"policy_sha256": policy["policy_sha256"], "p90_bytes": policy["p90_bytes"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
