#!/usr/bin/env python3
"""Build the auditable run manifest used by BrewFS perf comparisons."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
from datetime import datetime, timezone
from typing import Mapping
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


REDACTED = "[REDACTED]"
SENSITIVE_NAME_PARTS = ("ACCESS_KEY", "SECRET", "TOKEN", "PASSWORD", "CREDENTIAL")
SENSITIVE_QUERY_PARTS = ("signature", "token", "secret", "password", "credential", "key")

DATASET_KEYS = (
    "PERF_FIO_SIZE",
    "PERF_FIO_SEQREAD_SIZE",
    "PERF_FIO_SEQWRITE_SIZE",
    "PERF_FIO_RANDREAD_SIZE",
    "PERF_FIO_RANDWRITE_SIZE",
    "PERF_FIO_RANDRW_SIZE",
    "PERF_FIO_BIGREAD_SIZE",
    "PERF_FIO_BIGWRITE_SIZE",
)
WORKLOAD_KEYS = (
    "PERF_TOOLS",
    "PERF_FIO_ARGS",
    "PERF_FIO_RUNTIME",
    "PERF_FIO_RW",
    "PERF_FIO_RWMIXREAD",
    "PERF_FIO_BS",
    "PERF_FIO_NUMJOBS",
    "PERF_FIO_IOENGINE",
    "PERF_FIO_IODEPTH",
    "PERF_FIO_DIRECT",
    "PERF_FIO_DIRECT_MATRIX",
    "PERF_FIO_LAT_PERCENTILES",
    "PERF_FIO_OUTPUT_FORMAT",
    "PERF_FIO_SYSCALL_TAIL",
    "PERF_LINES_PROTOCOL",
    "PERF_BACKEND_METRICS_URL",
    "PERF_MAINTENANCE_OBSERVE_SECS",
    "PERF_FIO_CLAT_PERCENTILES",
    "PERF_FIO_PERCENTILE_LIST",
    "PERF_FIO_RANDSEED",
    "PERF_FIO_RANDREPEAT",
)
CACHE_PREPARATION_KEYS = (
    "PERF_FIO_COLD_READ",
    "PERF_FIO_PREFILL_DRAIN",
    "PERF_FIO_PREFILL_REMOUNT",
    "PERF_FIO_COLD_READ_CLEAR_CACHE",
    "PERF_FIO_DROP_CACHES",
    "PERF_FIO_COLD_READ_DROP_CACHES",
)
COMPLETION_KEYS = (
    "PERF_FIO_POST_WRITE_DRAIN",
    "PERF_FIO_POST_WRITE_DRAIN_TIMEOUT_SECS",
    "PERF_FIO_POST_WRITE_DRAIN_PENDING_BYTES",
    "PERF_METADATA_POST_TOOL_DRAIN",
    "PERF_METADATA_POST_TOOL_DRAIN_TIMEOUT_SECS",
    "PERF_METADATA_POST_TOOL_DRAIN_PENDING_BYTES",
    "PERF_FIO_PREFILL_DRAIN_INTERVAL_SECS",
    "PERF_FIO_POST_WRITE_DRAIN_INTERVAL_SECS",
    "PERF_DRAIN_STABLE_SAMPLES",
)
FILESYSTEM_KEYS = (
    "BREWFS_CHUNK_SIZE",
    "BREWFS_BLOCK_SIZE",
    "BREWFS_COMPRESSION",
    "BREWFS_READ_MEMORY_BYTES",
    "BREWFS_READ_SSD_BYTES",
    "BREWFS_WRITE_MEMORY_BYTES",
    "BREWFS_WRITE_SSD_BYTES",
    "BREWFS_MEMORY_BUDGET_BYTES",
    "BREWFS_FUSE_WORKERS",
    "BREWFS_FUSE_MAX_BACKGROUND",
    "BREWFS_FUSE_DIRECT_IO",
    "BREWFS_FUSE_READ_DIRECT_IO",
    "BREWFS_FUSE_WRITE_DIRECT_IO",
    "BREWFS_FUSE_WRITEBACK",
    "BREWFS_WRITEBACK_MODE",
    "BREWFS_WRITEBACK_UPLOAD_CONCURRENCY",
    "BREWFS_S3_MAX_CONCURRENCY",
    "BREWFS_UPLOAD_CONCURRENCY",
    "BREWFS_PREFETCH_ENABLED",
    "BREWFS_PREFETCH_MAX_BYTES",
    "BREWFS_PREFETCH_CONCURRENCY",
    "BREWFS_WRITEBACK_PERSIST_SYNC",
    "BREWFS_WRITEBACK_REQUIRE_STAGE_BEFORE_COMMIT",
    "BREWFS_METADATA_OPEN_CACHE_TTL_MS",
    "BREWFS_METADATA_ALLOW_WRITE_OPEN_CACHE",
    "BREWFS_FUSE_KEEP_CACHE",
    "BREWFS_VOLUME_FORMAT",
    "BREWFS_WORKSPACE_ID",
    "BREWFS_WORKSPACE_NAMESPACE",
    "BREWFS_REQUEST_TRACE",
    "BREWFS_REQUEST_TRACE_SAMPLE_RATE",
    "TOKIO_WORKER_THREADS",
)
OBSERVATION_KEYS = (
    "PERF_FUSE_OPS_LOG",
    "PERF_WRITEBACK_SAMPLER",
    "PERF_WRITEBACK_SAMPLE_INTERVAL_SECS",
    "BREWFS_VFS_TIMING",
    "BREWFS_REQUEST_TRACE",
    "BREWFS_REQUEST_TRACE_SAMPLE_RATE",
    "TOKIO_WORKER_THREADS",
)


def redact_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
    except ValueError:
        return REDACTED
    if not parsed.scheme or not parsed.netloc:
        return value

    hostname = parsed.hostname or ""
    if ":" in hostname and not hostname.startswith("["):
        hostname = f"[{hostname}]"
    port = f":{parsed.port}" if parsed.port is not None else ""
    userinfo = f"{REDACTED}@" if parsed.username is not None else ""
    netloc = f"{userinfo}{hostname}{port}"
    query = urlencode(
        [
            (key, REDACTED if any(part in key.lower() for part in SENSITIVE_QUERY_PARTS) else item)
            for key, item in parse_qsl(parsed.query, keep_blank_values=True)
        ]
    )
    return urlunsplit((parsed.scheme, netloc, parsed.path, query, parsed.fragment))


def redact_value(name: str, value: str) -> str:
    if any(part in name.upper() for part in SENSITIVE_NAME_PARTS):
        return REDACTED
    if "://" in value:
        return redact_url(value)
    return value


def selected(env: Mapping[str, str], keys: tuple[str, ...]) -> dict[str, str | None]:
    return {key: redact_value(key, env[key]) for key in keys if key in env}


def file_sha256(path: pathlib.Path) -> str | None:
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read_first(paths: tuple[pathlib.Path, ...], default: str = "unknown") -> str:
    for path in paths:
        try:
            value = path.read_text().strip()
        except OSError:
            continue
        if value:
            return value
    return default


def detect_resource_limits(cgroup_root: pathlib.Path = pathlib.Path("/sys/fs/cgroup")) -> dict[str, str]:
    return {
        "memory_limit_bytes": read_first(
            (cgroup_root / "memory.max", cgroup_root / "memory" / "memory.limit_in_bytes")
        ),
        "cpu_limit": read_first((cgroup_root / "cpu.max",)),
    }


def build_manifest(
    env: Mapping[str, str],
    *,
    binary_sha256: str | None,
    resource_limits: Mapping[str, str] | None = None,
) -> dict:
    declared_changes = [
        item.strip() for item in env.get("PERF_DECLARED_CHANGES", "").split(",") if item.strip()
    ]
    source_dirty = env.get("BREWFS_SOURCE_DIRTY", "").lower()
    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "revision": env.get("BREWFS_SOURCE_REVISION") or None,
            "dirty": source_dirty in ("1", "true", "yes") if source_dirty else None,
            "dirty_diff_sha256": env.get("BREWFS_SOURCE_DIFF_SHA256") or None,
            "binary_sha256": binary_sha256,
        },
        "experiment": {
            "variable": env.get("PERF_EXPERIMENT_VARIABLE") or None,
            "declared_changes": declared_changes,
        },
        "resolved_endpoints": {
            "metadata": redact_value("BREWFS_META_URL", env.get("BREWFS_META_URL", "")),
            "object": redact_value("BREWFS_S3_ENDPOINT", env.get("BREWFS_S3_ENDPOINT", "")),
        },
        "comparability": {
            "resources": dict(resource_limits or detect_resource_limits()),
            "topology": {
                "runner": env.get("PERF_RUNNER_TOPOLOGY", "compose"),
                "metadata": env.get("BREWFS_META_BACKEND") or None,
                "object": env.get("BREWFS_DATA_BACKEND") or None,
                "metadata_endpoint": redact_value("BREWFS_META_URL", env.get("BREWFS_META_URL", "")),
                "object_endpoint": redact_value("BREWFS_S3_ENDPOINT", env.get("BREWFS_S3_ENDPOINT", "")),
            },
            "dataset": selected(env, DATASET_KEYS),
            "workload": selected(env, WORKLOAD_KEYS),
            "cache_preparation": selected(env, CACHE_PREPARATION_KEYS),
            "completion": selected(env, COMPLETION_KEYS),
            "filesystem": selected(env, FILESYSTEM_KEYS),
            "observation": selected(env, OBSERVATION_KEYS),
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write a redacted BrewFS performance run manifest.")
    parser.add_argument("--output", required=True, type=pathlib.Path)
    parser.add_argument("--binary", type=pathlib.Path, default=pathlib.Path("/usr/local/bin/brewfs"))
    parser.add_argument("--profile", type=pathlib.Path)
    return parser.parse_args()


def load_profile(path: pathlib.Path | None) -> dict[str, str]:
    if path is None or not path.is_file():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key:
            values[key] = value
    return values


def main() -> int:
    args = parse_args()
    resolved_env = dict(os.environ)
    resolved_env.update(load_profile(args.profile))
    manifest = build_manifest(resolved_env, binary_sha256=file_sha256(args.binary))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
