#!/usr/bin/env python3
"""Compare BrewFS compose-xfstests perf artifacts.

The script is intentionally local and dependency-free so controllers can run it
after collecting artifacts from multiple candidate branches. It reads the
artifact layout produced by docker/compose-xfstests/run_redis_perf.sh:

  perf-summary.tsv
  post-write-drain.tsv
  results/fio*.json
  diagnostics/stats-*-after.txt
"""

from __future__ import annotations

import argparse
import csv
import json
import pathlib
import sys
from dataclasses import dataclass
from typing import Iterable


BYTES_PER_MIB = 1024.0 * 1024.0
BYTES_PER_GIB = BYTES_PER_MIB * 1024.0


@dataclass(frozen=True)
class Metric:
    kind: str
    item: str
    name: str
    value: float | str
    unit: str = ""


@dataclass(frozen=True)
class Row:
    kind: str
    item: str
    metric: str
    baseline: float | str
    candidate: float | str
    delta_pct: str
    unit: str


def die(message: str, code: int = 2) -> None:
    print(f"error: {message}", file=sys.stderr)
    raise SystemExit(code)


def warn(message: str) -> None:
    print(f"warning: {message}", file=sys.stderr)


def as_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_seconds(value: str | None) -> tuple[float, bool] | None:
    if value is None or value == "":
        return None
    if value.startswith("timeout:"):
        seconds = as_float(value.split(":", 1)[1], -1.0)
        return (seconds, True) if seconds >= 0 else None
    seconds = as_float(value, -1.0)
    return (seconds, False) if seconds >= 0 else None


def format_value(value: float | str) -> str:
    if isinstance(value, float):
        return f"{value:.3f}"
    return value


def format_delta(baseline: float | str, candidate: float | str) -> str:
    if not isinstance(baseline, float) or not isinstance(candidate, float):
        return ""
    if baseline == 0:
        if candidate == 0:
            return "+0.0"
        return ""
    delta = (candidate - baseline) / baseline * 100.0
    sign = "+" if delta >= 0 else ""
    return f"{sign}{delta:.1f}"


def percentile(op: dict, field: str, pct: float) -> float | None:
    values = op.get(f"{field}_ns", {}).get("percentile", {})
    raw = values.get(f"{pct:.6f}")
    if raw is None:
        raw = values.get(str(pct))
    if raw is None and float(pct).is_integer():
        raw = values.get(str(int(pct)))
    value = as_float(raw, -1.0)
    return value if value >= 0 else None


def latency_stats(ops: list[dict], field: str) -> dict[str, float | None]:
    values = [op.get(f"{field}_ns", {}) for op in ops]
    samples = [(as_float(value.get("mean"), -1.0), as_float(value.get("N"), -1.0)) for value in values]
    weighted = [(mean, count) for mean, count in samples if mean >= 0 and count > 0]
    total_n = sum(count for _, count in weighted)
    mean = sum(value * count for value, count in weighted) / total_n if total_n else None
    result: dict[str, float | None] = {"mean_ns": mean}
    for pct, name in ((50.0, "p50_ns"), (95.0, "p95_ns"), (99.0, "p99_ns"), (99.9, "p999_ns")):
        samples = [value for op in ops if (value := percentile(op, field, pct)) is not None]
        # Pxx from multiple fio jobs cannot be averaged.  The worst group is
        # retained, which is conservative and preserves the group boundary.
        result[name] = max(samples) if samples else None
    return result


def op_totals(jobs: list[dict], op_name: str) -> dict[str, float]:
    ops = [job.get(op_name, {}) for job in jobs]
    clat = latency_stats(ops, "clat")
    slat = latency_stats(ops, "slat")
    total = latency_stats(ops, "lat")
    runtimes = [as_float(op.get("runtime")) for op in ops if as_float(op.get("runtime")) > 0]
    result: dict[str, float] = {
        "io_bytes": sum(as_float(op.get("io_bytes")) for op in ops),
        "total_ios": sum(as_float(op.get("total_ios")) for op in ops),
        "bw_bytes": sum(as_float(op.get("bw_bytes")) for op in ops),
        "iops": sum(as_float(op.get("iops")) for op in ops),
        "runtime_ms": max(runtimes) if runtimes else 0.0,
    }
    for prefix, stats in (("clat", clat), ("slat", slat), ("total", total)):
        for suffix, value in stats.items():
            if value is not None:
                result[f"{prefix}_{suffix}"] = value
    return result


def first_options(jobs: list[dict]) -> dict:
    for job in jobs:
        options = job.get("job options", {})
        if options:
            return options
    return {}


def iter_fio_paths(artifact_dir: pathlib.Path) -> list[pathlib.Path]:
    paths: list[pathlib.Path] = []
    for subdir in ("results", "fio"):
        root = artifact_dir / subdir
        if root.exists():
            paths.extend(root.glob("fio*.json"))
    paths.extend(artifact_dir.glob("fio*.json"))
    return sorted({path.resolve(): path for path in paths}.values())


def load_fio_metrics(artifact_dir: pathlib.Path) -> list[Metric]:
    metrics: list[Metric] = []
    for path in iter_fio_paths(artifact_dir):
        try:
            data = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            warn(f"{artifact_dir}: skipping unreadable fio JSON {path.name}: {exc}")
            continue
        jobs = data.get("jobs", [])
        if not jobs:
            continue

        item = path.stem
        options = first_options(jobs)
        read = op_totals(jobs, "read")
        write = op_totals(jobs, "write")
        runtime_ms = max(read["runtime_ms"], write["runtime_ms"])
        raw_job_runtime_ms = max((as_float(job.get("job_runtime")) for job in jobs), default=0.0)

        for prefix, op in (("read", read), ("write", write)):
            if op["bw_bytes"] <= 0:
                continue
            missing_latency = []
            group_counts = []
            for index, job in enumerate(jobs):
                group = job.get(prefix, {})
                if as_float(group.get("total_ios")) <= 0:
                    continue
                group_item = f"{item}/group-{index}"
                count = as_float(group.get("total_ios"))
                group_counts.append(count)
                metrics.append(Metric("fio_group", group_item, f"{prefix}_total_ios", count, "ios"))
                metrics.append(Metric("fio_group", group_item, f"{prefix}_p99_sample_sufficient", float(count >= 1000), "bool"))
                for field in ("slat", "clat", "lat"):
                    for pct, suffix in ((50.0, "p50"), (95.0, "p95"), (99.0, "p99"), (99.9, "p999")):
                        value = percentile(group, field, pct)
                        if value is None:
                            missing_latency.append(f"group-{index}/{field}/{suffix}")
                        else:
                            metrics.append(Metric("fio_group", group_item, f"{prefix}_{field}_{suffix}_ms", value / 1_000_000.0, "ms"))
            metrics.append(Metric("gap", item, f"{prefix}_missing_latency_percentiles", ",".join(missing_latency) if missing_latency else "none", ""))
            metrics.append(Metric("fio", item, f"{prefix}_p99_sample_sufficient", float(bool(group_counts) and min(group_counts) >= 1000), "bool"))
            metrics.extend([
                Metric("fio", item, f"{prefix}_io_mib", op["io_bytes"] / BYTES_PER_MIB, "MiB"),
                Metric("fio", item, f"{prefix}_total_ios", op["total_ios"], "ios"),
                Metric("fio", item, f"{prefix}_bw_mib_s", op["bw_bytes"] / BYTES_PER_MIB, "MiB/s"),
                Metric("fio", item, f"{prefix}_iops", op["iops"], "iops"),
            ])
            for latency in ("clat", "slat", "total"):
                for suffix in ("mean_ms", "p50_ms", "p95_ms", "p99_ms", "p999_ms"):
                    key = f"{latency}_{suffix[:-3]}_ns"
                    if key not in op:
                        continue
                    metric_name = f"{prefix}_{latency}_{suffix}"
                    metrics.append(Metric("fio", item, metric_name, op[key] / 1_000_000.0, "ms"))
                    # Keep the historical clat names stable for existing
                    # reports while exposing explicit slat/total distributions.
                    if latency == "clat" and suffix in {"mean_ms", "p95_ms", "p99_ms", "p999_ms"}:
                        metrics.append(Metric("fio", item, f"{prefix}_{suffix}", op[key] / 1_000_000.0, "ms"))

        if runtime_ms > 0:
            metrics.append(Metric("fio", item, "active_io_runtime_s", runtime_ms / 1000.0, "s"))
        if raw_job_runtime_ms > 0:
            metrics.append(Metric("fio", item, "raw_job_runtime_s", raw_job_runtime_ms / 1000.0, "s"))
        for opt_name in (
            "rw",
            "bs",
            "numjobs",
            "direct",
            "lat_percentiles",
            "clat_percentiles",
            "percentile_list",
            "randseed",
            "randrepeat",
        ):
            if opt_name in options:
                metrics.append(Metric("fio_config", item, opt_name, str(options[opt_name]), ""))
    return metrics


def load_summary_metrics(artifact_dir: pathlib.Path) -> list[Metric]:
    path = artifact_dir / "perf-summary.tsv"
    if not path.exists():
        warn(f"{artifact_dir}: missing perf-summary.tsv")
        return []

    metrics: list[Metric] = []
    try:
        with path.open(newline="") as f:
            rows = list(csv.DictReader(f, delimiter="\t"))
    except OSError as exc:
        warn(f"{artifact_dir}: failed reading perf-summary.tsv: {exc}")
        return []

    for row in rows:
        tool = row.get("tool", "")
        if not tool:
            continue
        if row.get("status"):
            metrics.append(Metric("summary", tool, "status", row["status"], ""))
        seconds = as_float(row.get("seconds"), -1.0)
        if seconds >= 0:
            metrics.append(Metric("summary", tool, "tool_wall_s", seconds, "s"))
    return metrics


def load_drain_metrics(artifact_dir: pathlib.Path) -> list[Metric]:
    path = artifact_dir / "post-write-drain.tsv"
    if not path.exists():
        warn(f"{artifact_dir}: missing post-write-drain.tsv")
        return []

    metrics: list[Metric] = []
    try:
        with path.open(newline="") as f:
            rows = list(csv.DictReader(f, delimiter="\t"))
    except OSError as exc:
        warn(f"{artifact_dir}: failed reading post-write-drain.tsv: {exc}")
        return []

    for row in rows:
        tool = row.get("tool", "")
        if not tool:
            continue
        # BrewFS artifacts record both the runner's first clear point and the
        # extended strict-drain interval.  Effective bandwidth must use the
        # latter; retain the old column names as a compatibility fallback for
        # older artifacts and JuiceFS fixtures.
        drain_value = (
            row.get("extended_strict_drain_s")
            or row.get("post_write_drain_s")
            or row.get("post_fio_drain_s")
        )
        parsed = parse_seconds(drain_value)
        if parsed:
            seconds, timed_out = parsed
            if row.get("extended_strict_drain_s"):
                metrics.append(Metric("drain", tool, "extended_strict_drain_s", seconds, "s"))
            metrics.append(Metric("drain", tool, "completion_evidence", "extended" if row.get("extended_strict_drain_s") else "legacy-unverified", ""))
            if row.get("runner_drain_s"):
                runner = parse_seconds(row.get("runner_drain_s"))
                if runner:
                    runner_seconds, runner_timed_out = runner
                    metrics.append(Metric("drain", tool, "runner_drain_s", runner_seconds, "s"))
                    timed_out = timed_out or runner_timed_out
            # Keep the historical metric key so consumers that only know the
            # old schema continue to see a value with the strict-drain
            # semantics.
            metrics.append(Metric("drain", tool, "post_write_drain_s", seconds, "s"))
            metrics.append(Metric("drain", tool, "post_write_drain_timeout", 1.0 if timed_out else 0.0, "bool"))
        elif drain_value:
            metrics.append(Metric("gap", tool, "invalid_drain", str(drain_value), ""))
        for source, name in (
            ("pending_bytes", "drain_pending_mib"),
            ("dirty_bytes", "drain_dirty_mib"),
            ("buffer_dirty_bytes", "drain_buffer_dirty_mib"),
        ):
            value = as_float(row.get(source), -1.0)
            if value >= 0:
                metrics.append(Metric("drain", tool, name, value / BYTES_PER_MIB, "MiB"))
    return metrics


STAT_METRICS = {
    "brewfs_writeback_recent_pending_upload_bytes": ("backpressure_pending_mib", "MiB", BYTES_PER_MIB),
    "brewfs_writeback_dirty_bytes": ("writeback_dirty_mib", "MiB", BYTES_PER_MIB),
    "brewfs_writeback_live_dirty_bytes": ("writeback_live_dirty_mib", "MiB", BYTES_PER_MIB),
    "brewfs_writeback_live_slices": ("writeback_live_slices", "slices", 1.0),
    "brewfs_writeback_live_normal_only_bytes": (
        "writeback_live_normal_only_mib",
        "MiB",
        BYTES_PER_MIB,
    ),
    "brewfs_writeback_live_normal_only_slices": (
        "writeback_live_normal_only_slices",
        "slices",
        1.0,
    ),
    "brewfs_writeback_live_cached_only_bytes": (
        "writeback_live_cached_only_mib",
        "MiB",
        BYTES_PER_MIB,
    ),
    "brewfs_writeback_live_cached_only_slices": (
        "writeback_live_cached_only_slices",
        "slices",
        1.0,
    ),
    "brewfs_writeback_live_mixed_origin_bytes": (
        "writeback_live_mixed_origin_mib",
        "MiB",
        BYTES_PER_MIB,
    ),
    "brewfs_writeback_live_mixed_origin_slices": (
        "writeback_live_mixed_origin_slices",
        "slices",
        1.0,
    ),
    "brewfs_writeback_live_unknown_origin_bytes": (
        "writeback_live_unknown_origin_mib",
        "MiB",
        BYTES_PER_MIB,
    ),
    "brewfs_writeback_live_unknown_origin_slices": (
        "writeback_live_unknown_origin_slices",
        "slices",
        1.0,
    ),
    "brewfs_buffer_dirty_bytes": ("buffer_dirty_mib", "MiB", BYTES_PER_MIB),
    "brewfs_writeback_recent_uploaded_bytes": ("writeback_recent_uploaded_mib", "MiB", BYTES_PER_MIB),
    "brewfs_writeback_recent_pending_upload_slices": ("writeback_recent_pending_slices", "slices", 1.0),
    "brewfs_writeback_recent_uploaded_slices": ("writeback_recent_uploaded_slices", "slices", 1.0),
    "brewfs_fuse_write_bytes_total": ("fuse_write_mib", "MiB", BYTES_PER_MIB),
    "brewfs_fuse_read_bytes_total": ("fuse_read_mib", "MiB", BYTES_PER_MIB),
    "brewfs_s3_put_ops_total": ("s3_put_ops", "ops", 1.0),
    "brewfs_s3_get_ops_total": ("s3_get_ops", "ops", 1.0),
    "brewfs_s3_put_bytes_total": ("s3_put_mib", "MiB", BYTES_PER_MIB),
    "brewfs_s3_get_bytes_total": ("s3_get_mib", "MiB", BYTES_PER_MIB),
    "brewfs_s3_put_lat_us_total": ("s3_put_total_ms", "ms", 1000.0),
    "brewfs_s3_get_lat_us_total": ("s3_get_total_ms", "ms", 1000.0),
    "brewfs_s3_put_avg_lat_us": ("s3_put_avg_ms", "ms", 1000.0),
    "brewfs_s3_get_avg_lat_us": ("s3_get_avg_ms", "ms", 1000.0),
    "brewfs_s3_put_prepare_avg_lat_us": ("s3_put_prepare_avg_ms", "ms", 1000.0),
    "brewfs_s3_put_cache_avg_lat_us": ("s3_put_cache_avg_ms", "ms", 1000.0),
    "brewfs_writeback_backpressure_soft_sleep_ops": ("writeback_soft_sleep_ops", "ops", 1.0),
    "brewfs_writeback_backpressure_soft_sleep_us": ("writeback_soft_sleep_ms", "ms", 1000.0),
    "brewfs_writeback_backpressure_hard_wait_ops": ("writeback_hard_wait_ops", "ops", 1.0),
    "brewfs_writeback_backpressure_hard_wait_us": ("writeback_hard_wait_ms", "ms", 1000.0),
    "brewfs_writeback_stage_inflight_bytes": ("writeback_stage_inflight_mib", "MiB", BYTES_PER_MIB),
    "brewfs_writeback_remote_upload_inflight_bytes": ("writeback_remote_upload_inflight_mib", "MiB", BYTES_PER_MIB),
    "brewfs_writeback_stage_ops_total": ("writeback_stage_ops", "ops", 1.0),
    "brewfs_writeback_stage_bytes_total": ("writeback_stage_mib", "MiB", BYTES_PER_MIB),
    "brewfs_writeback_stage_lat_us_total": ("writeback_stage_total_ms", "ms", 1000.0),
    "brewfs_writeback_stage_failures_total": ("writeback_stage_failures", "ops", 1.0),
    "brewfs_writeback_commit_before_stage_ops_total": ("writeback_commit_before_stage_ops", "ops", 1.0),
    "brewfs_writeback_commit_wait_upload_ops_total": ("writeback_commit_wait_upload_ops", "ops", 1.0),
    "brewfs_writeback_commit_wait_upload_us_total": ("writeback_commit_wait_upload_ms", "ms", 1000.0),
    "brewfs_writeback_commit_wait_upload_size_ops_total": ("writeback_commit_wait_upload_size_ops", "ops", 1.0),
    "brewfs_writeback_commit_wait_upload_size_us_total": ("writeback_commit_wait_upload_size_ms", "ms", 1000.0),
    "brewfs_writeback_commit_wait_upload_max_unflushed_ops_total": (
        "writeback_commit_wait_upload_max_unflushed_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_commit_wait_upload_max_unflushed_us_total": (
        "writeback_commit_wait_upload_max_unflushed_ms",
        "ms",
        1000.0,
    ),
    "brewfs_writeback_commit_wait_upload_explicit_flush_ops_total": (
        "writeback_commit_wait_upload_explicit_flush_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_commit_wait_upload_explicit_flush_us_total": (
        "writeback_commit_wait_upload_explicit_flush_ms",
        "ms",
        1000.0,
    ),
    "brewfs_writeback_commit_wait_upload_auto_ops_total": ("writeback_commit_wait_upload_auto_ops", "ops", 1.0),
    "brewfs_writeback_commit_wait_upload_auto_us_total": ("writeback_commit_wait_upload_auto_ms", "ms", 1000.0),
    "brewfs_writeback_commit_wait_upload_commit_age_ops_total": (
        "writeback_commit_wait_upload_commit_age_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_commit_wait_upload_commit_age_us_total": (
        "writeback_commit_wait_upload_commit_age_ms",
        "ms",
        1000.0,
    ),
    "brewfs_writeback_commit_wait_upload_unknown_reason_ops_total": (
        "writeback_commit_wait_upload_unknown_reason_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_commit_wait_upload_unknown_reason_us_total": (
        "writeback_commit_wait_upload_unknown_reason_ms",
        "ms",
        1000.0,
    ),
    "brewfs_writeback_commit_wait_upload_normal_only_ops_total": (
        "writeback_commit_wait_upload_normal_only_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_commit_wait_upload_normal_only_us_total": (
        "writeback_commit_wait_upload_normal_only_ms",
        "ms",
        1000.0,
    ),
    "brewfs_writeback_commit_wait_upload_cached_only_ops_total": (
        "writeback_commit_wait_upload_cached_only_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_commit_wait_upload_cached_only_us_total": (
        "writeback_commit_wait_upload_cached_only_ms",
        "ms",
        1000.0,
    ),
    "brewfs_writeback_commit_wait_upload_mixed_origin_ops_total": (
        "writeback_commit_wait_upload_mixed_origin_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_commit_wait_upload_mixed_origin_us_total": (
        "writeback_commit_wait_upload_mixed_origin_ms",
        "ms",
        1000.0,
    ),
    "brewfs_writeback_commit_wait_upload_unknown_origin_ops_total": (
        "writeback_commit_wait_upload_unknown_origin_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_commit_wait_upload_unknown_origin_us_total": (
        "writeback_commit_wait_upload_unknown_origin_ms",
        "ms",
        1000.0,
    ),
    "brewfs_writeback_commit_wait_retry_ops_total": ("writeback_commit_wait_retry_ops", "ops", 1.0),
    "brewfs_writeback_commit_wait_retry_us_total": ("writeback_commit_wait_retry_ms", "ms", 1000.0),
    "brewfs_writeback_slice_create_ops_total": ("writeback_slice_create_ops", "ops", 1.0),
    "brewfs_writeback_slice_reuse_ops_total": ("writeback_slice_reuse_ops", "ops", 1.0),
    "brewfs_writeback_slice_reject_older_unique_ops_total": (
        "writeback_slice_reject_older_unique_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_slice_reject_dispatched_prefix_ops_total": (
        "writeback_slice_reject_dispatched_prefix_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_freeze_size_ops_total": ("writeback_freeze_size_ops", "ops", 1.0),
    "brewfs_writeback_freeze_size_bytes_total": ("writeback_freeze_size_mib", "MiB", BYTES_PER_MIB),
    "brewfs_writeback_freeze_max_unflushed_ops_total": (
        "writeback_freeze_max_unflushed_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_freeze_max_unflushed_bytes_total": (
        "writeback_freeze_max_unflushed_mib",
        "MiB",
        BYTES_PER_MIB,
    ),
    "brewfs_writeback_freeze_explicit_flush_ops_total": (
        "writeback_freeze_explicit_flush_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_freeze_explicit_flush_bytes_total": (
        "writeback_freeze_explicit_flush_mib",
        "MiB",
        BYTES_PER_MIB,
    ),
    "brewfs_writeback_freeze_auto_ops_total": ("writeback_freeze_auto_ops", "ops", 1.0),
    "brewfs_writeback_freeze_auto_bytes_total": (
        "writeback_freeze_auto_mib",
        "MiB",
        BYTES_PER_MIB,
    ),
    "brewfs_writeback_freeze_commit_age_ops_total": (
        "writeback_freeze_commit_age_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_freeze_commit_age_bytes_total": (
        "writeback_freeze_commit_age_mib",
        "MiB",
        BYTES_PER_MIB,
    ),
    "brewfs_writeback_upload_batch_ops_total": ("writeback_upload_batch_ops", "ops", 1.0),
    "brewfs_writeback_upload_batch_bytes_total": (
        "writeback_upload_batch_mib",
        "MiB",
        BYTES_PER_MIB,
    ),
    "brewfs_writeback_upload_batch_blocks_total": (
        "writeback_upload_batch_blocks",
        "blocks",
        1.0,
    ),
    "brewfs_writeback_upload_batch_single_block_ops_total": (
        "writeback_upload_batch_single_block_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_upload_batch_multi_block_ops_total": (
        "writeback_upload_batch_multi_block_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_upload_partial_tail_ops_total": (
        "writeback_upload_partial_tail_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_upload_partial_tail_size_ops_total": (
        "writeback_upload_partial_tail_size_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_upload_partial_tail_max_unflushed_ops_total": (
        "writeback_upload_partial_tail_max_unflushed_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_upload_partial_tail_explicit_flush_ops_total": (
        "writeback_upload_partial_tail_explicit_flush_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_upload_partial_tail_auto_ops_total": (
        "writeback_upload_partial_tail_auto_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_upload_partial_tail_normal_only_ops_total": (
        "writeback_upload_partial_tail_normal_only_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_upload_partial_tail_cached_only_ops_total": (
        "writeback_upload_partial_tail_cached_only_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_upload_partial_tail_mixed_origin_ops_total": (
        "writeback_upload_partial_tail_mixed_origin_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_upload_partial_tail_unknown_origin_ops_total": (
        "writeback_upload_partial_tail_unknown_origin_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_upload_partial_tail_auto_age_ops_total": (
        "writeback_upload_partial_tail_auto_age_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_upload_partial_tail_auto_idle_ops_total": (
        "writeback_upload_partial_tail_auto_idle_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_upload_partial_tail_auto_pressure_ops_total": (
        "writeback_upload_partial_tail_auto_pressure_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_upload_partial_tail_auto_too_many_ops_total": (
        "writeback_upload_partial_tail_auto_too_many_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_upload_partial_tail_auto_buffer_high_ops_total": (
        "writeback_upload_partial_tail_auto_buffer_high_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_upload_partial_tail_auto_flush_duration_ops_total": (
        "writeback_upload_partial_tail_auto_flush_duration_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_upload_partial_tail_auto_unknown_ops_total": (
        "writeback_upload_partial_tail_auto_unknown_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_upload_partial_tail_auto_normal_only_ops_total": (
        "writeback_upload_partial_tail_auto_normal_only_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_upload_partial_tail_auto_cached_only_ops_total": (
        "writeback_upload_partial_tail_auto_cached_only_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_upload_partial_tail_auto_mixed_origin_ops_total": (
        "writeback_upload_partial_tail_auto_mixed_origin_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_upload_partial_tail_auto_unknown_origin_ops_total": (
        "writeback_upload_partial_tail_auto_unknown_origin_ops",
        "ops",
        1.0,
    ),
    "brewfs_writeback_upload_partial_tail_commit_age_ops_total": (
        "writeback_upload_partial_tail_commit_age_ops",
        "ops",
        1.0,
    ),
}

CRITICAL_STAT_METRICS = (
    "brewfs_s3_put_ops_total",
    "brewfs_s3_put_bytes_total",
    "brewfs_s3_get_ops_total",
    "brewfs_s3_get_bytes_total",
    "brewfs_fuse_write_bytes_total",
    "brewfs_fuse_read_bytes_total",
    "brewfs_writeback_recent_pending_upload_bytes",
    "brewfs_writeback_dirty_bytes",
    "brewfs_writeback_metadata_inflight",
    "brewfs_memory_cas_retries_total",
    "brewfs_memory_allocation_rejections_total",
    "brewfs_writeback_backpressure_soft_sleep_ops",
    "brewfs_writeback_backpressure_soft_sleep_us",
    "brewfs_writeback_backpressure_hard_wait_ops",
    "brewfs_writeback_backpressure_hard_wait_us",
    "brewfs_writeback_slice_create_ops_total",
    "brewfs_writeback_upload_batch_ops_total",
    "brewfs_writeback_upload_batch_bytes_total",
)

REQUEST_OBSERVATION_METRICS = (
    "brewfs_request_observation_enabled",
    "brewfs_request_observation_sample_rate",
    "brewfs_request_observation_received_total",
    "brewfs_request_observation_sampled_total",
    "brewfs_request_observation_closed_total",
    "brewfs_request_observation_cancelled_total",
    "brewfs_request_observation_errors_total",
    "brewfs_request_observation_dropped_total",
    "brewfs_request_observation_active",
    "brewfs_request_observation_closure_gap_ns_total",
    "brewfs_request_observation_closure_gap_ns_max",
    "brewfs_request_fuse_queue_depth",
    "brewfs_request_fuse_queue_oldest_age_ns",
)


def request_observation_raw_names() -> tuple[str, ...]:
    names = list(REQUEST_OBSERVATION_METRICS)
    for segment in (
        "fuse_queue_wait",
        "file_lock_wait",
        "chunk_lock_wait",
        "dirty_merge",
        "flush_wait",
        "metadata_rpc_wait",
        "metadata_commit",
        "object_put_queue_wait",
        "object_put",
        "reply",
        "handler",
    ):
        names.extend((f"brewfs_request_{segment}_ns_count", f"brewfs_request_{segment}_ns_sum"))
    for segment in (
        "file_lock_wait",
        "chunk_lock_wait",
        "flush_wait",
        "metadata_rpc_wait",
        "object_put_queue_wait",
    ):
        names.extend(
            (
                f"brewfs_request_{segment}_queue_depth",
                f"brewfs_request_{segment}_queue_oldest_age_ns",
            )
        )
    for operation in ("read", "write", "close", "fsync", "metadata"):
        names.extend(
            (
                f"brewfs_request_operation_{operation}_ns_count",
                f"brewfs_request_operation_{operation}_ns_sum",
            )
        )
    return tuple(names)


def raw_metric(raw_metrics: dict[str, float], name: str) -> float | None:
    return raw_metrics.get(name)


def append_ratio_metric(
    metrics: list[Metric],
    item: str,
    name: str,
    numerator: float | None,
    denominator: float | None,
    unit: str,
) -> None:
    if numerator is None or denominator is None or denominator <= 0:
        return
    metrics.append(Metric("amplification", item, name, numerator / denominator, unit))


def append_stats_derived_metrics(metrics: list[Metric], item: str, raw_metrics: dict[str, float]) -> None:
    put_ops = raw_metric(raw_metrics, "brewfs_s3_put_ops_total")
    put_bytes = raw_metric(raw_metrics, "brewfs_s3_put_bytes_total")
    get_ops = raw_metric(raw_metrics, "brewfs_s3_get_ops_total")
    get_bytes = raw_metric(raw_metrics, "brewfs_s3_get_bytes_total")
    fuse_write_bytes = raw_metric(raw_metrics, "brewfs_fuse_write_bytes_total")
    fuse_read_bytes = raw_metric(raw_metrics, "brewfs_fuse_read_bytes_total")
    uploaded_bytes = raw_metric(raw_metrics, "brewfs_writeback_recent_uploaded_bytes")
    live_bytes = raw_metric(raw_metrics, "brewfs_writeback_live_dirty_bytes")
    live_slices = raw_metric(raw_metrics, "brewfs_writeback_live_slices")
    pending_bytes = raw_metric(raw_metrics, "brewfs_writeback_recent_pending_upload_bytes")
    pending_slices = raw_metric(raw_metrics, "brewfs_writeback_recent_pending_upload_slices")
    upload_batch_ops = raw_metric(raw_metrics, "brewfs_writeback_upload_batch_ops_total")
    upload_batch_bytes = raw_metric(raw_metrics, "brewfs_writeback_upload_batch_bytes_total")
    upload_batch_blocks = raw_metric(raw_metrics, "brewfs_writeback_upload_batch_blocks_total")
    upload_batch_single_block_ops = raw_metric(
        raw_metrics, "brewfs_writeback_upload_batch_single_block_ops_total"
    )
    partial_tail_ops = raw_metric(raw_metrics, "brewfs_writeback_upload_partial_tail_ops_total")

    append_ratio_metric(metrics, item, "upload_byte_amp", put_bytes, fuse_write_bytes, "ratio")
    append_ratio_metric(metrics, item, "writeback_upload_byte_amp", uploaded_bytes, fuse_write_bytes, "ratio")
    append_ratio_metric(metrics, item, "s3_put_avg_object_mib", put_bytes, put_ops * BYTES_PER_MIB if put_ops else None, "MiB/op")
    append_ratio_metric(metrics, item, "s3_get_avg_object_mib", get_bytes, get_ops * BYTES_PER_MIB if get_ops else None, "MiB/op")
    append_ratio_metric(metrics, item, "put_ops_per_gib_written", put_ops, fuse_write_bytes / BYTES_PER_GIB if fuse_write_bytes else None, "ops/GiB")
    append_ratio_metric(metrics, item, "get_ops_per_gib_read", get_ops, fuse_read_bytes / BYTES_PER_GIB if fuse_read_bytes else None, "ops/GiB")
    append_ratio_metric(metrics, item, "writeback_avg_live_slice_mib", live_bytes, live_slices * BYTES_PER_MIB if live_slices else None, "MiB/slice")
    append_ratio_metric(
        metrics,
        item,
        "writeback_avg_recent_pending_slice_mib",
        pending_bytes,
        pending_slices * BYTES_PER_MIB if pending_slices else None,
        "MiB/slice",
    )
    append_ratio_metric(
        metrics,
        item,
        "writeback_avg_upload_batch_mib",
        upload_batch_bytes,
        upload_batch_ops * BYTES_PER_MIB if upload_batch_ops else None,
        "MiB/op",
    )
    append_ratio_metric(
        metrics,
        item,
        "writeback_avg_upload_batch_blocks",
        upload_batch_blocks,
        upload_batch_ops,
        "blocks/op",
    )
    append_ratio_metric(
        metrics,
        item,
        "writeback_single_block_batch_ratio",
        upload_batch_single_block_ops,
        upload_batch_ops,
        "ratio",
    )
    append_ratio_metric(
        metrics,
        item,
        "writeback_partial_tail_ratio",
        partial_tail_ops,
        upload_batch_ops,
        "ratio",
    )
    append_ratio_metric(
        metrics,
        item,
        "s3_puts_per_upload_batch",
        put_ops,
        upload_batch_ops,
        "ops/batch",
    )


def append_missing_stats_metric(metrics: list[Metric], item: str, raw_metrics: dict[str, float]) -> None:
    missing = [name for name in CRITICAL_STAT_METRICS if name not in raw_metrics]
    metrics.append(Metric("gap", item, "missing_critical_stats", ",".join(missing) if missing else "none", ""))


def parse_stats_file(path: pathlib.Path) -> dict[str, float]:
    parsed: dict[str, float] = {}
    try:
        lines = path.read_text(errors="replace").splitlines()
    except OSError as exc:
        warn(f"skipping unreadable stats file {path}: {exc}")
        return parsed

    for raw in lines:
        raw = raw.strip()
        if not raw.startswith("brewfs_"):
            continue
        parts = raw.split()
        if len(parts) < 2:
            continue
        try:
            parsed[parts[0]] = float(parts[1])
        except ValueError:
            continue
    return parsed


def stats_item_name(path: pathlib.Path) -> str:
    name = path.name
    if name.startswith("stats-"):
        name = name[len("stats-") :]
    if name.endswith("-after.txt"):
        name = name[: -len("-after.txt")]
    elif name.endswith(".txt"):
        name = name[:-4]
    return name


def append_request_observation_metrics(
    metrics: list[Metric], item: str, raw_metrics: dict[str, float]
) -> None:
    required = request_observation_raw_names()
    missing = [name for name in required if name not in raw_metrics]
    metrics.append(
        Metric(
            "request_observation_gap",
            item,
            "missing_fields",
            ",".join(missing) if missing else "none",
            "",
        )
    )
    for name in required:
        value = raw_metrics.get(name)
        if value is not None:
            unit = "ns" if name.endswith(("_ns_sum", "_age_ns", "_gap_ns_total", "_gap_ns_max")) else "count"
            metrics.append(Metric("request_observation", item, name, value, unit))

    received = raw_metrics.get("brewfs_request_observation_received_total")
    sampled = raw_metrics.get("brewfs_request_observation_sampled_total")
    closed = raw_metrics.get("brewfs_request_observation_closed_total")
    dropped = raw_metrics.get("brewfs_request_observation_dropped_total")
    gap = raw_metrics.get("brewfs_request_observation_closure_gap_ns_total")
    intervals = [raw_metrics.get(f"brewfs_request_{segment}_ns_sum") for segment in ("fuse_queue_wait", "handler", "reply")]
    accounted = sum(intervals) if all(value is not None for value in intervals) else None
    if sampled is not None and sampled > 0:
        if closed is not None:
            metrics.append(Metric("request_observation", item, "closure_rate", closed / sampled, "ratio"))
        if dropped is not None:
            metrics.append(Metric("request_observation", item, "dropped_rate", dropped / sampled, "ratio"))
    if gap is not None and accounted is not None and accounted + gap > 0:
        metrics.append(
            Metric(
                "request_observation",
                item,
                "closure_gap_ratio",
                gap / (accounted + gap),
                "ratio",
            )
        )
    if received is not None and sampled is not None and received > 0:
        metrics.append(Metric("request_observation", item, "sample_fraction", sampled / received, "ratio"))


def load_stats_metrics(artifact_dir: pathlib.Path) -> list[Metric]:
    diag_dir = artifact_dir / "diagnostics"
    if not diag_dir.exists():
        warn(f"{artifact_dir}: missing diagnostics directory")
        return []

    metrics: list[Metric] = []
    for path in sorted(diag_dir.glob("stats-*-after.txt")):
        item = stats_item_name(path)
        raw_metrics = parse_stats_file(path)
        for raw_name, (metric_name, unit, divisor) in STAT_METRICS.items():
            if raw_name in raw_metrics:
                metrics.append(Metric("stats", item, metric_name, raw_metrics[raw_name] / divisor, unit))
        append_stats_derived_metrics(metrics, item, raw_metrics)
        append_missing_stats_metric(metrics, item, raw_metrics)
        append_request_observation_metrics(metrics, item, raw_metrics)
    return metrics


def get_metric_value(metrics: dict[tuple[str, str, str], Metric], kind: str, item: str, name: str) -> float | None:
    metric = metrics.get((kind, item, name))
    if metric is None or not isinstance(metric.value, float):
        return None
    return metric.value


def append_artifact_gap_metrics(metrics: list[Metric], artifact_dir: pathlib.Path) -> None:
    missing: list[str] = []
    if not (artifact_dir / "perf-summary.tsv").exists():
        missing.append("perf-summary.tsv")
    if not (artifact_dir / "post-write-drain.tsv").exists():
        missing.append("post-write-drain.tsv")
    if not iter_fio_paths(artifact_dir):
        missing.append("results/fio*.json")
    diag_dir = artifact_dir / "diagnostics"
    if not diag_dir.exists() or not list(diag_dir.glob("stats-*-after.txt")):
        missing.append("diagnostics/stats-*-after.txt")
    metrics.append(Metric("gap", "__artifact__", "missing_artifact_sections", ",".join(missing) if missing else "none", ""))
    invalid = sorted(path.name for path in artifact_dir.glob("*.invalid"))
    metrics.append(Metric("gap", "__artifact__", "invalid_observation_markers", ",".join(invalid) if invalid else "none", ""))


def append_runtime_metrics(metrics: list[Metric]) -> list[Metric]:
    metric_map = {(metric.kind, metric.item, metric.name): metric for metric in metrics}
    items = sorted({metric.item for metric in metrics})
    derived: list[Metric] = []
    for item in items:
        wall_s = get_metric_value(metric_map, "summary", item, "tool_wall_s")
        active_s = get_metric_value(metric_map, "fio", item, "active_io_runtime_s")
        raw_job_s = get_metric_value(metric_map, "fio", item, "raw_job_runtime_s")
        drain_s = get_metric_value(metric_map, "drain", item, "extended_strict_drain_s")
        if drain_s is None:
            drain_s = get_metric_value(metric_map, "drain", item, "post_write_drain_s")

        if wall_s is not None and active_s is not None:
            derived.append(Metric("runtime", item, "wall_active_tail_s", wall_s - active_s, "s"))
            if active_s > 0:
                derived.append(Metric("runtime", item, "wall_active_ratio", wall_s / active_s, "ratio"))
        if wall_s is not None and raw_job_s is not None:
            derived.append(Metric("runtime", item, "wall_job_runtime_tail_s", wall_s - raw_job_s, "s"))
            if raw_job_s > 0:
                derived.append(Metric("runtime", item, "wall_job_runtime_ratio", wall_s / raw_job_s, "ratio"))
        if active_s is not None and drain_s is not None:
            derived.append(Metric("runtime", item, "active_plus_drain_s", active_s + drain_s, "s"))
            if active_s > 0:
                derived.append(Metric("runtime", item, "drain_active_ratio", drain_s / active_s, "ratio"))
        read_mib = get_metric_value(metric_map, "fio", item, "read_io_mib")
        write_mib = get_metric_value(metric_map, "fio", item, "write_io_mib")
        io_totals = {"read": read_mib, "write": write_mib}
        if read_mib is not None and write_mib is not None:
            io_totals["mixed"] = read_mib + write_mib
            derived.append(Metric("fio", item, "mixed_io_mib", read_mib + write_mib, "MiB"))
        for prefix, io_mib in io_totals.items():
            if io_mib is None:
                continue
            if wall_s is not None and wall_s > 0:
                derived.append(
                    Metric(
                        "runtime",
                        item,
                        f"{prefix}_effective_wall_bw_mib_s",
                        io_mib / wall_s,
                        "MiB/s",
                    )
                )
            if active_s is not None and drain_s is not None and active_s + drain_s > 0:
                derived.append(
                    Metric(
                        "runtime",
                        item,
                        f"{prefix}_effective_active_plus_drain_bw_mib_s",
                        io_mib / (active_s + drain_s),
                        "MiB/s",
                    )
                )
    return metrics + derived


def load_maintenance_metrics(artifact_dir: pathlib.Path) -> list[Metric]:
    path = artifact_dir / "maintenance.tsv"
    if not path.exists():
        return []
    metrics = []
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle, delimiter="\t"):
            try:
                seconds = (int(row["ended_ns"]) - int(row["started_ns"])) / 1e9
                if seconds < int(row["requested_observation_s"]):
                    raise ValueError("short maintenance window")
                metrics.append(Metric("runtime", row["tool"], "maintenance_wall_s", seconds, "s"))
            except (KeyError, ValueError):
                metrics.append(Metric("gap", row.get("tool", "unknown"), "invalid_maintenance", "invalid boundaries", ""))
    return metrics


def load_object_ledger_metrics(artifact_dir: pathlib.Path) -> list[Metric]:
    from perf_lines_observe import object_ledger
    metrics = []
    for tool, evidence in object_ledger(artifact_dir)["tools"].items():
        if evidence["status"] != "measured":
            metrics.append(Metric("gap", tool, "object_ledger", evidence.get("reason", evidence["status"])))
            continue
        for window in ("active_close_drain", "maintenance", "between_drain_and_maintenance", "total"):
            for name, value in evidence[window].items():
                if value is not None:
                    metrics.append(Metric("object_ledger", tool, f"{window}_{name}", float(value)))
    return metrics


def load_artifact(artifact_dir: pathlib.Path) -> dict[tuple[str, str, str], Metric]:
    if not artifact_dir.exists():
        die(f"missing artifact directory: {artifact_dir}")
    if not artifact_dir.is_dir():
        die(f"artifact path is not a directory: {artifact_dir}")

    metrics: list[Metric] = []
    metrics.extend(load_fio_metrics(artifact_dir))
    metrics.extend(load_summary_metrics(artifact_dir))
    metrics.extend(load_drain_metrics(artifact_dir))
    metrics.extend(load_stats_metrics(artifact_dir))
    metrics.extend(load_maintenance_metrics(artifact_dir))
    metrics.extend(load_object_ledger_metrics(artifact_dir))

    if not metrics:
        die(
            f"artifact has no comparable perf data: {artifact_dir} "
            "(expected results/fio*.json, perf-summary.tsv, post-write-drain.tsv, "
            "or diagnostics/stats-*-after.txt)"
        )

    append_artifact_gap_metrics(metrics, artifact_dir)
    metrics = append_runtime_metrics(metrics)
    return {(metric.kind, metric.item, metric.name): metric for metric in metrics}


def compare_metrics(
    baseline: dict[tuple[str, str, str], Metric],
    candidate: dict[tuple[str, str, str], Metric],
) -> list[Row]:
    rows: list[Row] = []
    common_keys = set(baseline) & set(candidate)
    gap_keys = {key for key in set(baseline) | set(candidate) if key[0] == "gap"}
    for key in sorted(common_keys | gap_keys):
        if key[0] != "gap" and key not in common_keys:
            continue
        base = baseline.get(key, Metric(key[0], key[1], key[2], "not_reported", ""))
        cand = candidate.get(key, Metric(key[0], key[1], key[2], "not_reported", ""))
        if key[0] == "gap" and base.value == "none" and cand.value == "none":
            continue
        unit = cand.unit or base.unit
        rows.append(
            Row(
                kind=key[0],
                item=key[1],
                metric=key[2],
                baseline=base.value,
                candidate=cand.value,
                delta_pct=format_delta(base.value, cand.value),
                unit=unit,
            )
        )
    return rows


def emit_tsv(rows: Iterable[Row]) -> str:
    lines = ["kind\titem\tmetric\tbaseline\tcandidate\tdelta_pct\tunit"]
    for row in rows:
        lines.append(
            "\t".join(
                [
                    row.kind,
                    row.item,
                    row.metric,
                    format_value(row.baseline),
                    format_value(row.candidate),
                    row.delta_pct,
                    row.unit,
                ]
            )
        )
    return "\n".join(lines) + "\n"


def markdown_table(rows: list[Row]) -> list[str]:
    lines = [
        "| Item | Metric | Baseline | Candidate | Delta | Unit |",
        "| --- | --- | ---: | ---: | ---: | --- |",
    ]
    for row in rows:
        lines.append(
            f"| {row.item} | {row.metric} | {format_value(row.baseline)} | "
            f"{format_value(row.candidate)} | {row.delta_pct} | {row.unit} |"
        )
    return lines


def emit_markdown(
    rows: list[Row],
    baseline_label: str,
    candidate_label: str,
    baseline_path: pathlib.Path,
    candidate_path: pathlib.Path,
) -> str:
    lines = [
        "# BrewFS Perf Artifact Comparison",
        "",
        f"Baseline: `{baseline_label}`",
        f"Candidate: `{candidate_label}`",
        "",
        f"- Baseline artifact: `{baseline_path}`",
        f"- Candidate artifact: `{candidate_path}`",
    ]

    groups = [
        ("summary", "## Summary"),
        ("fio_config", "## Fio Config"),
        ("fio", "## Fio"),
        ("fio_group", "## Fio Groups (P99 Requires At Least 1000 I/Os)"),
        ("runtime", "## Runtime And Tail"),
        ("drain", "## Drain And Backpressure"),
        ("stats", "## BrewFS Stats"),
        ("amplification", "## Object And Upload Amplification"),
        ("request_observation", "## Request Observation"),
        ("request_observation_gap", "## Request Observation Gaps"),
        ("gap", "## Metric Gaps"),
    ]
    for kind, heading in groups:
        group_rows = [row for row in rows if row.kind == kind]
        if not group_rows:
            continue
        lines.extend(["", heading, ""])
        lines.extend(markdown_table(group_rows))
    return "\n".join(lines) + "\n"


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare BrewFS perf-run artifacts, including fio, post-write drain, and BrewFS stats."
    )
    parser.add_argument("baseline", type=pathlib.Path, help="Baseline/current perf-run artifact directory")
    parser.add_argument("candidate", type=pathlib.Path, help="Candidate perf-run artifact directory")
    parser.add_argument(
        "--format",
        choices=("markdown", "tsv"),
        default="markdown",
        help="Output format (default: markdown)",
    )
    parser.add_argument("--baseline-label", default=None, help="Label to show for the baseline artifact")
    parser.add_argument("--candidate-label", default=None, help="Label to show for the candidate artifact")
    parser.add_argument("-o", "--output", type=pathlib.Path, help="Write output to a file instead of stdout")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    baseline_path = args.baseline.resolve()
    candidate_path = args.candidate.resolve()

    baseline = load_artifact(baseline_path)
    candidate = load_artifact(candidate_path)
    rows = compare_metrics(baseline, candidate)
    if not rows:
        die(f"no comparable metrics found between {baseline_path} and {candidate_path}")

    baseline_label = args.baseline_label or baseline_path.name
    candidate_label = args.candidate_label or candidate_path.name
    if args.format == "tsv":
        output = emit_tsv(rows)
    else:
        output = emit_markdown(rows, baseline_label, candidate_label, baseline_path, candidate_path)

    if args.output:
        args.output.write_text(output)
    else:
        sys.stdout.write(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
