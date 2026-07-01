#!/usr/bin/env python3
"""
BrewFS Performance Analysis Tool

Generates a detailed performance profile report from fio JSON results,
brewfs logs, and optional perf profiling data.

Usage:
    # Analyze a single run (markdown)
    python3 analyze_perf.py /path/to/perf-run-XXXXX/

    # Compare two runs
    python3 analyze_perf.py --compare /path/to/baseline/ /path/to/current/

    # Analyze with bottleneck identification
    python3 analyze_perf.py --bottleneck /path/to/perf-run-XXXXX/

    # LLM-readable indented text
    python3 analyze_perf.py --llm /path/to/perf-run-XXXXX/
    python3 analyze_perf.py --llm --compare /path/to/baseline/ /path/to/current/ \\
        --hotspots /tmp/brewfs-perf/flame/oncpu-brewfs.folded
"""

import argparse
import json
import os
import pathlib
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional


@dataclass
class FioResult:
    """Parsed fio job result for one workload."""

    name: str
    rw: str
    bs: str
    numjobs: int
    error: int = 0
    # Read metrics
    read_bw_bytes: float = 0
    read_iops: float = 0
    read_lat_mean_ns: float = 0
    read_lat_percentiles: dict = field(default_factory=dict)
    read_total_ios: int = 0
    # Write metrics
    write_bw_bytes: float = 0
    write_iops: float = 0
    write_lat_mean_ns: float = 0
    write_lat_percentiles: dict = field(default_factory=dict)
    write_total_ios: int = 0
    # Submission latency (time from submission to actual dispatch)
    read_slat_mean_ns: float = 0
    write_slat_mean_ns: float = 0
    # Runtime
    runtime_ms: float = 0


def parse_fio_json(path: pathlib.Path) -> Optional[FioResult]:
    """Parse a single fio JSON output file."""
    try:
        text = path.read_text(errors="replace")
        start = text.find("{")
        if start < 0:
            return None
        data = json.loads(text[start:])
    except (json.JSONDecodeError, OSError):
        return None

    jobs = data.get("jobs", [])
    if not jobs:
        return None

    # Aggregate across all jobs (group_reporting combines them)
    job = jobs[0]
    options = job.get("job options", {})

    result = FioResult(
        name=path.stem,
        rw=options.get("rw", "unknown"),
        bs=options.get("bs", "unknown"),
        numjobs=int(options.get("numjobs", 1)),
        error=int(job.get("error", 0) or 0),
    )

    read_op = job.get("read", {})
    if read_op.get("bw_bytes", 0) > 0:
        result.read_bw_bytes = read_op["bw_bytes"]
        result.read_iops = read_op["iops"]
        result.read_lat_mean_ns = read_op.get("lat_ns", {}).get("mean", 0)
        result.read_lat_percentiles = read_op.get("clat_ns", {}).get("percentile", {})
        result.read_slat_mean_ns = read_op.get("slat_ns", {}).get("mean", 0)
        result.read_total_ios = int(read_op.get("total_ios", 0))
        result.runtime_ms = read_op.get("runtime", 0)

    write_op = job.get("write", {})
    if write_op.get("bw_bytes", 0) > 0:
        result.write_bw_bytes = write_op["bw_bytes"]
        result.write_iops = write_op["iops"]
        result.write_lat_mean_ns = write_op.get("lat_ns", {}).get("mean", 0)
        result.write_lat_percentiles = write_op.get("clat_ns", {}).get("percentile", {})
        result.write_slat_mean_ns = write_op.get("slat_ns", {}).get("mean", 0)
        result.write_total_ios = int(write_op.get("total_ios", 0))
        if result.runtime_ms == 0:
            result.runtime_ms = write_op.get("runtime", 0)

    return result


def fio_json_files(artifact_dir: pathlib.Path) -> list[pathlib.Path]:
    """Return fio JSON files from both historical and current result layouts."""
    candidates = []
    seen = set()
    for subdir in ("fio", "results"):
        path = artifact_dir / subdir
        if path.exists():
            for item in path.glob("fio*.json"):
                try:
                    key = item.resolve()
                except OSError:
                    key = item
                if key in seen:
                    continue
                seen.add(key)
                candidates.append(item)
    return sorted(candidates)


def fmt_bw(bw_bytes: float) -> str:
    """Format bandwidth in human-readable form."""
    if bw_bytes == 0:
        return "-"
    mib = bw_bytes / (1024 * 1024)
    if mib >= 1024:
        return f"{mib / 1024:.2f} GiB/s"
    return f"{mib:.1f} MiB/s"


def fmt_lat(ns: float) -> str:
    """Format latency from nanoseconds."""
    if ns == 0:
        return "-"
    ms = ns / 1_000_000
    if ms < 0.1:
        return f"{ns / 1000:.1f} µs"
    if ms < 1000:
        return f"{ms:.2f} ms"
    return f"{ms / 1000:.2f} s"


def fmt_iops(iops: float) -> str:
    if iops == 0:
        return "-"
    if iops >= 1000:
        return f"{iops / 1000:.1f}K"
    return f"{iops:.1f}"


PERCENTILE_KEYS = [
    "1.000000", "5.000000", "10.000000", "25.000000",
    "50.000000", "75.000000", "90.000000", "95.000000",
    "99.000000", "99.900000", "99.990000",
]

PERCENTILE_LABELS = [
    "p1", "p5", "p10", "p25", "p50", "p75", "p90", "p95", "p99", "p99.9", "p99.99"
]


def generate_latency_table(results: list[FioResult]) -> list[str]:
    """Generate detailed latency percentile table."""
    lines = [
        "",
        "## Latency Distribution",
        "",
        "### Read Latency Percentiles",
        "",
        "| Workload | p1 | p5 | p25 | p50 | p75 | p90 | p95 | p99 | p99.9 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]

    pct_keys = ["1.000000", "5.000000", "25.000000", "50.000000",
                "75.000000", "90.000000", "95.000000", "99.000000", "99.900000"]
    pct_labels = ["p1", "p5", "p25", "p50", "p75", "p90", "p95", "p99", "p99.9"]

    for r in results:
        if not r.read_lat_percentiles:
            continue
        cols = [r.name]
        for k in pct_keys:
            val = r.read_lat_percentiles.get(k, 0)
            cols.append(fmt_lat(val))
        lines.append("| " + " | ".join(cols) + " |")

    lines.extend([
        "",
        "### Write Latency Percentiles",
        "",
        "| Workload | p1 | p5 | p25 | p50 | p75 | p90 | p95 | p99 | p99.9 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ])

    for r in results:
        if not r.write_lat_percentiles:
            continue
        cols = [r.name]
        for k in pct_keys:
            val = r.write_lat_percentiles.get(k, 0)
            cols.append(fmt_lat(val))
        lines.append("| " + " | ".join(cols) + " |")

    return lines


def generate_bottleneck_analysis(results: list[FioResult]) -> list[str]:
    """Heuristic bottleneck identification based on latency patterns."""
    lines = [
        "",
        "## Bottleneck Analysis",
        "",
    ]

    for r in results:
        findings = []
        if r.error:
            findings.append(
                f"  - **fio error**: job failed with error={r.error}; "
                "throughput/latency values are incomplete and this run should not be used as a clean performance baseline."
            )

        # Analyze read path
        if r.read_lat_percentiles:
            p50 = r.read_lat_percentiles.get("50.000000", 0) / 1e6
            p99 = r.read_lat_percentiles.get("99.000000", 0) / 1e6
            p999 = r.read_lat_percentiles.get("99.900000", 0) / 1e6

            if p99 > p50 * 5:
                findings.append(
                    f"  - **Read tail latency**: p99/p50 = {p99/p50:.1f}x "
                    f"({fmt_lat(p50*1e6)} → {fmt_lat(p99*1e6)}). "
                    "Likely cause: S3 GET retry or cache miss on cold blocks."
                )
            if p999 > p99 * 3:
                findings.append(
                    f"  - **Read outliers**: p99.9/p99 = {p999/p99:.1f}x. "
                    "Possible GC pause, TCP retransmit, or lock contention."
                )
            if p50 > 50:  # >50ms median for reads
                findings.append(
                    f"  - **High read baseline**: p50={p50:.0f}ms. "
                    "Network RTT to S3 dominates. Consider prefetch tuning or local cache."
                )

        # Analyze write path
        if r.write_lat_percentiles:
            p50 = r.write_lat_percentiles.get("50.000000", 0) / 1e6
            p99 = r.write_lat_percentiles.get("99.000000", 0) / 1e6
            p999 = r.write_lat_percentiles.get("99.900000", 0) / 1e6

            if p50 < 10 and p99 > 100:
                findings.append(
                    f"  - **Write stall pattern**: p50={p50:.1f}ms, p99={p99:.0f}ms. "
                    "Most writes are buffered (fast), but auto_flush/freeze triggers "
                    "S3 upload that blocks subsequent writes (write buffer hard limit)."
                )
            if p99 > 500:
                findings.append(
                    f"  - **Write P99 > 500ms** ({p99:.0f}ms): "
                    "Consider increasing write buffer capacity or S3 upload concurrency."
                )

        # Throughput analysis
        if r.read_bw_bytes > 0 and r.numjobs > 1:
            per_job_bw = r.read_bw_bytes / r.numjobs / (1024 * 1024)
            if per_job_bw < 50:
                findings.append(
                    f"  - **Read scaling**: {per_job_bw:.0f} MiB/s/job "
                    f"({r.numjobs} jobs). May be limited by S3 connection pool or prefetch contention."
                )

        if r.write_bw_bytes > 0:
            # Check if write BW is suspiciously low relative to p50
            effective_bw = r.write_iops * int(r.bs.replace("m", "")) * 1024 * 1024
            if r.write_bw_bytes < effective_bw * 0.5 and r.numjobs > 1:
                findings.append(
                    f"  - **Write contention**: actual BW lower than expected from IOPS×BS. "
                    "Likely mutex contention or write buffer serialization."
                )

        if findings:
            lines.append(f"### {r.name} ({r.rw}, bs={r.bs}, jobs={r.numjobs})")
            lines.append("")
            lines.extend(findings)
            lines.append("")

    if len(lines) == 3:
        lines.append("No significant bottlenecks detected in latency distribution.")
        lines.append("")

    return lines


def generate_comparison(baseline: list[FioResult], current: list[FioResult]) -> list[str]:
    """Compare two runs and show deltas."""
    lines = [
        "",
        "## Comparison (Baseline → Current)",
        "",
        "| Workload | Metric | Baseline | Current | Delta |",
        "| --- | --- | ---: | ---: | ---: |",
    ]

    baseline_map = {r.name: r for r in baseline}

    for curr in current:
        base = baseline_map.get(curr.name)
        if not base:
            continue

        # Read BW
        if curr.read_bw_bytes > 0 and base.read_bw_bytes > 0:
            delta_pct = (curr.read_bw_bytes - base.read_bw_bytes) / base.read_bw_bytes * 100
            sign = "+" if delta_pct > 0 else ""
            lines.append(
                f"| {curr.name} | Read BW | {fmt_bw(base.read_bw_bytes)} | "
                f"{fmt_bw(curr.read_bw_bytes)} | {sign}{delta_pct:.1f}% |"
            )

        # Write BW
        if curr.write_bw_bytes > 0 and base.write_bw_bytes > 0:
            delta_pct = (curr.write_bw_bytes - base.write_bw_bytes) / base.write_bw_bytes * 100
            sign = "+" if delta_pct > 0 else ""
            lines.append(
                f"| {curr.name} | Write BW | {fmt_bw(base.write_bw_bytes)} | "
                f"{fmt_bw(curr.write_bw_bytes)} | {sign}{delta_pct:.1f}% |"
            )

        # Read P99
        if curr.read_lat_percentiles and base.read_lat_percentiles:
            c99 = curr.read_lat_percentiles.get("99.000000", 0)
            b99 = base.read_lat_percentiles.get("99.000000", 0)
            if b99 > 0:
                delta_pct = (c99 - b99) / b99 * 100
                sign = "+" if delta_pct > 0 else ""
                lines.append(
                    f"| {curr.name} | Read P99 | {fmt_lat(b99)} | "
                    f"{fmt_lat(c99)} | {sign}{delta_pct:.1f}% |"
                )

        # Write P99
        if curr.write_lat_percentiles and base.write_lat_percentiles:
            c99 = curr.write_lat_percentiles.get("99.000000", 0)
            b99 = base.write_lat_percentiles.get("99.000000", 0)
            if b99 > 0:
                delta_pct = (c99 - b99) / b99 * 100
                sign = "+" if delta_pct > 0 else ""
                lines.append(
                    f"| {curr.name} | Write P99 | {fmt_lat(b99)} | "
                    f"{fmt_lat(c99)} | {sign}{delta_pct:.1f}% |"
                )

    return lines


def generate_optimization_roadmap(results: list[FioResult]) -> list[str]:
    """Generate prioritized optimization suggestions based on results."""
    lines = [
        "",
        "## Optimization Roadmap",
        "",
    ]

    suggestions = []

    for r in results:
        if r.error:
            suggestions.append((
                "Fix Failed Benchmark Workload",
                f"{r.name} failed with fio error={r.error}. Fix the filesystem/tooling error before treating this run as a performance baseline.",
                0,
            ))

        # High write tail latency → buffer management
        if r.write_lat_percentiles:
            p99 = r.write_lat_percentiles.get("99.000000", 0) / 1e6
            if p99 > 300 and "write" in r.rw:
                suggestions.append((
                    "Write Buffer Management",
                    f"Write P99={p99:.0f}ms in {r.name}. Consider: "
                    "increase write_buffer_hard_limit, use adaptive auto_flush "
                    "based on upload throughput feedback, or implement S3 upload "
                    "pipelining to avoid blocking writes during uploads.",
                    1,
                ))

        # High read latency → prefetch/cache
        if r.read_lat_percentiles:
            p50 = r.read_lat_percentiles.get("50.000000", 0) / 1e6
            if p50 > 100 and "rand" in r.rw:
                suggestions.append((
                    "Random Read Prefetch",
                    f"Random read p50={p50:.0f}ms in {r.name}. Each 4MB block "
                    "requires a full S3 GET. Consider: smaller block size for "
                    "random workloads, read-ahead pattern detection, or tiered "
                    "block cache with SSD backing.",
                    2,
                ))
            elif p50 > 10 and "seq" in r.rw:
                suggestions.append((
                    "Sequential Read Pipeline",
                    f"Seq read p50={p50:.0f}ms in {r.name}. Consider: "
                    "aggressive prefetch (read-ahead window), coalescing "
                    "adjacent block fetches into single range GET, or "
                    "io_uring for concurrent S3 requests.",
                    3,
                ))

        # Multi-job scaling
        if r.numjobs > 1 and r.read_bw_bytes > 0:
            per_job = r.read_bw_bytes / r.numjobs / (1024 * 1024)
            if per_job < 40:
                suggestions.append((
                    "Parallel Read Scaling",
                    f"Only {per_job:.0f} MiB/s/job in {r.name} ({r.numjobs} jobs). "
                    "May be limited by: connection pool size, prefetch contention, "
                    "or per-inode lock granularity. Consider per-chunk parallelism.",
                    4,
                ))

    # Deduplicate
    seen = set()
    priority = 1
    for title, desc, _ in sorted(suggestions, key=lambda x: x[2]):
        if title in seen:
            continue
        seen.add(title)
        lines.append(f"### {priority}. {title}")
        lines.append("")
        lines.append(desc)
        lines.append("")
        priority += 1

    if not suggestions:
        lines.append("All workloads performing within expected parameters.")
        lines.append("")

    return lines


def generate_meta_perf_analysis(artifact_dir: pathlib.Path) -> list[str]:
    """Parse metaperf log for metadata operation analysis."""
    metaperf_log = artifact_dir / "tools" / "metaperf.log"
    if not metaperf_log.exists():
        return []

    lines = [
        "",
        "## Metadata Performance",
        "",
        "| Operation | Ops/sec | Latency (µs/op) |",
        "| --- | ---: | ---: |",
    ]

    for line in metaperf_log.read_text().splitlines():
        # Format: "create: 25 times, 200 file(s) ... ops/sec=176.21, usec/op=5675.17"
        if "ops/sec=" in line and "usec/op" in line:
            op = line.split(":")[0].strip()
            ops_sec = line.split("ops/sec=")[1].split(",")[0]
            usec_op = line.split("usec/op")[1].strip().lstrip("=").strip()
            lines.append(f"| {op} | {float(ops_sec):.1f} | {float(usec_op):.0f} |")

    return lines


def generate_report(artifact_dir: pathlib.Path, bottleneck: bool = False) -> str:
    """Generate comprehensive performance report."""
    results: list[FioResult] = []

    for json_file in fio_json_files(artifact_dir):
        r = parse_fio_json(json_file)
        if r:
            results.append(r)

    lines = [
        "# BrewFS Detailed Performance Profile",
        "",
        f"Artifact: `{artifact_dir.name}`",
        "",
    ]

    # Summary table
    lines.extend([
        "## Throughput Summary",
        "",
        "| Workload | Mode | BS | Jobs | Error | Read BW | Write BW | Read IOPS | Write IOPS |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ])
    for r in results:
        lines.append(
            f"| {r.name} | {r.rw} | {r.bs} | {r.numjobs} | "
            f"{r.error or '-'} | "
            f"{fmt_bw(r.read_bw_bytes)} | {fmt_bw(r.write_bw_bytes)} | "
            f"{fmt_iops(r.read_iops)} | {fmt_iops(r.write_iops)} |"
        )

    # Latency summary
    lines.extend([
        "",
        "## Latency Summary",
        "",
        "| Workload | Read Mean | Read P50 | Read P99 | Write Mean | Write P50 | Write P99 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ])
    for r in results:
        rp50 = fmt_lat(r.read_lat_percentiles.get("50.000000", 0))
        rp99 = fmt_lat(r.read_lat_percentiles.get("99.000000", 0))
        wp50 = fmt_lat(r.write_lat_percentiles.get("50.000000", 0))
        wp99 = fmt_lat(r.write_lat_percentiles.get("99.000000", 0))
        lines.append(
            f"| {r.name} | {fmt_lat(r.read_lat_mean_ns)} | {rp50} | {rp99} | "
            f"{fmt_lat(r.write_lat_mean_ns)} | {wp50} | {wp99} |"
        )

    # Detailed percentiles
    lines.extend(generate_latency_table(results))

    # Metadata perf
    lines.extend(generate_meta_perf_analysis(artifact_dir))

    # Bottleneck analysis
    if bottleneck:
        lines.extend(generate_bottleneck_analysis(results))
        lines.extend(generate_optimization_roadmap(results))

    return "\n".join(lines) + "\n"


def generate_comparison_report(
    baseline_dir: pathlib.Path, current_dir: pathlib.Path
) -> str:
    """Generate comparison report between two runs."""
    baseline_results = []
    current_results = []

    for json_file in fio_json_files(baseline_dir):
        r = parse_fio_json(json_file)
        if r:
            baseline_results.append(r)

    for json_file in fio_json_files(current_dir):
        r = parse_fio_json(json_file)
        if r:
            current_results.append(r)

    lines = [
        "# BrewFS Performance Comparison",
        "",
        f"Baseline: `{baseline_dir.name}`",
        f"Current:  `{current_dir.name}`",
        "",
    ]

    lines.extend(generate_comparison(baseline_results, current_results))

    # Also show current absolute numbers
    lines.extend([
        "",
        "## Current Run Details",
        "",
    ])
    for r in current_results:
        parts = [f"**{r.name}** ({r.rw}, bs={r.bs}, {r.numjobs}j):"]
        if r.read_bw_bytes > 0:
            parts.append(f"Read {fmt_bw(r.read_bw_bytes)}")
        if r.write_bw_bytes > 0:
            parts.append(f"Write {fmt_bw(r.write_bw_bytes)}")
        lines.append("- " + ", ".join(parts))

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# LLM-readable indented text output
# ---------------------------------------------------------------------------

def _parse_hotspots(folded_path: str) -> dict:
    """Parse a folded stack file into top functions + leaf functions."""
    func_counts: Counter[str] = Counter()
    leaf_counts: Counter[str] = Counter()
    with open(folded_path) as f:
        for line in f:
            parts = line.strip().rsplit(" ", 1)
            if len(parts) != 2:
                continue
            stack, count_str = parts
            try:
                count = int(count_str)
            except ValueError:
                continue
            frames = stack.split(";")
            for frame in frames:
                short = frame.rsplit("(", 1)[0].rsplit("+", 1)[0].strip()
                if short and not short.startswith("["):
                    func_counts[short] += count
            if frames:
                leaf = frames[-1].rsplit("(", 1)[0].rsplit("+", 1)[0].strip()
                if leaf and not leaf.startswith("["):
                    leaf_counts[leaf] += count
    return {
        "top": func_counts.most_common(15),
        "leaves": leaf_counts.most_common(10),
    }


def _crypto_pct(folded_path: str) -> float:
    """Return crypto overhead percentage from folded stacks."""
    total = 0
    crypto = 0
    crypto_kw = ("sha256", "md5::", "crc_fast", "hmac", "sha2::", "digest::")
    with open(folded_path) as f:
        for line in f:
            parts = line.strip().rsplit(" ", 1)
            if len(parts) != 2:
                continue
            stack, count_str = parts
            try:
                cnt = int(count_str)
            except ValueError:
                continue
            total += cnt
            if any(x in stack for x in crypto_kw):
                crypto += cnt
    return (crypto * 100 / total) if total > 0 else 0.0


def _llm_throughput(results: list) -> list[str]:
    """Throughput rows as indented text."""
    lines = ["Throughput:"]
    for r in results:
        parts = [f"  {r.name:<14} {r.rw:<10} bs={r.bs} jobs={r.numjobs}"]
        if getattr(r, "error", 0):
            parts.append(f"ERROR={r.error}")
        if r.read_bw_bytes > 0:
            parts.append(f"read {fmt_bw(r.read_bw_bytes):>10s}  {fmt_iops(r.read_iops):>6s} IOPS")
        if r.write_bw_bytes > 0:
            parts.append(f"write {fmt_bw(r.write_bw_bytes):>10s}  {fmt_iops(r.write_iops):>6s} IOPS")
        lines.append("  | ".join(parts))
    return lines


def _llm_latency(results: list) -> list[str]:
    """Latency summary as indented text, split read/write."""
    lines = []
    # Read
    read_rows = []
    for r in results:
        if not r.read_lat_percentiles:
            continue
        p = r.read_lat_percentiles
        read_rows.append(
            f"  {r.name:<14} mean={fmt_lat(r.read_lat_mean_ns):>8s}  "
            f"p50={fmt_lat(p.get('50.000000', 0)):>8s}  "
            f"p99={fmt_lat(p.get('99.000000', 0)):>8s}  "
            f"p99.9={fmt_lat(p.get('99.900000', 0)):>8s}"
        )
    if read_rows:
        lines.append("Latency (read):")
        lines.extend(read_rows)
    # Write
    write_rows = []
    for r in results:
        if not r.write_lat_percentiles:
            continue
        p = r.write_lat_percentiles
        write_rows.append(
            f"  {r.name:<14} mean={fmt_lat(r.write_lat_mean_ns):>8s}  "
            f"p50={fmt_lat(p.get('50.000000', 0)):>8s}  "
            f"p99={fmt_lat(p.get('99.000000', 0)):>8s}  "
            f"p99.9={fmt_lat(p.get('99.900000', 0)):>8s}"
        )
    if write_rows:
        lines.append("Latency (write):")
        lines.extend(write_rows)
    return lines


def _llm_bottlenecks(results: list) -> list[str]:
    """Heuristic bottlenecks as indented text with severity tags."""
    lines = ["Bottlenecks:"]
    found = 0

    for r in results:
        if getattr(r, "error", 0):
            found += 1
            lines.append(f"  [HIGH] {r.name}: fio job failed with error={r.error}; performance numbers are incomplete")

        if r.read_lat_percentiles:
            p50 = r.read_lat_percentiles.get("50.000000", 0) / 1e6
            p99 = r.read_lat_percentiles.get("99.000000", 0) / 1e6
            p999 = r.read_lat_percentiles.get("99.900000", 0) / 1e6

            if p50 > 100:
                found += 1
                lines.append(f"  [HIGH] {r.name}: read P50={p50:.0f}ms — each 4MB block = full S3 GET")
                lines.append(f"    → target: src/vfs/cache/prefetch.rs, src/chunk/store.rs")

            if p99 > p50 * 5:
                found += 1
                lines.append(f"  [MED]  {r.name}: read P99/P50={p99/p50:.1f}x tail — S3 GET retry or cold-block miss")
                lines.append(f"    → target: src/cadapter/s3.rs, src/vfs/cache/lru_cache.rs")

            if p999 > p99 * 3:
                found += 1
                lines.append(f"  [LOW]  {r.name}: read P99.9/P99={p999/p99:.1f}x — GC pause or TCP retransmit")

            if r.numjobs > 1 and r.read_bw_bytes > 0:
                per_job = r.read_bw_bytes / r.numjobs / (1024 * 1024)
                if per_job < 50:
                    found += 1
                    lines.append(f"  [MED]  {r.name}: read {per_job:.0f} MiB/s/job (×{r.numjobs} jobs) — scaling bottleneck")
                    lines.append(f"    → target: src/cadapter/s3.rs (connection pool), src/vfs/cache/prefetch.rs")

        if r.write_lat_percentiles:
            p50 = r.write_lat_percentiles.get("50.000000", 0) / 1e6
            p99 = r.write_lat_percentiles.get("99.000000", 0) / 1e6

            if p50 < 10 and p99 > 100:
                found += 1
                ratio = p99 / max(p50, 0.01)
                lines.append(f"  [HIGH] {r.name}: write P50={p50:.1f}ms P99={p99:.0f}ms ({ratio:.0f}x gap) — write buffer stall")
                lines.append(f"    → target: src/vfs/io/writer.rs (buffer hard limit, auto_flush)")

            if p99 > 500:
                found += 1
                lines.append(f"  [HIGH] {r.name}: write P99={p99:.0f}ms >500ms threshold")
                lines.append(f"    → increase write buffer capacity or S3 upload concurrency")
                lines.append(f"    → target: src/vfs/io/writer.rs, src/vfs/cache/mod.rs")

    if found == 0:
        lines.append("  (no significant bottlenecks detected)")
    return lines


def _llm_roadmap(results: list) -> list[str]:
    """Prioritized optimization items as indented text."""
    items: list[tuple[int, str, str, str, str]] = []

    for r in results:
        if getattr(r, "error", 0):
            items.append((0, "Fix Failed Benchmark Workload", "HIGH", "MED",
                          "src/vfs/io/writer.rs, tools/perf/run_perf.sh"))
        if r.write_lat_percentiles:
            p99 = r.write_lat_percentiles.get("99.000000", 0) / 1e6
            if p99 > 300:
                items.append((1, "Write Buffer Backpressure", "HIGH", "MED",
                              "src/vfs/io/writer.rs"))
        if r.read_lat_percentiles:
            p50 = r.read_lat_percentiles.get("50.000000", 0) / 1e6
            if p50 > 50 and "rand" in r.rw:
                items.append((2, "Random Read Latency", "HIGH", "HIGH",
                              "src/vfs/cache/prefetch.rs, src/chunk/store.rs"))
            elif p50 > 10 and "seq" in r.rw:
                items.append((3, "Sequential Read Pipeline", "MED", "MED",
                              "src/vfs/cache/prefetch.rs"))
        if r.numjobs > 1 and r.read_bw_bytes > 0:
            per_job = r.read_bw_bytes / r.numjobs / (1024 * 1024)
            if per_job < 40:
                items.append((4, "Parallel Read Scaling", "MED", "MED",
                              "src/cadapter/s3.rs"))

    seen = set()
    lines = ["Optimization Roadmap:"]
    priority = 1
    for _, title, impact, effort, files in sorted(items, key=lambda x: x[0]):
        if title in seen:
            continue
        seen.add(title)
        lines.append(f"  {priority}. {title} [impact={impact}, effort={effort}]")
        lines.append(f"     {files}")
        priority += 1

    if not seen:
        lines.append("  (all metrics within expected range)")
    return lines


def _llm_comparison(baseline: list, current: list) -> list[str]:
    """Side-by-side comparison as indented text."""
    base_map = {r.name: r for r in baseline}
    lines = ["Comparison (baseline → current):"]
    found = 0

    for c in current:
        b = base_map.get(c.name)
        if not b:
            continue
        # Read BW
        if c.read_bw_bytes > 0 and b.read_bw_bytes > 0:
            delta = (c.read_bw_bytes - b.read_bw_bytes) / b.read_bw_bytes * 100
            tag = "REGRESSION" if delta < -5 else ("GAIN" if delta > 5 else "")
            tag_str = f"  *** {tag}" if tag else ""
            lines.append(
                f"  {c.name:<14} read_bw:  {fmt_bw(b.read_bw_bytes):>10s} → {fmt_bw(c.read_bw_bytes):>10s} "
                f"({delta:+.1f}%){tag_str}"
            )
            found += 1
        # Write BW
        if c.write_bw_bytes > 0 and b.write_bw_bytes > 0:
            delta = (c.write_bw_bytes - b.write_bw_bytes) / b.write_bw_bytes * 100
            tag = "REGRESSION" if delta < -5 else ("GAIN" if delta > 5 else "")
            tag_str = f"  *** {tag}" if tag else ""
            lines.append(
                f"  {c.name:<14} write_bw: {fmt_bw(b.write_bw_bytes):>10s} → {fmt_bw(c.write_bw_bytes):>10s} "
                f"({delta:+.1f}%){tag_str}"
            )
            found += 1
        # Read P99
        if c.read_lat_percentiles and b.read_lat_percentiles:
            c99 = c.read_lat_percentiles.get("99.000000", 0)
            b99 = b.read_lat_percentiles.get("99.000000", 0)
            if b99 > 0:
                delta = (c99 - b99) / b99 * 100
                tag = "REGRESSION" if delta > 10 else ("GAIN" if delta < -10 else "")
                tag_str = f"  *** {tag}" if tag else ""
                lines.append(
                    f"  {c.name:<14} read_p99: {fmt_lat(b99):>8s} → {fmt_lat(c99):>8s} "
                    f"({delta:+.1f}%){tag_str}"
                )
                found += 1
        # Write P99
        if c.write_lat_percentiles and b.write_lat_percentiles:
            c99 = c.write_lat_percentiles.get("99.000000", 0)
            b99 = b.write_lat_percentiles.get("99.000000", 0)
            if b99 > 0:
                delta = (c99 - b99) / b99 * 100
                tag = "REGRESSION" if delta > 10 else ("GAIN" if delta < -10 else "")
                tag_str = f"  *** {tag}" if tag else ""
                lines.append(
                    f"  {c.name:<14} write_p99:{fmt_lat(b99):>8s} → {fmt_lat(c99):>8s} "
                    f"({delta:+.1f}%){tag_str}"
                )
                found += 1

    if found == 0:
        lines.append("  (no comparable workloads)")
    return lines


def generate_llm_text(
    artifact_dir: pathlib.Path,
    hotspots_path: Optional[str] = None,
    baseline_results: Optional[list] = None,
    current_results: Optional[list] = None,
) -> str:
    """Generate an LLM-readable indented text performance profile."""
    out: list[str] = []

    # Header
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    out.append(f"BrewFS Performance Profile: {artifact_dir.name}")
    out.append(f"Generated: {ts}")
    out.append("")

    # Use provided results or parse from artifact_dir
    if current_results:
        results = current_results
    else:
        results = []
        for f in fio_json_files(artifact_dir):
            r = parse_fio_json(f)
            if r:
                results.append(r)

    if not results:
        out.append("(no fio results found)")
        return "\n".join(out) + "\n"

    # Throughput
    out.extend(_llm_throughput(results))
    out.append("")

    # Latency
    out.extend(_llm_latency(results))
    out.append("")

    # Bottlenecks
    out.extend(_llm_bottlenecks(results))
    out.append("")

    # Hotspots (from flame graph folded data)
    if hotspots_path and pathlib.Path(hotspots_path).exists():
        try:
            hs = _parse_hotspots(hotspots_path)
            out.append(f"Hotspots (on-CPU top {len(hs['top'])}):")
            total_top = sum(c for _, c in hs["top"])
            for func, cnt in hs["top"]:
                pct = (cnt * 100 / total_top) if total_top > 0 else 0
                out.append(f"  {cnt:>10d}  {pct:5.1f}%  {func[:100]}")
            out.append("")
            if hs["leaves"]:
                out.append(f"Hotspots (leaf functions, top {len(hs['leaves'])}):")
                for func, cnt in hs["leaves"]:
                    out.append(f"  {cnt:>10d}  {func[:100]}")
                out.append("")

            crypto = _crypto_pct(hotspots_path)
            flag = " *** OVER 10% — disable SigV4 payload signing!" if crypto > 10 else ""
            out.append(f"Crypto overhead: {crypto:.1f}% of on-CPU samples{flag}")
            out.append("")
        except Exception:
            pass

    # Optimization roadmap
    out.extend(_llm_roadmap(results))
    out.append("")

    # Comparison
    if baseline_results:
        out.extend(_llm_comparison(baseline_results, results))
        out.append("")

    return "\n".join(out) + "\n"


def _load_results_from_dir(d: pathlib.Path) -> list:
    """Parse all fio JSON results from a directory."""
    results = []
    for f in fio_json_files(d):
        r = parse_fio_json(f)
        if r:
            results.append(r)
    return results


def main():
    parser = argparse.ArgumentParser(
        description="BrewFS performance analysis tool"
    )
    parser.add_argument(
        "artifact_dir",
        nargs="?",
        help="Path to perf-run artifact directory",
    )
    parser.add_argument(
        "--compare",
        nargs=2,
        metavar=("BASELINE", "CURRENT"),
        help="Compare two runs",
    )
    parser.add_argument(
        "--bottleneck",
        action="store_true",
        help="Include bottleneck identification and optimization roadmap",
    )
    parser.add_argument(
        "--llm",
        action="store_true",
        help="Output indented text format for LLM consumption",
    )
    parser.add_argument(
        "--hotspots",
        metavar="FOLDED",
        help="Path to on-CPU folded stack file (for --llm mode)",
    )
    parser.add_argument(
        "--output", "-o",
        help="Output file (default: stdout)",
    )
    args = parser.parse_args()

    if args.llm:
        # LLM-readable indented text mode
        if args.compare:
            baseline_results = _load_results_from_dir(pathlib.Path(args.compare[0]))
            current_results = _load_results_from_dir(pathlib.Path(args.compare[1]))
            report = generate_llm_text(
                pathlib.Path(args.compare[1]),
                hotspots_path=args.hotspots,
                baseline_results=baseline_results,
                current_results=current_results,
            )
        elif args.artifact_dir:
            report = generate_llm_text(
                pathlib.Path(args.artifact_dir),
                hotspots_path=args.hotspots,
            )
        else:
            parser.print_help()
            sys.exit(1)
    elif args.compare:
        report = generate_comparison_report(
            pathlib.Path(args.compare[0]),
            pathlib.Path(args.compare[1]),
        )
    elif args.artifact_dir:
        report = generate_report(
            pathlib.Path(args.artifact_dir),
            bottleneck=args.bottleneck,
        )
    else:
        parser.print_help()
        sys.exit(1)

    if args.output:
        pathlib.Path(args.output).write_text(report)
        print(f"Report written to {args.output}", file=sys.stderr)
    else:
        print(report)


if __name__ == "__main__":
    main()
