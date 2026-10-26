#!/usr/bin/env python3
"""Generate a provenance-aware matrix from an observation TSV and campaign JSON."""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


class MatrixError(ValueError):
    """Raised when the campaign or observation ledger is incomplete or invalid."""


IDENTITY_FIELDS = ("preset", "direct", "tool", "metric")
COLUMNS = ("native", "native-overlay", "juicefs")
LEDGER_FIELDS = (*IDENTITY_FIELDS[:1], "direct", *IDENTITY_FIELDS[2:], "column", "repetition", "value")
REPORT_CELL_COUNT = re.compile(r"\(n=\d+/\d+")


def _identity(row: dict[str, Any]) -> tuple[str, str, str, str]:
    return tuple(str(row[field]) for field in IDENTITY_FIELDS)  # type: ignore[return-value]


def load_campaign(path: Path) -> dict[str, Any]:
    try:
        campaign = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise MatrixError(f"cannot read campaign manifest {path}: {error}") from error
    if not isinstance(campaign, dict) or campaign.get("schema") != "observation-matrix-campaign-v1":
        raise MatrixError("campaign manifest has an unsupported schema")

    required = campaign.get("required_observations")
    if isinstance(required, bool) or not isinstance(required, int) or required < 1:
        raise MatrixError("required_observations must be a positive integer")
    if not isinstance(campaign.get("name"), str) or not campaign["name"].strip():
        raise MatrixError("campaign name is missing")
    if not isinstance(campaign.get("cache_policy"), str) or not campaign["cache_policy"].strip():
        raise MatrixError("cache_policy is missing")

    builds = campaign.get("builds")
    if not isinstance(builds, dict) or any(
        not isinstance(builds.get(name), str) or not builds[name].strip()
        for name in ("brewfs", "juicefs")
    ):
        raise MatrixError("builds must identify BrewFS and JuiceFS")

    rows = campaign.get("rows")
    if not isinstance(rows, list) or not rows:
        raise MatrixError("campaign rows are missing")
    identities: set[tuple[str, str, str, str]] = set()
    for row in rows:
        if not isinstance(row, dict) or any(not isinstance(row.get(field), str) for field in IDENTITY_FIELDS):
            raise MatrixError("each row needs string preset, direct, tool and metric fields")
        if row.get("direction") not in ("rate", "elapsed"):
            raise MatrixError(f"row {_identity(row)} has an invalid direction")
        identity = _identity(row)
        if identity in identities:
            raise MatrixError(f"duplicate row identity: {identity}")
        identities.add(identity)
    return campaign


def load_observations(
    path: Path, campaign: dict[str, Any]
) -> dict[tuple[tuple[str, str, str, str], str], list[float]]:
    known_rows = {_identity(row) for row in campaign["rows"]}
    observations: dict[tuple[tuple[str, str, str, str], str], list[float]] = defaultdict(list)
    seen_repetitions: set[tuple[tuple[str, str, str, str], str, int]] = set()
    try:
        source = path.open(newline="", encoding="utf-8")
    except OSError as error:
        raise MatrixError(f"cannot open observation ledger {path}: {error}") from error

    with source:
        reader = csv.DictReader(source, delimiter="\t")
        if reader.fieldnames is None or any(field not in reader.fieldnames for field in LEDGER_FIELDS):
            raise MatrixError(f"ledger must contain columns: {', '.join(LEDGER_FIELDS)}")
        for line_number, record in enumerate(reader, start=2):
            identity = tuple(record[field] for field in IDENTITY_FIELDS)
            if identity not in known_rows:
                raise MatrixError(f"ledger line {line_number} has an undeclared row: {identity}")
            column = record["column"]
            if column not in COLUMNS:
                raise MatrixError(f"ledger line {line_number} has an unknown column: {column}")
            try:
                repetition = int(record["repetition"])
                value = float(record["value"])
            except (TypeError, ValueError) as error:
                raise MatrixError(f"ledger line {line_number} has an invalid repetition or value") from error
            if repetition < 1 or not math.isfinite(value):
                raise MatrixError(f"ledger line {line_number} has an invalid repetition or non-finite value")
            key = (identity, column, repetition)
            if key in seen_repetitions:
                raise MatrixError(f"ledger line {line_number} duplicates repetition {repetition} for {identity}/{column}")
            seen_repetitions.add(key)
            observations[(identity, column)].append(value)
    return observations


def _measurement(values: list[float], required: int) -> str:
    count = len(values)
    if count < required:
        return f"not-measured (n={count}/{required})"
    return f"{statistics.median(values):.6g} (n={count}/{required})"


def _speedup(
    filesystem_values: list[float], juicefs_values: list[float], required: int, direction: str
) -> str:
    filesystem_n = len(filesystem_values)
    juicefs_n = len(juicefs_values)
    counts = f"n={filesystem_n}/{required}, n={juicefs_n}/{required}"
    if filesystem_n < required or juicefs_n < required:
        return f"not-measured ({counts})"

    filesystem = statistics.median(filesystem_values)
    juicefs = statistics.median(juicefs_values)
    numerator, denominator = (filesystem, juicefs) if direction == "rate" else (juicefs, filesystem)
    if denominator == 0:
        return f"not-comparable ({counts})"
    return f"{numerator / denominator:.6g}x ({counts})"


def render_table(
    campaign: dict[str, Any],
    observations: dict[tuple[tuple[str, str, str, str], str], list[float]],
) -> str:
    required = campaign["required_observations"]
    builds = campaign["builds"]
    output = [
        f"# Aggregated performance matrix: {campaign['name']}",
        "",
        f"A numeric measurement is the median of its observed values and is authoritative only at n >= {required}.",
        "Every measurement and comparison cell prints its observation count and required count; insufficient cells are `not-measured` and excluded from comparisons and rankings.",
        "Rate means higher is better; elapsed means lower is better. Direction-adjusted speedup is filesystem/JuiceFS for rates and JuiceFS/filesystem for elapsed time, so values above 1x mean faster.",
        f"BrewFS build (native and native-overlay): {builds['brewfs']}",
        f"JuiceFS build: {builds['juicefs']}",
        f"Cache policy C: {campaign['cache_policy']}",
        "",
        "| preset | direct | tool | metric | direction | BrewFS build (native + overlay) | JuiceFS build | cache policy | native | native-overlay | juicefs | native speedup vs JFS | overlay speedup vs JFS |",
        "|---|---|---|---|---|---|---|---|---:|---:|---:|---:|---:|",
    ]

    for row in sorted(campaign["rows"], key=_identity):
        identity = _identity(row)
        direction = row["direction"]
        direction_label = "rate: higher is better" if direction == "rate" else "elapsed: lower is better"
        values = {column: observations.get((identity, column), []) for column in COLUMNS}
        measurements = [_measurement(values[column], required) for column in COLUMNS]
        ratios = [
            _speedup(values[column], values["juicefs"], required, direction)
            for column in ("native", "native-overlay")
        ]
        fields = [
            *identity,
            direction_label,
            builds["brewfs"],
            builds["juicefs"],
            "C (warm cache possible; no eviction)",
            *measurements,
            *ratios,
        ]
        output.append("| " + " | ".join(fields) + " |")

    report = "\n".join(output) + "\n"
    _validate_report_counts(report, len(campaign["rows"]))
    return report


def _validate_report_counts(report: str, expected_rows: int) -> None:
    table_lines = [
        line
        for line in report.splitlines()
        if line.startswith("| ") and not line.startswith("| preset |") and not line.startswith("|---")
    ]
    if len(table_lines) != expected_rows:
        raise MatrixError(f"rendered {len(table_lines)} rows, expected {expected_rows}")
    for line_number, line in enumerate(table_lines, start=1):
        cells = line[1:-1].split("|")
        if len(cells) != 13:
            raise MatrixError(f"rendered row {line_number} has an unexpected number of cells")
        for cell in cells[8:]:
            if REPORT_CELL_COUNT.search(cell) is None:
                raise MatrixError(f"rendered row {line_number} contains a measurement without its count")


def generate(ledger: Path, manifest: Path) -> str:
    campaign = load_campaign(manifest)
    observations = load_observations(ledger, campaign)
    return render_table(campaign, observations)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", required=True, type=Path, help="observed-metrics.tsv")
    parser.add_argument("--campaign", required=True, type=Path, help="campaign metadata and row catalog JSON")
    parser.add_argument("--output", type=Path, help="output Markdown file; omit to write to stdout")
    args = parser.parse_args(argv)
    try:
        report = generate(args.ledger, args.campaign)
        if args.output:
            args.output.write_text(report, encoding="utf-8")
        else:
            sys.stdout.write(report)
    except (MatrixError, OSError) as error:
        print(f"aggregate_observation_matrix: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
