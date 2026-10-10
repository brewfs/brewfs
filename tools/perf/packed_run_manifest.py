#!/usr/bin/env python3
"""Create and validate a fail-closed manifest for bounded packed-v3 runs."""

from __future__ import annotations

import argparse
import datetime as _datetime
import hashlib
import json
import math
import pathlib
import re
from typing import Any

SCHEMA = "packed-v3-run-manifest-v1"
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_HEX40 = re.compile(r"^[0-9a-f]{40}$")


class ArtifactError(ValueError):
    """Raised when an artifact is not complete enough to consume."""


def _atomic_json(path: pathlib.Path, value: dict[str, Any]) -> None:
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")
    temporary.replace(path)


def _read_json(path: pathlib.Path) -> Any:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise ArtifactError(f"invalid JSON artifact {path.name}: {error}") from error


def _require_file(artifact: pathlib.Path, name: str, *, success: bool) -> pathlib.Path | None:
    path = artifact / name
    if path.is_file() and path.stat().st_size:
        return path
    if success:
        raise ArtifactError(f"missing required artifact file: {name}")
    return None


def _digest(path: pathlib.Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


def _validate_digest_file(path: pathlib.Path) -> int:
    count = 0
    for line in path.read_text().splitlines():
        fields = line.split()
        if not fields:
            continue
        if not _HEX64.fullmatch(fields[0].lower()):
            raise ArtifactError(f"invalid digest in {path.name}")
        count += 1
    if not count:
        raise ArtifactError(f"empty digest inventory: {path.name}")
    return count


def _validate_toolchain(path: pathlib.Path) -> None:
    """Validate the immutable build provenance captured by the runner."""
    value = _read_json(path)
    if not isinstance(value, dict):
        raise ArtifactError("toolchain provenance is not an object")
    for name in ("rustc_verbose", "cargo_version", "host", "binary_profile"):
        field = value.get(name)
        if not isinstance(field, str) or not field.strip():
            raise ArtifactError(f"toolchain provenance is missing {name}")
    revision = value.get("revision")
    if not isinstance(revision, str) or not _HEX40.fullmatch(revision.lower()):
        raise ArtifactError("toolchain provenance has an invalid revision")
    dirty_diff = value.get("dirty_diff_sha256")
    if not isinstance(dirty_diff, str) or not _HEX64.fullmatch(dirty_diff.lower()):
        raise ArtifactError("toolchain provenance has an invalid dirty diff digest")


def _validate_resource_journal(
    path: pathlib.Path,
    *,
    status: int,
    run_id: str,
    artifact: pathlib.Path,
) -> None:
    """Require an owned resource domain and an explicit cleanup terminal state."""
    try:
        from .packed_resource_journal import ResourceJournalError, validate_journal
    except ImportError:  # direct execution from tools/perf
        from packed_resource_journal import ResourceJournalError, validate_journal
    try:
        validate_journal(
            path,
            expected_status=status,
            expected_run_id=run_id,
            expected_artifact=str(artifact),
        )
    except (OSError, ResourceJournalError) as error:
        raise ArtifactError(f"invalid resource journal: {error}") from error


def _read_profile(path: pathlib.Path) -> dict[str, str]:
    """Read the runner's key/value profile without executing it as shell."""
    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(path.read_text().splitlines(), 1):
        line = raw_line.strip()
        if not line:
            continue
        name, separator, value = line.partition("=")
        if not separator or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ArtifactError(f"malformed profile.env line {line_number}")
        if name in values:
            raise ArtifactError(f"duplicate profile.env key: {name}")
        values[name] = value
    if not values:
        raise ArtifactError("profile.env is empty")
    return values


def _validate_profile(manifest: dict[str, Any], path: pathlib.Path) -> None:
    """Bind invocation controls to the profile captured by the runner."""
    profile = _read_profile(path)
    controls = manifest.get("controls")
    if not isinstance(controls, dict):
        raise ArtifactError("manifest controls are missing")
    expected = {
        "packed_version": "v3",
        "wire_version": manifest.get("wire_version"),
        "scanner_seed": controls.get("scanner_seed"),
        "fixture_prefix": manifest.get("fixture_prefix"),
        "manifest_schema": SCHEMA,
        # The scanner contract is part of the request-trace identity.
        "order": "shuffle",
    }
    control_bindings = {
        "files": "files",
        "file_bytes": "file_bytes",
        "metadata_bytes": "metadata_bytes",
        "workers": "workers",
        "epochs": "epochs",
        "mode": "mode",
        "frame_policy": "frame_policy",
        "inline_data": "inline_data",
        "metadata_codec": "metadata_codec",
        "data_codec": "data_codec",
        "access_profile": "access_profile",
        "p90_training_trace_sha256": "p90_training_trace_sha256",
    }
    for profile_name, control_name in control_bindings.items():
        if control_name in controls and controls[control_name] is not None:
            expected[profile_name] = controls[control_name]
    for name, value in expected.items():
        if value is None or profile.get(name) != str(value):
            raise ArtifactError(f"profile.env does not bind {name} to the manifest")


def init_manifest(
    artifact: pathlib.Path,
    *,
    run_id: str,
    wire_version: int,
    controls: dict[str, Any],
    fixture_prefix: str,
) -> None:
    if not run_id or any(character.isspace() for character in run_id):
        raise ArtifactError("run_id must be a non-empty token")
    if wire_version != 5:
        raise ArtifactError("packed-v3 runs require the current encoding 005")
    if not fixture_prefix or fixture_prefix.startswith("/") or ".." in fixture_prefix.split("/"):
        raise ArtifactError("fixture prefix is not owned")
    seed = controls.get("scanner_seed")
    if not isinstance(seed, int) or seed < 0:
        raise ArtifactError("scanner_seed must be a non-negative integer")
    _validate_layout_controls(controls)
    artifact.mkdir(parents=True, exist_ok=True)
    _atomic_json(
        artifact / "run-manifest.json",
        {
            "schema": SCHEMA,
            "run_id": run_id,
            "created_at_utc": _datetime.datetime.now(_datetime.timezone.utc).isoformat(),
            "packed_version": "v3",
            "wire_version": wire_version,
            "fixture_prefix": fixture_prefix,
            "controls": controls,
            "status": "running",
        },
    )


def _validate_layout_controls(controls: dict[str, Any]) -> None:
    allowed = {
        "frame_policy": {"size-only", "static-256kib", "static-1mib", "static-4mib", "p90-training"},
        "inline_data": {"on", "off"},
        "metadata_codec": {"raw", "zstd"},
        "data_codec": {"raw", "zstd"},
        "access_profile": {"random-small-file", "sequential-small-file", "mixed"},
    }
    for name, values in allowed.items():
        value = controls.get(name)
        if value is not None and value not in values:
            raise ArtifactError(f"invalid {name} control")
    if controls.get("frame_policy") == "p90-training":
        trace = controls.get("p90_training_trace_sha256")
        if not isinstance(trace, str) or not _HEX64.fullmatch(trace.lower()):
            raise ArtifactError("p90-training requires a training trace SHA-256")


def _summary_rows(path: pathlib.Path) -> list[dict[str, Any]]:
    payload = _read_json(path)
    rows = payload.get("epochs") if isinstance(payload, dict) else None
    if rows is None:
        rows = [payload]
    if not isinstance(rows, list) or not rows or not all(isinstance(row, dict) for row in rows):
        raise ArtifactError("summary.json has no usable epoch rows")
    return rows


def _validate_summary(manifest: dict[str, Any], summary_path: pathlib.Path) -> dict[str, Any]:
    controls = manifest.get("controls")
    if not isinstance(controls, dict):
        raise ArtifactError("manifest controls are missing")
    seed = controls.get("scanner_seed")
    mode = controls.get("mode")
    _validate_layout_controls(controls)
    rows = _summary_rows(summary_path)
    traces: list[str] = []
    for row in rows:
        if row.get("order") != "shuffle":
            raise ArtifactError("summary does not record the required shuffle order")
        if row.get("shuffle_seed") != seed:
            raise ArtifactError("summary shuffle seed does not match manifest")
        if row.get("mode") != mode:
            raise ArtifactError("summary mode does not match manifest")
        if row.get("errors") != 0 or row.get("files") != row.get("expected_files"):
            raise ArtifactError("summary contains errors or an incomplete file count")
        trace = row.get("trace_sha256")
        if not isinstance(trace, str) or not _HEX64.fullmatch(trace):
            raise ArtifactError("summary is missing request trace digest")
        traces.append(trace)
    return {
        "request_trace_sha256": traces[0],
        "request_trace_sha256_all_epochs": traces,
        "epochs": len(rows),
        "phases": ["mount", "active", "drain", "total"],
    }


def _validate_timing(path: pathlib.Path) -> dict[str, float]:
    value = _read_json(path)
    if not isinstance(value, dict):
        raise ArtifactError("timing.json is not an object")
    phases = ("mount_seconds", "active_seconds", "drain_seconds", "total_seconds")
    result: dict[str, float] = {}
    for phase in phases:
        number = value.get(phase)
        if not isinstance(number, (int, float)) or isinstance(number, bool) or not math.isfinite(number) or number < 0:
            raise ArtifactError(f"timing is missing a finite {phase}")
        result[phase] = float(number)
    return result


def finalize_manifest(artifact: pathlib.Path, *, status: int) -> dict[str, Any]:
    path = artifact / "run-manifest.json"
    if not path.is_file():
        raise ArtifactError("run-manifest.json is missing")
    manifest = _read_json(path)
    if not isinstance(manifest, dict) or manifest.get("schema") != SCHEMA:
        raise ArtifactError("unsupported run manifest schema")
    if manifest.get("packed_version") != "v3" or manifest.get("wire_version") != 5:
        raise ArtifactError("run manifest does not identify the current packed-v3 encoding")
    success = status == 0
    required = [
        "manifest-key.txt",
        "binary-sha256.txt",
        "source-sha256.json",
        "profile.env",
        "cache-proof.env",
        "toolchain.json",
        # A failed process must leave an auditable cleanup decision rather than
        # merely an exit code.
        "resource-journal.json",
    ]
    files: dict[str, Any] = {}
    for name in required:
        required_path = _require_file(artifact, name, success=success)
        if required_path is not None:
            files[name] = {"bytes": required_path.stat().st_size, "sha256": _digest(required_path)}
    # A journal is required even for failed runs: the failure artifact must
    # prove whether its owned mount/process/Compose/work resources survived.
    journal_path = _require_file(artifact, "resource-journal.json", success=True)
    assert journal_path is not None
    _validate_resource_journal(
        journal_path,
        status=status,
        run_id=str(manifest.get("run_id", "")),
        artifact=artifact,
    )
    files[journal_path.name] = {
        "bytes": journal_path.stat().st_size,
        "sha256": _digest(journal_path),
    }
    if success:
        key_path = artifact / "manifest-key.txt"
        key = key_path.read_text().strip()
        prefix = str(manifest.get("fixture_prefix", "")).rstrip("/")
        if not key or not prefix or not key.startswith(prefix + "/"):
            raise ArtifactError("fixture manifest key is outside the owned prefix")
        binary_path = artifact / "binary-sha256.txt"
        _validate_digest_file(binary_path)
        source = _read_json(artifact / "source-sha256.json")
        if not isinstance(source, dict) or not source or any(not isinstance(value, str) or not _HEX64.fullmatch(value) for value in source.values()):
            raise ArtifactError("source hash inventory is missing or malformed")
        _validate_toolchain(artifact / "toolchain.json")
        _validate_profile(manifest, artifact / "profile.env")
        controls = manifest.get("controls")
        if isinstance(controls, dict) and controls.get("frame_policy") == "p90-training":
            policy_path = _require_file(artifact, "p90-policy.json", success=True)
            assert policy_path is not None
            try:
                from .packed_p90_policy import P90PolicyError, validate_policy
            except ImportError:  # direct execution from tools/perf
                from packed_p90_policy import P90PolicyError, validate_policy
            try:
                policy = validate_policy(
                    json.loads(policy_path.read_text()),
                    expected_trace_sha256=controls["p90_training_trace_sha256"],
                )
            except (OSError, json.JSONDecodeError, P90PolicyError) as error:
                raise ArtifactError(f"invalid p90 policy: {error}") from error
            files[policy_path.name] = {"bytes": policy_path.stat().st_size, "sha256": _digest(policy_path)}
            manifest["p90_policy"] = {
                "p90_bytes": policy["p90_bytes"],
                "trace_sha256": policy["trace_sha256"],
                "policy_sha256": policy["policy_sha256"],
            }
        summary_path = _require_file(artifact, "summary.json", success=True)
        timing_path = _require_file(artifact, "timing.json", success=True)
        assert summary_path is not None and timing_path is not None
        measurement = _validate_summary(manifest, summary_path)
        measurement.update(_validate_timing(timing_path))
        files.update({name: {"bytes": (artifact / name).stat().st_size, "sha256": _digest(artifact / name)} for name in ("summary.json", "timing.json")})
        provenance_path = artifact / "manifest-key.source.json"
        if provenance_path.is_file() and provenance_path.stat().st_size:
            files[provenance_path.name] = {"bytes": provenance_path.stat().st_size, "sha256": _digest(provenance_path)}
        manifest["fixture_manifest_key"] = key
        manifest["measurement"] = measurement
    manifest["files"] = files
    manifest["status"] = "passed" if success else "failed"
    manifest["exit_status"] = int(status)
    manifest["finalized_at_utc"] = _datetime.datetime.now(_datetime.timezone.utc).isoformat()
    _atomic_json(path, manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    init = subparsers.add_parser("init")
    init.add_argument("--artifact", type=pathlib.Path, required=True)
    init.add_argument("--run-id", required=True)
    init.add_argument("--wire-version", type=int, required=True)
    init.add_argument("--files", type=int, required=True)
    init.add_argument("--file-bytes", type=int, required=True)
    init.add_argument("--metadata-bytes", type=int, required=True)
    init.add_argument("--workers", type=int, required=True)
    init.add_argument("--epochs", type=int, required=True)
    init.add_argument("--mode", required=True)
    init.add_argument("--scanner-seed", type=int, required=True)
    init.add_argument("--fixture-prefix", required=True)
    init.add_argument("--frame-policy", choices=["size-only", "static-256kib", "static-1mib", "static-4mib", "p90-training"], default="size-only")
    init.add_argument("--inline-data", choices=["on", "off"], default="on")
    init.add_argument("--metadata-codec", choices=["raw", "zstd"], default="zstd")
    init.add_argument("--data-codec", choices=["raw", "zstd"], default="zstd")
    init.add_argument("--access-profile", choices=["random-small-file", "sequential-small-file", "mixed"], default="random-small-file")
    init.add_argument("--p90-training-trace-sha256", default=None)
    finish = subparsers.add_parser("finalize")
    finish.add_argument("--artifact", type=pathlib.Path, required=True)
    finish.add_argument("--status", type=int, required=True)
    args = parser.parse_args()
    try:
        if args.command == "init":
            init_manifest(
                args.artifact,
                run_id=args.run_id,
                wire_version=args.wire_version,
                controls={
                    "files": args.files,
                    "file_bytes": args.file_bytes,
                    "metadata_bytes": args.metadata_bytes,
                    "workers": args.workers,
                    "epochs": args.epochs,
                    "mode": args.mode,
                    "order": "shuffle",
                    "scanner_seed": args.scanner_seed,
                    "frame_policy": args.frame_policy,
                    "inline_data": args.inline_data,
                    "metadata_codec": args.metadata_codec,
                    "data_codec": args.data_codec,
                    "access_profile": args.access_profile,
                    "p90_training_trace_sha256": args.p90_training_trace_sha256,
                },
                fixture_prefix=args.fixture_prefix,
            )
        else:
            finalize_manifest(args.artifact, status=args.status)
    except ArtifactError as error:
        print(f"artifact validation failed: {error}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
