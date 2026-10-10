#!/usr/bin/env python3
"""Record and validate owned resources for a packed-v3 runner invocation.

The journal is deliberately a small, append-only event log.  It proves the
runner's resource domain and cleanup decisions without trusting a shell exit
status or an artifact directory that may have been left behind by a killed
process.
"""

from __future__ import annotations

import argparse
import datetime as _datetime
import json
import pathlib
import tempfile
from typing import Any


SCHEMA = "packed-v3-resource-journal-v1"
_EVENTS = {
    "init",
    "compose_started",
    "mount_ready",
    "mount_unmounted",
    "compose_stopped",
    "work_removed",
    "finalize",
}


class ResourceJournalError(ValueError):
    """Raised when a resource journal cannot prove ownership or cleanup."""


def _now() -> str:
    return _datetime.datetime.now(_datetime.timezone.utc).isoformat()


def _atomic_write(path: pathlib.Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    ) as stream:
        json.dump(value, stream, sort_keys=True, indent=2)
        stream.write("\n")
        temporary = pathlib.Path(stream.name)
    temporary.replace(path)


def _read(path: pathlib.Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise ResourceJournalError(f"invalid resource journal: {error}") from error
    if not isinstance(value, dict) or value.get("schema") != SCHEMA:
        raise ResourceJournalError("unsupported resource journal schema")
    events = value.get("events")
    if not isinstance(events, list) or not events:
        raise ResourceJournalError("resource journal has no events")
    return value


def _token(name: str, value: str) -> str:
    if not isinstance(value, str) or not value.strip() or any(c.isspace() for c in value):
        raise ResourceJournalError(f"{name} must be a non-empty token")
    return value


def init_journal(
    path: pathlib.Path,
    *,
    run_id: str,
    artifact: str,
    project: str,
    work: str,
) -> None:
    if path.exists():
        raise ResourceJournalError("resource journal already exists")
    for name, value in (
        ("run_id", run_id),
        ("artifact", artifact),
        ("project", project),
        ("work", work),
    ):
        _token(name, value)
    _atomic_write(
        path,
        {
            "schema": SCHEMA,
            "run_id": run_id,
            "owned": {"artifact": artifact, "compose_project": project, "work": work},
            "events": [{"name": "init", "at_utc": _now()}],
            "status": "running",
        },
    )


def append_event(path: pathlib.Path, name: str, *, details: dict[str, Any] | None = None) -> None:
    if name not in _EVENTS or name in {"init", "finalize"}:
        raise ResourceJournalError(f"event is not appendable: {name}")
    value = _read(path)
    if value.get("status") != "running":
        raise ResourceJournalError("cannot append after journal finalization")
    events = value["events"]
    assert isinstance(events, list)
    events.append({"name": name, "at_utc": _now(), **(details or {})})
    _atomic_write(path, value)


def _validate(
    value: dict[str, Any],
    *,
    expected_status: int | None = None,
    expected_run_id: str | None = None,
    expected_artifact: str | None = None,
) -> None:
    if value.get("schema") != SCHEMA:
        raise ResourceJournalError("unsupported resource journal schema")
    owned = value.get("owned")
    if not isinstance(owned, dict):
        raise ResourceJournalError("resource ownership is missing")
    for name in ("artifact", "compose_project", "work"):
        _token(f"owned.{name}", owned.get(name))
    if expected_run_id is not None and value.get("run_id") != expected_run_id:
        raise ResourceJournalError("resource journal run id does not match manifest")
    if expected_artifact is not None and owned.get("artifact") != expected_artifact:
        raise ResourceJournalError("resource journal artifact is outside manifest")
    events = value.get("events")
    if not isinstance(events, list) or not events:
        raise ResourceJournalError("resource journal has no events")
    names: list[str] = []
    for event in events:
        if not isinstance(event, dict) or event.get("name") not in _EVENTS:
            raise ResourceJournalError("resource journal contains an unknown event")
        if not isinstance(event.get("at_utc"), str) or not event["at_utc"].strip():
            raise ResourceJournalError("resource journal event has no timestamp")
        names.append(event["name"])
    if names[0] != "init" or names[-1] != "finalize":
        raise ResourceJournalError("resource journal is missing init/finalize boundary")
    if names.count("init") != 1 or names.count("finalize") != 1:
        raise ResourceJournalError("resource journal has duplicate terminal events")
    exit_status = value.get("exit_status")
    if not isinstance(exit_status, int) or isinstance(exit_status, bool):
        raise ResourceJournalError("resource journal has no integer exit status")
    if expected_status is not None and exit_status != expected_status:
        raise ResourceJournalError("resource journal exit status does not match runner")
    expected_label = "passed" if exit_status == 0 else "failed"
    if value.get("status") != expected_label:
        raise ResourceJournalError("resource journal status does not match exit status")
    cleanup = value.get("cleanup")
    if not isinstance(cleanup, dict):
        raise ResourceJournalError("resource journal cleanup result is missing")
    for name in ("mount_present", "process_alive", "compose_present", "work_present"):
        if not isinstance(cleanup.get(name), bool):
            raise ResourceJournalError(f"cleanup result {name} is not boolean")
    if exit_status == 0:
        required = {"compose_started", "mount_ready", "mount_unmounted", "compose_stopped", "work_removed"}
        missing = required.difference(names)
        if missing:
            raise ResourceJournalError(f"successful run is missing cleanup events: {sorted(missing)}")
        if any(cleanup[name] for name in ("mount_present", "process_alive", "compose_present", "work_present")):
            raise ResourceJournalError("successful run still owns a live resource")


def finalize_journal(
    path: pathlib.Path,
    *,
    status: int,
    mount_present: bool,
    process_alive: bool,
    compose_present: bool,
    work_present: bool,
) -> None:
    value = _read(path)
    if value.get("status") != "running":
        raise ResourceJournalError("resource journal is already finalized")
    value["events"].append({"name": "finalize", "at_utc": _now()})
    value["cleanup"] = {
        "mount_present": bool(mount_present),
        "process_alive": bool(process_alive),
        "compose_present": bool(compose_present),
        "work_present": bool(work_present),
    }
    value["exit_status"] = int(status)
    value["status"] = "passed" if status == 0 else "failed"
    value["finalized_at_utc"] = _now()
    _validate(value, expected_status=status)
    _atomic_write(path, value)


def validate_journal(
    path: pathlib.Path,
    *,
    expected_status: int | None = None,
    expected_run_id: str | None = None,
    expected_artifact: str | None = None,
) -> dict[str, Any]:
    value = _read(path)
    _validate(
        value,
        expected_status=expected_status,
        expected_run_id=expected_run_id,
        expected_artifact=expected_artifact,
    )
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    init = sub.add_parser("init")
    init.add_argument("--path", type=pathlib.Path, required=True)
    init.add_argument("--run-id", required=True)
    init.add_argument("--artifact", required=True)
    init.add_argument("--project", required=True)
    init.add_argument("--work", required=True)
    event = sub.add_parser("event")
    event.add_argument("--path", type=pathlib.Path, required=True)
    event.add_argument("--name", choices=sorted(_EVENTS - {"init", "finalize"}), required=True)
    finish = sub.add_parser("finalize")
    finish.add_argument("--path", type=pathlib.Path, required=True)
    finish.add_argument("--status", type=int, required=True)
    finish.add_argument("--mount-present", action="store_true")
    finish.add_argument("--process-alive", action="store_true")
    finish.add_argument("--compose-present", action="store_true")
    finish.add_argument("--work-present", action="store_true")
    args = parser.parse_args()
    try:
        if args.command == "init":
            init_journal(args.path, run_id=args.run_id, artifact=args.artifact, project=args.project, work=args.work)
        elif args.command == "event":
            append_event(args.path, args.name)
        else:
            finalize_journal(
                args.path,
                status=args.status,
                mount_present=args.mount_present,
                process_alive=args.process_alive,
                compose_present=args.compose_present,
                work_present=args.work_present,
            )
    except ResourceJournalError as error:
        print(f"resource journal validation failed: {error}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
