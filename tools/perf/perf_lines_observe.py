#!/usr/bin/env python3
"""Validate and summarize request-observation perf artifacts.

The compose runner deliberately keeps detailed request tracing disabled for
throughput measurements.  This helper therefore distinguishes four states:
``valid`` (the trace was enabled and met the sampling gates), ``disabled``
(the artifact is a legitimate observation-off run), and ``unavailable`` (the
artifact is missing fields), and ``invalid`` (a measured gate failed). Missing
fields are never converted to zero.

The input is an artifact directory produced by the xfstests runner.  The
summary is dependency-free JSON by default so it can be archived next to the
raw fio JSON and stats snapshots.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import pathlib
import re
import sys
import tempfile
import time
import hashlib
import subprocess
import shutil
from collections import Counter
from contextlib import nullcontext
from decimal import Decimal
from concurrent.futures import ProcessPoolExecutor
from typing import Any


SEGMENTS = (
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
    "handler_poll",
)
OPERATIONS = ("read", "write", "close", "fsync", "metadata")
REQUIRED = (
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
    "brewfs_request_trace_records_dropped_total",
    "brewfs_request_fuse_queue_depth",
    "brewfs_request_fuse_queue_oldest_age_ns",
    "brewfs_memory_cas_retries_total",
    "brewfs_memory_allocation_rejections_total",
)
QUEUE_SEGMENTS = (
    "file_lock_wait",
    "chunk_lock_wait",
    "flush_wait",
    "metadata_rpc_wait",
    "object_put_queue_wait",
)

WAIT_SEGMENTS = tuple(name for name in SEGMENTS if name not in ("handler", "handler_poll", "dirty_merge"))

# One shared measurement envelope. Values are strings because this is the
# actual environment passed to compose, not an approximation of YAML defaults.
PROTOCOL_ENV = dict(line.split("=", 1) for line in """
PERF_LOG_TO_CONSOLE=false
PERF_LINES_PROTOCOL=ws0
PERF_FIO_OUTPUT_FORMAT=json+
PERF_FIO_SYSCALL_TAIL=true
PERF_MAINTENANCE_OBSERVE_SECS=60
PERF_FIO_DIRECT=0
PERF_FIO_IOENGINE=io_uring
PERF_FIO_IODEPTH=1
PERF_FIO_BS=4m
PERF_FIO_SIZE=512m
PERF_FIO_RUNTIME=20
PERF_FIO_NUMJOBS=4
PERF_FIO_RWMIXREAD=70
BREWFS_FUSE_DIRECT_IO=0
BREWFS_FUSE_READ_DIRECT_IO=0
BREWFS_FUSE_WRITE_DIRECT_IO=0
BREWFS_FUSE_KEEP_CACHE=false
BREWFS_FUSE_WRITEBACK=0
BREWFS_WRITEBACK_PERSIST_SYNC=true
BREWFS_WRITEBACK_REQUIRE_STAGE_BEFORE_COMMIT=true
BREWFS_CHUNK_SIZE=67108864
BREWFS_BLOCK_SIZE=4194304
BREWFS_COMPRESSION=none
BREWFS_VERIFY_CACHE_CHECKSUM=full
BREWFS_READ_MEMORY_BYTES=4294967296
BREWFS_WRITE_MEMORY_BYTES=4294967296
BREWFS_READ_SSD_BYTES=8589934592
BREWFS_WRITE_SSD_BYTES=4294967296
BREWFS_MEMORY_BUDGET_BYTES=12884901888
BREWFS_S3_MAX_CONCURRENCY=16
BREWFS_WRITEBACK_UPLOAD_CONCURRENCY=6
BREWFS_UPLOAD_CONCURRENCY=32
BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES=2147483648
BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES=3221225472
BREWFS_FUSE_WORKERS=16
BREWFS_FUSE_MAX_BACKGROUND=512
BREWFS_PREFETCH_MAX_BYTES=1073741824
BREWFS_PREFETCH_CONCURRENCY=2
BREWFS_METADATA_OPEN_CACHE_TTL_MS=0
BREWFS_METADATA_ALLOW_WRITE_OPEN_CACHE=false
PERF_FIO_PREFILL_DRAIN=true
PERF_FIO_PREFILL_REMOUNT=true
PERF_FIO_PREFILL_DRAIN_PENDING_BYTES=0
PERF_FIO_PREFILL_DRAIN_TIMEOUT_SECS=600
PERF_FIO_POST_WRITE_DRAIN=true
PERF_FIO_POST_WRITE_DRAIN_PENDING_BYTES=0
PERF_FIO_POST_WRITE_DRAIN_TIMEOUT_SECS=600
PERF_FIO_POST_WRITE_DRAIN_INTERVAL_SECS=1
PERF_METADATA_POST_TOOL_DRAIN=true
PERF_METADATA_POST_TOOL_DRAIN_PENDING_BYTES=0
PERF_METADATA_POST_TOOL_DRAIN_TIMEOUT_SECS=600
PERF_FIO_COLD_READ_CLEAR_CACHE=false
TOKIO_WORKER_THREADS=1
PERF_FIO_LAT_PERCENTILES=1
PERF_FIO_CLAT_PERCENTILES=1
PERF_FIO_PERCENTILE_LIST=50:95:99:99.9
PERF_FIO_RANDSEED=20261031
PERF_FIO_RANDREPEAT=1
""".strip().splitlines())

# Remaining writeback-throughput-profile defaults from the same compose
# runner. These are explicit to prevent host exports from changing a cell.
WRITEBACK_PROFILE_ENV = {
    "BREWFS_WRITEBACK_MODE": "commit_before_upload",
    "BREWFS_CACHE_ROOT": "/var/lib/brewfs/cache",
    "BREWFS_DIRTY_SLICE_TARGET_SIZE": "67108864",
    "BREWFS_CACHED_BLOCK_ASSEMBLER": "true",
    "BREWFS_POPULATE_WRITE_CACHE_AFTER_UPLOAD": "true",
    "BREWFS_PERSIST_WRITE_CACHE_AFTER_UPLOAD": "true",
    "BREWFS_METADATA_OPEN_CACHE_CAPACITY": "65536",
}

MATRICES = {
    "p-w": "fio-seqwrite fio-randwrite fio-randrw",
    "p-s": "fio-randwrite fio-randrw",
    "p-m": "metaperf dirstress dirperf",
    "p-broad": "fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw fio-bigread fio-bigwrite metaperf dirstress dirperf",
    "client-io": "ws0-io",
    "client-metadata": "ws0-metadata",
}


def matrix_environment(matrix: str, arm: str, sample_rate: int, smoke: bool = False) -> dict[str, str]:
    if sample_rate < 1:
        raise ValueError("sample rate must be positive")
    env = {**PROTOCOL_ENV, **WRITEBACK_PROFILE_ENV, "PERF_TOOLS": MATRICES[matrix],
           "BREWFS_REQUEST_TRACE": "true" if arm == "bobs-on" else "false",
           "BREWFS_REQUEST_TRACE_SAMPLE_RATE": str(sample_rate)}
    if matrix == "p-s":
        env["PERF_FIO_BS"] = "4k"
    if matrix in ("p-m", "p-broad"):
        env.update(PERF_METAPERF_SECONDS="30", PERF_METAPERF_FILE_SIZE="4096",
                   PERF_METAPERF_OP_FILES="200", PERF_METAPERF_BG_FILES="2000")
    if matrix == "p-broad":
        env.update(PERF_FIO_BIGWRITE_SIZE="1g", PERF_FIO_BIGWRITE_NUMJOBS="8",
                   PERF_FIO_BIGREAD_SIZE="1g", PERF_FIO_BIGREAD_NUMJOBS="8",
                   PERF_FIO_BIGREAD_WARMUP_PASSES="1", PERF_FIO_BIGREAD_REPEATS="3",
                   PERF_FIO_BIGREAD_REMOUNT_BETWEEN_REPEATS="true",
                   PERF_FIO_BIGREAD_EVICT_LOCAL_CACHE_PAGES="true")
    if smoke:
        env.update(PERF_FIO_SIZE="8m", PERF_FIO_NUMJOBS="1", PERF_FIO_RUNTIME="1",
                   PERF_EXPERIMENT_VARIABLE="infrastructure-smoke-not-baseline")
    return env


def compose_run(argv: list[str]) -> int:
    """Run immutable copies of the existing runner in a fresh compose project.

    No builds occur in this command. B0 uses the very same collection envelope;
    absent B0 counters remain missing evidence, never synthesized zeroes.
    """
    parser = argparse.ArgumentParser(description="WS-0 Redis/RustFS collection envelope (not an acceptance decision)")
    parser.add_argument("--artifact", type=pathlib.Path, required=True)
    parser.add_argument("--binary", type=pathlib.Path, required=True)
    parser.add_argument("--source-tree", type=pathlib.Path, required=True)
    parser.add_argument("--image", required=True, help="already built perf image with strace and real xfstests tools")
    parser.add_argument("--rustfs-image", required=True)
    parser.add_argument("--redis-image", default="redis:7.2-alpine")
    parser.add_argument("--init-image", default="amazon/aws-cli:latest")
    parser.add_argument("--otlp-python-path", type=pathlib.Path, required=True,
                        help="directory with pinned opentelemetry-proto and protobuf packages")
    parser.add_argument("--matrix", choices=tuple(MATRICES), required=True)
    parser.add_argument("--arm", choices=("b0", "bobs-off", "bobs-on"), required=True)
    parser.add_argument("--sample-rate", type=int, default=128)
    parser.add_argument("--features", required=True, help="features used to build the supplied binary")
    parser.add_argument("--diagnostic-smoke", action="store_true")
    args = parser.parse_args(argv)
    artifact, source = args.artifact.resolve(), args.source_tree.resolve()
    if artifact.exists():
        parser.error("artifact directory already exists; preserve previous experiments")
    if not args.binary.is_file() or not args.otlp_python_path.is_dir():
        parser.error("binary and pinned OTLP Python dependencies must exist")
    if not args.diagnostic_smoke:
        from fuse_read_cache import compiler_processes
        if compiler_processes():
            parser.error("cargo/rustc is running; finish builds before measurement")
    root = pathlib.Path(__file__).resolve().parents[2]
    env = matrix_environment(args.matrix, args.arm, args.sample_rate, args.diagnostic_smoke)
    revision = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
    changed = subprocess.check_output(["git", "-C", str(source), "ls-files", "-m", "-o", "--exclude-standard", "-z"]).split(b"\0")
    changed = [os.fsdecode(path) for path in changed if path]
    if args.arm == "b0" and (revision != "80ddaf739e163c8933b0bb84390c4aa559bd2b40" or changed):
        parser.error("B0 requires a clean source tree at the fixed main SHA")
    artifact.mkdir(parents=True)
    envelope = artifact / "envelope"
    envelope.mkdir()
    command_log = artifact / "commands.jsonl"

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess:
        with command_log.open("a") as handle:
            handle.write(json.dumps({"argv": command, "started_ns": time.time_ns()}) + "\n")
        result = subprocess.run(command, **kwargs)
        with command_log.open("a") as handle:
            handle.write(json.dumps({"exit_code": result.returncode, "ended_ns": time.time_ns()}) + "\n")
        return result

    image_ids = {}
    for service, name in (("perf", args.image), ("redis", args.redis_image), ("rustfs", args.rustfs_image), ("rustfs-init", args.init_image)):
        data = json.loads(run(["docker", "image", "inspect", name], capture_output=True, text=True, check=True).stdout)[0]
        image_ids[service] = {"reference": name, "id": data["Id"], "repo_digests": data.get("RepoDigests", [])}
    env.update(BREWFS_SOURCE_REVISION=revision, BREWFS_SOURCE_DIRTY=str(bool(changed)).lower(),
               BREWFS_DATA_BACKEND="s3", BREWFS_META_BACKEND="redis", BREWFS_META_URL="redis://redis:6379/0",
               BREWFS_S3_ENDPOINT="http://rustfs:9000", BREWFS_ARTIFACT_DIR="/artifacts/run",
               PERF_BACKEND_METRICS_URL="http://collector:14318/snapshot")
    diff = subprocess.check_output(["git", "-C", str(source), "diff", "--binary", "HEAD"])
    (envelope / "source.patch").write_bytes(diff)
    env["BREWFS_SOURCE_DIFF_SHA256"] = hashlib.sha256(diff).hexdigest()
    changed_hashes = {}
    for name in changed:
        path = source / name
        changed_hashes[name] = hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else "deleted"
        if path.is_file():
            target = envelope / "changed-source" / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
    payloads = {"run_perf_in_container.sh": root / "docker/compose-xfstests/run_perf_in_container.sh",
                "perf_lines_observe.py": pathlib.Path(__file__).resolve(),
                "perf_manifest.py": root / "tools/perf/perf_manifest.py", "brewfs": args.binary.resolve()}
    for name, path in payloads.items():
        shutil.copy2(path, envelope / name)
        (envelope / name).chmod((envelope / name).stat().st_mode | 0o111)
    hashes = {name: hashlib.sha256((envelope / name).read_bytes()).hexdigest() for name in payloads}
    identity = {"schema": "ws0-envelope-v1", "argv": sys.argv, "arm": args.arm, "matrix": args.matrix,
                "diagnostic_smoke": args.diagnostic_smoke, "baseline_accepted": False, "source_revision": revision,
                "changed_source_sha256": changed_hashes, "payload_sha256": hashes, "images": image_ids,
                "features": args.features, "environment": env,
                "binary_source_association": "caller supplied; build log and these hashes must be verified before acceptance",
                "otlp_packages": {path.parent.name: hashlib.sha256(path.read_bytes()).hexdigest()
                                  for path in args.otlp_python_path.glob("*.dist-info/METADATA")},
                "compiler": subprocess.check_output(["rustc", "-vV"], text=True),
                "kernel": os.uname().release, "kernel_io_pages_fix": "unverified",
                "host_memory": pathlib.Path("/proc/meminfo").read_text(),
                "cpu_count": os.cpu_count(), "disk_free_bytes": shutil.disk_usage(artifact).free}
    (envelope / "manifest.json").write_text(json.dumps(identity, indent=2) + "\n")
    project = "brewfs-ws0-" + hashlib.sha256(str(artifact).encode()).hexdigest()[:12]
    # Render from the repository compose topology. Then remove shared names,
    # host ports and host-selected volumes, assigning this run private state.
    config = json.loads(run(["docker", "compose", "-p", project, "-f", str(root / "docker/compose-xfstests/docker-compose.redis-perf.yml"),
                             "config", "--format", "json"], env={"PATH": os.environ["PATH"], **env},
                            capture_output=True, text=True, check=True).stdout)
    services = {}
    for name in ("redis", "rustfs", "rustfs-init", "perf"):
        service = config["services"][name]
        for key in ("container_name", "ports", "build"):
            service.pop(key, None)
        service.update(image=image_ids[name]["id"], networks={"default": None})
        services[name] = service
    services["redis"]["volumes"] = ["redis-data:/data"]
    services["rustfs"]["volumes"] = ["object-data:/data"]
    # RustFS omits never-used metric series. Establish a real PUT counter
    # outside the client dataset/window; do not manufacture an initial zero.
    services["rustfs-init"]["command"][0] += (
        '\nprintf "ws0-counter-seed\\n" > /tmp/ws0-counter-seed\n'
        'aws --endpoint-url http://rustfs:9000 s3api put-object --bucket brewfs-data '
        '--key ws0-counter-seed --body /tmp/ws0-counter-seed >/dev/null\n'
    )
    services["rustfs"].setdefault("environment", {}).update(
        RUSTFS_OBS_METRIC_ENDPOINT="http://collector:14318/v1/metrics", RUSTFS_OBS_METER_INTERVAL="1",
        RUSTFS_OBS_TRACES_EXPORT_ENABLED="false", RUSTFS_OBS_LOGS_EXPORT_ENABLED="false", RUSTFS_OBS_METRICS_EXPORT_ENABLED="true")
    (artifact / "run").mkdir()
    shared_mounts = [{"type": "bind", "source": str(artifact / "run"), "target": "/artifacts/run"}]
    for name in payloads:
        shared_mounts.append({"type": "bind", "source": str(envelope / name), "target": f"/usr/local/bin/{name}", "read_only": True})
    services["perf"]["volumes"] = [*shared_mounts, "client-data:/var/lib/brewfs"]
    services["perf"].setdefault("environment", {}).update(env)
    (artifact / "backend-otlp").mkdir()
    services["collector"] = {"image": image_ids["perf"]["id"], "init": True, "networks": {"default": None},
        "entrypoint": ["python3", "/usr/local/bin/perf_lines_observe.py", "serve-otlp", "--bind", "0.0.0.0", "--directory", "/backend-otlp"],
        "environment": {"PYTHONPATH": "/otlp-python"}, "volumes": [
            {"type": "bind", "source": str(envelope / "perf_lines_observe.py"), "target": "/usr/local/bin/perf_lines_observe.py", "read_only": True},
            {"type": "bind", "source": str(args.otlp_python_path.resolve()), "target": "/otlp-python", "read_only": True},
            {"type": "bind", "source": str(artifact / "backend-otlp"), "target": "/backend-otlp"}]}
    compose = envelope / "compose.json"
    compose.write_text(json.dumps({"name": project, "services": services, "networks": {"default": {}},
                                  "volumes": {name: {} for name in ("redis-data", "object-data", "client-data")}}, indent=2) + "\n")
    base = ["docker", "compose", "-p", project, "-f", str(compose)]
    status = 1
    from fuse_read_cache import CompilerWatch, CompilerOverlap
    monitor = None if args.diagnostic_smoke else CompilerWatch()
    try:
        with monitor if monitor is not None else nullcontext(), (artifact / "console.log").open("w") as log:
            run([*base, "up", "-d", "--wait", "redis", "rustfs", "collector"], stdout=log, stderr=subprocess.STDOUT, check=True)
            run([*base, "run", "--rm", "rustfs-init"], stdout=log, stderr=subprocess.STDOUT, check=True)
            status = run([*base, "run", "--rm", "--no-deps", "perf"], stdout=log, stderr=subprocess.STDOUT).returncode
        evidence = summarize(artifact / "run")
        (artifact / "observation.json").write_text(json.dumps(evidence, indent=2) + "\n")
        expected_trace = "valid" if args.arm == "bobs-on" else "disabled"
        if evidence["status"] != expected_trace:
            (artifact / "run/request-observation.invalid").touch()
            status = status or 1
        if any(row["status"] == "invalid" for row in evidence["object_ledger"]["tools"].values()):
            (artifact / "run/object-ledger.invalid").touch()
            status = status or 1
    except CompilerOverlap:
        (artifact / "run/compiler-overlap.invalid").touch()
        status = 1
    finally:
        if monitor is not None:
            (artifact / "compiler-watch.json").write_text(json.dumps(monitor.report(), indent=2) + "\n")
        with (artifact / "service.log").open("w") as log:
            run([*base, "logs", "--no-color"], stdout=log, stderr=subprocess.STDOUT)
        tasks = run(["ps", "-eo", "pid,ppid,stat,wchan:40,comm"], capture_output=True, text=True, check=True).stdout
        (artifact / "teardown-tasks.txt").write_text(tasks)
        blocked = any("request_wait_answer" in line and "D" in line.split()[2] for line in tasks.splitlines()[1:])
        cleanup_status = 1
        with (artifact / "cleanup.log").open("w") as log:
            if blocked:
                log.write("D-state request_wait_answer detected; preserved services and volumes for kernel diagnostics.\n")
                (artifact / "run/teardown.invalid").touch()
            else:
                try:
                    cleanup_status = run([*base, "down", "-v", "--remove-orphans", "--timeout", "60"],
                                         stdout=log, stderr=subprocess.STDOUT, timeout=90).returncode
                except subprocess.TimeoutExpired:
                    (artifact / "run/teardown.invalid").touch()
        (artifact / "exit.json").write_text(json.dumps({"runner": status, "cleanup": cleanup_status, "baseline_accepted": False}) + "\n")
    return status or cleanup_status

YAML_PROTOCOL_KEYS = {
    **{f"cache.{key}": "BREWFS_" + key.upper() for key in (
        "read_memory_bytes", "write_memory_bytes", "read_ssd_bytes", "write_ssd_bytes",
        "memory_budget_bytes", "compression", "verify_cache_checksum", "upload_concurrency",
        "prefetch_max_bytes", "prefetch_concurrency", "writeback_persist_sync",
        "writeback_require_stage_before_commit", "writeback_recent_pending_soft_bytes",
        "writeback_recent_pending_hard_bytes")},
    "layout.chunk_size": "BREWFS_CHUNK_SIZE", "layout.block_size": "BREWFS_BLOCK_SIZE",
    "data.s3.max_concurrency": "BREWFS_S3_MAX_CONCURRENCY",
    "meta.open_file_cache_ttl_ms": "BREWFS_METADATA_OPEN_CACHE_TTL_MS",
    "meta.allow_write_open_cache": "BREWFS_METADATA_ALLOW_WRITE_OPEN_CACHE",
    "fuse.workers": "BREWFS_FUSE_WORKERS", "fuse.max_background": "BREWFS_FUSE_MAX_BACKGROUND",
    **{f"cache.{key}": "BREWFS_" + key.upper() for key in (
        "writeback_mode", "dirty_slice_target_size", "populate_write_cache_after_upload",
        "persist_write_cache_after_upload")},
    "cache.root": "BREWFS_CACHE_ROOT",
}


def generated_yaml_scalars(text: str) -> dict[str, str]:
    """Read the runner's simple generated mapping, not arbitrary user YAML."""
    stack: list[tuple[int, str]] = []
    values = {}
    for line in text.splitlines():
        match = re.match(r"^( *)([A-Za-z_][A-Za-z0-9_]*):(?:\s*(.*))?$", line)
        if not match:
            continue
        indent, key, value = len(match[1]), match[2], match[3] or ""
        while stack and stack[-1][0] >= indent:
            stack.pop()
        path = ".".join([item[1] for item in stack] + [key])
        if value:
            if path in values:
                raise ValueError(f"duplicate generated YAML key: {path}")
            values[path] = value.strip("\"'")
        else:
            stack.append((indent, key))
    return values


def capture_mount(argv: list[str]) -> int:
    from perf_manifest import redact_value
    parser = argparse.ArgumentParser(description="Capture actual mount process and generated config")
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--config", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--require-protocol", action="store_true")
    args = parser.parse_args(argv)
    proc = pathlib.Path(f"/proc/{args.pid}")
    environment = dict(item.split("=", 1) for item in (proc / "environ").read_text().split("\0") if "=" in item)
    config = generated_yaml_scalars(args.config.read_text())
    expected = {**PROTOCOL_ENV, **WRITEBACK_PROFILE_ENV}
    failures = []
    for path, variable in YAML_PROTOCOL_KEYS.items():
        if config.get(path) != expected[variable]:
            failures.append({"field": path, "actual": config.get(path), "expected": expected[variable]})
    for variable in ("TOKIO_WORKER_THREADS", "BREWFS_FUSE_DIRECT_IO", "BREWFS_FUSE_READ_DIRECT_IO",
                     "BREWFS_FUSE_WRITE_DIRECT_IO", "BREWFS_FUSE_WRITEBACK", "BREWFS_FUSE_KEEP_CACHE",
                     "BREWFS_WRITEBACK_UPLOAD_CONCURRENCY", "BREWFS_CACHED_BLOCK_ASSEMBLER"):
        if environment.get(variable) != expected[variable]:
            failures.append({"field": variable, "actual": environment.get(variable), "expected": expected[variable]})
    expected_ttl = "1000" if config.get("volume_format") == "workspace-v1" else "0"
    if config.get("fuse.cache_ttl_ms") != expected_ttl:
        failures.append({"field": "fuse.cache_ttl_ms", "actual": config.get("fuse.cache_ttl_ms"), "expected": expected_ttl})
    report = {"pid": args.pid, "argv": (proc / "cmdline").read_text().rstrip("\0").split("\0"),
              "binary_sha256": hashlib.sha256((proc / "exe").read_bytes()).hexdigest(),
              "environment": {key: redact_value(key, value) for key, value in environment.items() if key.startswith(("BREWFS_", "TOKIO_", "ASYNCFUSE_", "RUST_"))},
              "generated_yaml": {key: redact_value(key, value) for key, value in config.items()},
              "protocol_mismatches": failures, "kernel": os.uname().release,
              "fio_version": subprocess.run(["fio", "--version"], capture_output=True, text=True, check=True).stdout.strip(),
              "kernel_io_pages_fix": "unverified", "network_rtt": "unavailable"}
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return int(args.require_protocol and bool(failures))


def system_sample(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Preserve raw counters for same-window CPU/RSS/context switch/IO/network analysis")
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args(argv)
    paths = [f"/proc/{args.pid}/{name}" for name in ("stat", "status", "io", "cgroup")]
    paths += ["/proc/net/dev", "/proc/diskstats", "/proc/pressure/io", "/proc/pressure/cpu", "/proc/pressure/memory"]
    paths += [f"/sys/fs/cgroup/{name}" for name in ("cpu.max", "memory.max", "memory.current", "io.stat", "io.pressure")]
    report: dict[str, Any] = {"monotonic_ns": time.monotonic_ns(), "wall_ns": time.time_ns(), "pid": args.pid, "raw": {}, "unavailable": {}}
    for name in paths:
        try:
            report["raw"][name] = pathlib.Path(name).read_text()
        except OSError as exc:
            report["unavailable"][name] = str(exc)
    args.output.write_text(json.dumps(report) + "\n")
    return int(f"/proc/{args.pid}/stat" in report["unavailable"])


def rustfs_http_counters(export: dict[str, Any]) -> dict[str, Any]:
    """RustFS 1.0.4's external HTTP outcomes, from cumulative OTLP sums.

    Retries are separate HTTP attempts. This does not invent SDK retry IDs or
    interpret storage-internal metrics as external requests. Missing operation
    series remain absent. Raw OTLP is retained alongside this projection.
    """
    operations: dict[str, dict[str, Any]] = {}
    for resource in export.get("resourceMetrics", []):
        for scope in resource.get("scopeMetrics", []):
            for metric in scope.get("metrics", []):
                if metric.get("name") != "rustfs_s3_http_requests_total":
                    continue
                aggregate = metric.get("sum", {})
                if aggregate.get("aggregationTemporality") != "AGGREGATION_TEMPORALITY_CUMULATIVE":
                    raise ValueError("backend HTTP counter is not cumulative")
                for point in aggregate.get("dataPoints", []):
                    attributes = {item["key"]: item["value"].get("stringValue") for item in point.get("attributes", [])}
                    operation, outcome = attributes.get("op"), attributes.get("outcome")
                    if operation is None or outcome is None or "asInt" not in point:
                        raise ValueError("incomplete backend HTTP outcome")
                    count = int(point["asInt"])
                    if count < 0:
                        raise ValueError("negative backend HTTP counter")
                    start, sampled = point.get("startTimeUnixNano"), point.get("timeUnixNano")
                    if any(value is None or not str(value).isdigit() for value in (start, sampled)) or not 0 < int(start) <= int(sampled):
                        raise ValueError("missing or invalid backend counter timestamps")
                    state = operations.setdefault(operation, {"outcomes": {}, "start_time_ns": start, "sample_time_ns": sampled})
                    if state["start_time_ns"] != start or state["sample_time_ns"] != sampled:
                        raise ValueError("mixed backend counter epochs or sample times")
                    if outcome in state["outcomes"]:
                        raise ValueError("multiple backend resources require explicit grouping")
                    state["outcomes"][outcome] = count
    for state in operations.values():
        state["completed_attempts"] = sum(state["outcomes"].values())
        state["success"] = state["outcomes"].get("2xx")
        state["observed_failed_attempts"] = sum(value for outcome, value in state["outcomes"].items() if outcome != "2xx")
    return operations


def serve_otlp(argv: list[str]) -> int:
    # Optional dependency used only by the backend collector. Pin and record
    # it in the experiment environment; ordinary report/probe commands use
    # only the standard library.
    import gzip
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from google.protobuf.json_format import MessageToDict
    from google.protobuf.message import DecodeError
    from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
    parser = argparse.ArgumentParser(description="Archive RustFS cumulative OTLP exports and expose fresh counter snapshots")
    parser.add_argument("--directory", type=pathlib.Path, required=True)
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=14318)
    args = parser.parse_args(argv)
    args.directory.mkdir(parents=True, exist_ok=True)
    latest: dict[str, Any] = {}
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_: Any) -> None:
            pass

        def do_POST(self) -> None:
            if self.path != "/v1/metrics":
                self.send_error(404)
                return
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 < size <= 16 * 1024 * 1024:
                self.send_error(413)
                return
            body = self.rfile.read(size)
            now = time.time_ns()
            (args.directory / f"{now}.otlp").write_bytes(body)
            try:
                payload = gzip.decompress(body) if self.headers.get("Content-Encoding") == "gzip" else body
                export = MessageToDict(ExportMetricsServiceRequest.FromString(payload))
                operations = rustfs_http_counters(export)
                (args.directory / f"{now}.json").write_text(json.dumps(export) + "\n")
                with lock:
                    latest.update(received_at_ns=now, sequence=latest.get("sequence", 0) + 1, operations=operations)
            except (ValueError, OSError, DecodeError) as exc:
                (args.directory / f"{now}.error").write_text(str(exc))
                self.send_error(400)
                return
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self) -> None:
            if self.path != "/snapshot":
                self.send_error(404)
                return
            with lock:
                data = dict(latest)
            body = json.dumps(data).encode()
            self.send_response(200 if data else 503)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    ThreadingHTTPServer((args.bind, args.port), Handler).serve_forever()
    return 0


def backend_snapshot(argv: list[str]) -> int:
    import urllib.request
    parser = argparse.ArgumentParser(description="Require a fresh cumulative backend PUT observation")
    parser.add_argument("--url", required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--after-ns", type=int, default=0)
    parser.add_argument("--wait-secs", type=int, default=0)
    args = parser.parse_args(argv)
    deadline = time.monotonic() + args.wait_secs
    while True:
        with urllib.request.urlopen(args.url, timeout=3) as response:
            data = json.load(response)
        data["requested_after_ns"] = args.after_ns
        args.output.write_text(json.dumps(data, indent=2) + "\n")
        put = data.get("operations", {}).get("s3:PutObject")
        if put is None:
            if time.monotonic() < deadline:
                time.sleep(1)
                continue
            raise ValueError("missing backend PUT counter after collection deadline")
        now = time.time_ns()
        for timestamp in (data["received_at_ns"], put["sample_time_ns"]):
            age = now - int(timestamp)
            if not 0 <= age <= 3_000_000_000:
                raise ValueError(f"backend export is stale or has clock skew: age_ns={age}")
        if int(put["sample_time_ns"]) >= args.after_ns:
            break
        if time.monotonic() >= deadline:
            raise ValueError("backend counter sample precedes requested phase boundary")
        time.sleep(1)
    # Distinct collector HTTP receipts do not prove distinct observations:
    # an exporter may retry the very same OTLP payload.
    values = [put["sample_time_ns"], put["start_time_ns"], put["success"], put["completed_attempts"]]
    if any(value is None or not str(value).isdigit() for value in values):
        raise ValueError("missing backend PUT counter")
    print(" ".join(str(value) for value in values))
    return 0


def summarize_write_group(records: list[dict[str, Any]]) -> dict[str, Any]:
    total = sum(record["total_ns"] for record in records)
    complete = bool(records) and all(not validate_partition(record) for record in records)
    exclusive = {name: sum(record.get("exclusive_ns", {}).get(name, 0) for record in records) for name in SEGMENTS}
    wait = sum(exclusive[name] for name in WAIT_SEGMENTS) if complete else None
    return {"count": len(records), "total_ns": total,
            "wait_ns": wait, "wait_share": wait / total if wait is not None and total else None,
            "timing_semantics": "exclusive foreground intervals; background excluded" if complete else "unavailable exclusive attribution",
            "segments": {name: {"sum_ns": exclusive[name] if complete else None} for name in SEGMENTS},
            "background_ns": {name: sum(record.get("background_ns", {}).get(name, 0) for record in records) for name in SEGMENTS} if complete else None}


def write_breakdown(records: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted((record for record in records if record.get("opcode") == 16 and record.get("total_ns", 0) > 0), key=lambda record: record["total_ns"], reverse=True)
    count = math.ceil(len(ordered) * .01)
    return {"ordinary": summarize_write_group(ordered[count:]), "slowest_1pct": summarize_write_group(ordered[:count]),
            "slowest_count": count, "tail_reliable_min_samples": 1000, "tail_reliable": len(ordered) >= 1000}


def measurement_windows(artifact: pathlib.Path, instances: list[dict[str, Any]]) -> dict[str, Any]:
    """Select by request receipt within each actual timed tool invocation."""
    try:
        with (artifact / "phase-boundaries.tsv").open() as handle:
            boundaries = [(tool, phase, int(at)) for tool, phase, at in csv.reader(handle, delimiter="\t")]
    except (OSError, ValueError) as exc:
        return {"status": "unavailable", "reason": str(exc), "windows": []}
    pending, windows, failures = {}, [], []
    for tool, phase, at in boundaries:
        if phase == "tool_start":
            if tool in pending:
                failures.append(f"{tool}:duplicate start")
            pending[tool] = at
        elif phase == "tool_return":
            start = pending.pop(tool, None)
            if start is None or at < start:
                failures.append(f"{tool}:invalid boundary order")
                continue
            selected = [record for instance in instances for record in instance["records"]
                        if start <= record.get("received_at_ns", -1) < at]
            windows.append({"tool": tool, "start_ns": start, "return_ns": at,
                            "request_count": len(selected), "write_wait_breakdown": write_breakdown(selected),
                            "scope": "requests received during tool invocation, including close; excludes prefill and background drain"})
    failures.extend(f"{tool}:missing return" for tool in pending)
    return {"status": "invalid" if failures else "measured" if windows else "unavailable", "failures": failures, "windows": windows}


def warn(message: str) -> None:
    print(f"warning: {message}", file=sys.stderr)


def parse_value(raw: str) -> float | None:
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if value == value and value not in (float("inf"), float("-inf")) else None


def parse_labels(raw: str) -> dict[str, str]:
    labels: dict[str, str] = {}
    for match in re.finditer(r'([A-Za-z_][A-Za-z0-9_]*)="((?:\\.|[^"\\])*)"', raw):
        labels[match.group(1)] = bytes(match.group(2), "utf-8").decode(
            "unicode_escape"
        )
    return labels


TRACE_RECORD_RE = re.compile(r"^brewfs_request_trace_record\{([^}]*)\}\s+(\S+)$")
TRACE_SEGMENT_RE = re.compile(
    r"^brewfs_request_trace_segment_ns\{([^}]*)\}\s+(\S+)$"
)


def parse_stats(path: pathlib.Path) -> tuple[dict[str, Any], list[str], list[dict[str, Any]]]:
    metrics: dict[str, Any] = {}
    malformed: list[str] = []
    traces: dict[str, dict[str, Any]] = {}
    versioned: dict[str, dict[str, Any]] = {}
    try:
        lines = path.read_text(errors="replace").splitlines()
    except OSError as exc:
        return {}, [f"unreadable:{exc}"], []
    for line in lines:
        line = line.strip()
        if line.startswith("# brewfs_request_trace_instance "):
            metrics["__instance_id"] = line.split()[-1]
            continue
        if line.startswith("# brewfs_request_trace_v2 "):
            try:
                record = json.loads(line.split(" ", 2)[2])
                for name in ("segments", "exclusive_ns", "background_ns"):
                    values = record[name]
                    if len(values) != len(SEGMENTS) or any(type(v) is not int or v < 0 for v in values):
                        raise ValueError(f"invalid {name}")
                    record[name] = dict(zip(SEGMENTS, values))
                record["wire_version"] = 2
                versioned[str(record["unique"])] = record
            except (KeyError, TypeError, ValueError) as exc:
                malformed.append(f"trace_v2:{exc}")
            continue
        if not line or line.startswith("#") or not line.startswith("brewfs_"):
            continue
        trace_match = TRACE_RECORD_RE.match(line)
        if trace_match:
            value = parse_value(trace_match.group(2))
            labels = parse_labels(trace_match.group(1))
            unique = labels.get("unique")
            if value is None or unique is None:
                malformed.append(line)
                continue
            record = traces.setdefault(
                unique,
                {
                    "unique": int(unique) if unique.isdigit() else unique,
                    "opcode": int(labels["opcode"]) if labels.get("opcode", "").isdigit() else None,
                    "terminal": labels.get("terminal"),
                    "file_id": labels.get("file_id") or None,
                    "chunk_id": labels.get("chunk_id") or None,
                    "slice_id": labels.get("slice_id") or None,
                    "lease_id": labels.get("lease_id") or None,
                    "total_ns": int(value),
                    "segments": {},
                },
            )
            record["total_ns"] = int(value)
            continue
        segment_match = TRACE_SEGMENT_RE.match(line)
        if segment_match:
            value = parse_value(segment_match.group(2))
            labels = parse_labels(segment_match.group(1))
            unique = labels.get("unique")
            segment = labels.get("segment")
            if value is None or unique is None or segment not in SEGMENTS:
                malformed.append(line)
                continue
            record = traces.setdefault(
                unique,
                {
                    "unique": int(unique) if unique.isdigit() else unique,
                    "opcode": int(labels["opcode"]) if labels.get("opcode", "").isdigit() else None,
                    "terminal": labels.get("terminal"),
                    "total_ns": 0,
                    "segments": {},
                },
            )
            record.setdefault("segments", {})[segment] = int(value)
            continue
        # Prometheus labels are retained in the key.  The lifecycle counters
        # used by this validator have no labels; trace records are summarized
        # separately and never treated as aggregate counters.
        match = re.match(r"^(\S+)\s+(\S+)$", line)
        if not match:
            malformed.append(line)
            continue
        value = parse_value(match.group(2))
        if value is None:
            malformed.append(line)
            continue
        metrics[match.group(1)] = value
    traces.update(versioned)
    for record in traces.values():
        record["instance_id"] = metrics.get("__instance_id")
    return metrics, malformed, list(traces.values())


def validate_partition(record: dict[str, Any]) -> list[str]:
    """Recompute the disjoint partition from raw intervals, per request."""
    failures = []
    try:
        total = record["total_ns"]
        intervals = record["intervals"]
        if type(total) is not int or total <= 0:
            raise ValueError("invalid total")
        if record["intervals_truncated"]:
            failures.append("intervals_truncated")
        boundaries = {0, total}
        for start, end, segment in intervals:
            if any(type(v) is not int for v in (start, end, segment)) or not (0 <= start <= end <= total and 0 <= segment < len(SEGMENTS)):
                raise ValueError("invalid interval")
            boundaries.update((start, end))
        parts = dict.fromkeys(SEGMENTS, 0)
        ambiguous = 0
        ordered = sorted(boundaries)
        for start, end in zip(ordered, ordered[1:]):
            covering = [i for i in intervals if i[0] <= start and i[1] >= end]
            selected = min(covering, key=lambda i: (SEGMENTS[i[2]] == "handler_poll", i[1] - i[0])) if covering else None
            parts[SEGMENTS[selected[2]] if selected else "handler"] += end - start
            if any(a[2] != b[2] and "handler_poll" not in (SEGMENTS[a[2]], SEGMENTS[b[2]]) and not ((a[0] <= b[0] and a[1] >= b[1]) or (b[0] <= a[0] and b[1] >= a[1])) for index, a in enumerate(covering) for b in covering[index + 1:]):
                ambiguous += end - start
        if parts != record["exclusive_ns"] or ambiguous != record["ambiguous_ns"]:
            failures.append("partition_mismatch")
        if parts["handler"] / total > 0.05:
            failures.append("unattributed_over_5pct")
        if ambiguous:
            failures.append("ambiguous_concurrent_intervals")
    except (KeyError, TypeError, ValueError, IndexError) as exc:
        failures.append(f"unavailable_partition:{exc}")
    return failures


def collect_requests(artifact: pathlib.Path) -> dict[str, Any]:
    """Merge rolling snapshots by mount identity and FUSE unique, not by inode.

    Ring eviction is not collection loss if an earlier snapshot retained the
    record. A missing mount identity or missing final closure stays unavailable.
    Background durations may increase after reply and are merged monotonically.
    """
    paths = sorted(set((artifact / "diagnostics").glob("stats-*.txt")) | set((artifact / "stats-timeseries").rglob("*.txt")))
    instances: dict[str, dict[str, Any]] = {}
    failures: list[str] = []
    for path in paths:
        metrics, malformed, records = parse_stats(path)
        if not metrics.get("brewfs_request_observation_enabled"):
            continue
        instance_id = metrics.get("__instance_id")
        if not instance_id:
            failures.append(f"{path}:missing_mount_identity")
            continue
        state = instances.setdefault(instance_id, {"records": {}, "sampled": 0, "closed": 0, "dropped": 0})
        for counter in ("sampled", "closed", "dropped"):
            value = metrics.get(f"brewfs_request_observation_{counter}_total")
            if value is None:
                failures.append(f"{path}:missing_{counter}")
            else:
                state[counter] = max(state[counter], int(value))
        failures.extend(f"{path}:{error}" for error in malformed)
        for record in records:
            key = str(record["unique"])
            previous = state["records"].get(key)
            if previous and previous.get("exclusive_ns") != record.get("exclusive_ns"):
                failures.append(f"{instance_id}/{key}:terminal_record_changed")
            if previous:
                for segment, value in previous.get("background_ns", {}).items():
                    record.setdefault("background_ns", {})[segment] = max(value, record.get("background_ns", {}).get(segment, 0))
            state["records"][key] = record
    summaries = []
    for instance_id, state in instances.items():
        records = list(state["records"].values())
        sampled, closed = state["sampled"], state["closed"]
        retained = len(records)
        if sampled == 0 or closed / sampled < .99:
            failures.append(f"{instance_id}:closure_below_99pct")
        lost = max(0, closed - retained) + state["dropped"]
        if sampled == 0 or lost / sampled >= .01:
            failures.append(f"{instance_id}:lost_samples_at_least_1pct")
        if retained > closed:
            failures.append(f"{instance_id}:more_records_than_closed_requests")
        for record in records:
            failures.extend(f"{instance_id}/{record['unique']}:{failure}" for failure in validate_partition(record))
        summaries.append({"instance_id": instance_id, "sampled": sampled, "closed": closed,
                          "collected": retained, "lost": lost, "records": records,
                          "window": "entire observed mount; may include prefill",
                          "write_wait_breakdown": write_breakdown(records)})
    return {"status": "invalid" if failures else "valid" if instances else "unavailable",
            "failures": failures, "instances": summaries}


def stats_paths(artifact: pathlib.Path) -> list[pathlib.Path]:
    diagnostics = artifact / "diagnostics"
    if not diagnostics.is_dir():
        return []
    return sorted(diagnostics.glob("stats-*-after.txt"))


def one_stats(path: pathlib.Path) -> dict[str, Any]:
    metrics, malformed, traces = parse_stats(path)
    missing = [name for name in REQUIRED if name not in metrics]
    for segment in SEGMENTS:
        for suffix in ("_count", "_sum"):
            name = f"brewfs_request_{segment}_ns{suffix}"
            if name not in metrics:
                missing.append(name)
    for segment in QUEUE_SEGMENTS:
        for suffix in ("_depth", "_oldest_age_ns"):
            name = f"brewfs_request_{segment}_queue{suffix}"
            if name not in metrics:
                missing.append(name)
    for operation in OPERATIONS:
        for suffix in ("_count", "_sum"):
            name = f"brewfs_request_operation_{operation}_ns{suffix}"
            if name not in metrics:
                missing.append(name)

    result: dict[str, Any] = {
        "path": str(path),
        "missing": sorted(set(missing)),
        "malformed": malformed,
        "trace_records": traces,
        "status": "unavailable" if missing or malformed else "disabled",
    }
    if missing or malformed:
        return result

    enabled = metrics["brewfs_request_observation_enabled"]
    received = metrics["brewfs_request_observation_received_total"]
    sampled = metrics["brewfs_request_observation_sampled_total"]
    closed = metrics["brewfs_request_observation_closed_total"]
    dropped = metrics["brewfs_request_observation_dropped_total"]
    active = metrics["brewfs_request_observation_active"]
    retained = len(traces)
    gap = metrics["brewfs_request_observation_closure_gap_ns_total"]
    accounted = sum(metrics[f"brewfs_request_{segment}_ns_sum"] for segment in ("fuse_queue_wait", "handler", "reply"))

    observation: dict[str, Any] = {
        "enabled": bool(enabled),
        "received": int(received),
        "sampled": int(sampled),
        "closed": int(closed),
        "dropped": int(dropped),
        "active": int(active),
        "retained_records": retained,
        "retained_fraction": (retained / closed) if closed > 0 else None,
        "trace_records_evicted": int(metrics["brewfs_request_trace_records_dropped_total"]),
        "closure_rate": (closed / sampled) if sampled > 0 else None,
        "dropped_rate": (dropped / sampled) if sampled > 0 else None,
        "closure_gap_ns_total": int(gap),
        "closure_gap_ratio": (gap / (gap + accounted)) if gap + accounted > 0 else None,
        "segments": {
            segment: {
                "count": int(metrics[f"brewfs_request_{segment}_ns_count"]),
                "sum_ns": int(metrics[f"brewfs_request_{segment}_ns_sum"]),
            }
            for segment in SEGMENTS
        },
        "queues": {
            segment: {
                "depth": int(metrics[f"brewfs_request_{segment}_queue_depth"]),
                "oldest_age_ns": int(
                    metrics[f"brewfs_request_{segment}_queue_oldest_age_ns"]
                ),
            }
            for segment in QUEUE_SEGMENTS
        },
        "operations": {
            operation: {
                "count": int(metrics[f"brewfs_request_operation_{operation}_ns_count"]),
                "sum_ns": int(metrics[f"brewfs_request_operation_{operation}_ns_sum"]),
            }
            for operation in OPERATIONS
        },
    }
    result["observation"] = observation
    write_records = [
        record
        for record in traces
        if record.get("opcode") == 16 and record.get("total_ns", 0) > 0
    ]

    ordered_writes = sorted(write_records, key=lambda record: record["total_ns"], reverse=True)
    slow_count = math.ceil(len(ordered_writes) * 0.01) if ordered_writes else 0
    slowest = ordered_writes[:slow_count]
    ordinary = ordered_writes[slow_count:]
    observation["write_trace_count"] = len(ordered_writes)
    observation["write_wait_breakdown"] = {
        "ordinary": summarize_write_group(ordinary),
        "slowest_1pct": summarize_write_group(slowest),
        "slowest_count": slow_count,
        "tail_reliable_min_samples": 1000,
        "tail_reliable": len(ordered_writes) >= 1000,
    }
    if not observation["enabled"]:
        result["status"] = "disabled"
    elif traces and all(record.get("wire_version") == 2 for record in traces):
        result["status"] = "snapshot"
        result["reason"] = "rolling snapshot; validate collection across all snapshots and mount instances"
    elif sampled <= 0:
        result["status"] = "unavailable"
        result["reason"] = "trace enabled but no sampled requests"
    elif (
        observation["closure_rate"] < 0.99
        or observation["dropped_rate"] >= 0.01
        or active != 0
        or (closed > 0 and retained / closed <= 0.99)
        or (observation["closure_gap_ratio"] is not None and observation["closure_gap_ratio"] > 0.05)
    ):
        result["status"] = "invalid"
        result["reason"] = "request lifecycle or retained sample coverage gate failed"
    else:
        result["status"] = "unavailable"
        result["reason"] = "exclusive request intervals and background separation are not present"
    return result


def read_manifest(artifact: pathlib.Path) -> dict[str, Any] | None:
    for name in ("manifest.json", "perf-manifest.json"):
        path = artifact / name
        if not path.is_file():
            continue
        try:
            if path.suffix == ".json":
                value = json.loads(path.read_text())
                return value if isinstance(value, dict) else {"value": value}
            return {"path": str(path), "text": path.read_text(errors="replace")}
        except (OSError, json.JSONDecodeError) as exc:
            return {"path": str(path), "error": str(exc)}
    return None


def object_ledger(artifact: pathlib.Path) -> dict[str, Any]:
    """Keep the entire write-induced window and the maintenance cost separate.

    These are measured HTTP outcomes, not inferred SDK retry counts. The byte
    numerator is the client's successful encoded PUT bytes; prefill counters
    are subtracted at the measured tool's before boundary.
    """
    tools: dict[str, Any] = {}
    maintenance = {}
    try:
        with (artifact / "maintenance.tsv").open() as handle:
            maintenance = {row["tool"]: row for row in csv.DictReader(handle, delimiter="\t")}
    except (OSError, KeyError):
        pass
    phases = ("before", "post-write-drained-after", "maintenance-before", "maintenance-after")
    for path in sorted((artifact / "results").glob("fio-*.json")):
        tool = path.stem
        row: dict[str, Any] = {"status": "unavailable"}
        tools[tool] = row
        try:
            jobs = json.loads(path.read_text())["jobs"]
            if not jobs or any(job.get("error", 0) for job in jobs):
                raise ValueError("missing jobs or fio error")
            logical_bytes = sum(int(job["write"]["io_bytes"]) for job in jobs)
            if tool.endswith("-prefill"):
                row.update(status="prefill-only", logical_write_bytes=logical_bytes,
                           reason="prefill is excluded from the measured tool numerator")
                continue
            if logical_bytes <= 0:
                row["reason"] = "no measured fio writes"
                continue
            window = maintenance[tool]
            if int(window["requested_observation_s"]) < 60 or int(window["ended_ns"]) - int(window["started_ns"]) < 60_000_000_000:
                raise ValueError("maintenance window shorter than 60 seconds")
            backend, client = [], []
            for phase in phases:
                data = json.loads((artifact / f"diagnostics/backend-{tool}-{phase}.json").read_text())
                backend.append(data["operations"]["s3:PutObject"])
                metrics, malformed, _ = parse_stats(artifact / f"diagnostics/stats-{tool}-{phase}.txt")
                if malformed:
                    raise ValueError(f"invalid client snapshot: {phase}")
                client.append(metrics)
            for previous, current in zip(backend, backend[1:]):
                if previous["start_time_ns"] != current["start_time_ns"]:
                    raise ValueError("backend counter epoch changed")
                for key in ("sample_time_ns", "success", "completed_attempts"):
                    if int(current[key]) < int(previous[key]):
                        raise ValueError(f"backend counter decreased: {key}")
            identities = {metrics["__instance_id"] for metrics in client}
            if len(identities) != 1:
                raise ValueError("client remounted inside object accounting window")

            def delta(start: int, end: int) -> dict[str, Any]:
                success = int(backend[end]["success"]) - int(backend[start]["success"])
                attempts = int(backend[end]["completed_attempts"]) - int(backend[start]["completed_attempts"])
                uploaded = client[end]["brewfs_s3_put_bytes_total"] - client[start]["brewfs_s3_put_bytes_total"]
                client_puts = client[end]["brewfs_s3_put_ops_total"] - client[start]["brewfs_s3_put_ops_total"]
                if uploaded < 0 or attempts < success:
                    raise ValueError("inconsistent object counters")
                if client_puts != success:
                    raise ValueError("client/backend PUT count mismatch; phase alignment or unaccounted backend writes")
                result = {"put_success": success, "put_completed_attempts": attempts,
                          "put_failed_attempts": attempts - success, "successful_put_bytes": uploaded,
                          "put_per_gib_written": success / (logical_bytes / 2**30),
                          "average_put_bytes": uploaded / success if success else None}
                batch_key = "brewfs_writeback_upload_batch_ops_total"
                tail_key = "brewfs_writeback_upload_partial_tail_ops_total"
                if all(key in metrics for metrics in (client[start], client[end]) for key in (batch_key, tail_key)):
                    batches = client[end][batch_key] - client[start][batch_key]
                    tails = client[end][tail_key] - client[start][tail_key]
                    if min(batches, tails) < 0 or tails > batches:
                        raise ValueError("inconsistent upload batch counters")
                    result.update(upload_batches=batches, partial_tail_batches=tails,
                                  partial_tail_batch_ratio=tails / batches if batches else None)
                return result

            # Construct all windows before publishing any normalized total.
            windows = {"active_close_drain": delta(0, 1), "maintenance": delta(2, 3),
                       "between_drain_and_maintenance": delta(1, 2), "total": delta(0, 3)}
            row.update(status="measured", logical_write_bytes=logical_bytes, **windows)
        except (OSError, KeyError, TypeError, json.JSONDecodeError) as exc:
            row["reason"] = f"missing or unreadable object evidence: {exc}"
        except ValueError as exc:
            row.update(status="invalid", reason=str(exc))
    return {"tools": tools,
            "scope": "active + close + extended strict drain + 60s maintenance; excludes prefill",
            "byte_source": "client successful encoded PUT bytes; outcome source: backend cumulative HTTP counters",
            "unavailable": ["SDK retry identities", "per-object partial-tail count", "publication rows/bytes",
                            "slice/chunk history", "overlap", "compaction bytes", "stale bytes", "GC backlog"]}


def syscall_completion(artifact: pathlib.Path) -> dict[str, Any]:
    """Use decoded FDs to avoid mixing loader/proc closes with data-file close."""
    pattern = re.compile(r"^(\d+\.\d+)\s+(close|fsync|fdatasync)\(\d+<([^>]+)>\)\s+=\s+(-?\d+).*<(\d+\.\d+)>$")
    grouped: dict[str, Any] = {}
    for path in sorted((artifact / "syscall-tail").glob("*")):
        if not path.is_file():
            continue
        tool = path.name.rsplit(".", 1)[0]
        state = grouped.setdefault(tool, {"records": [], "unresolved_calls": [], "other_fd_calls": 0})
        for line in path.read_text(errors="replace").splitlines():
            match = pattern.match(line)
            if match is None:
                if re.search(r"\b(close|fsync|fdatasync)\b", line):
                    state["unresolved_calls"].append({"file": path.name, "line": line})
                continue
            at, operation, filename, result, duration = match.groups()
            if not re.search(r"/\.perf-fio[^/]*/", filename):
                state["other_fd_calls"] += 1
                continue
            state["records"].append({"start_ns": int(Decimal(at) * 10**9), "duration_ns": int(Decimal(duration) * 10**9),
                                     "operation": operation, "file": filename, "result": int(result), "trace": path.name})
    for state in grouped.values():
        records = state["records"]
        state["status"] = "unavailable" if not records or state["unresolved_calls"] else "observed"
        state["operations"] = {name: latency_summary([r["duration_ns"] for r in records if r["operation"] == name])
                               for name in ("close", "fsync", "fdatasync")}
        state["completion_call_window_ns"] = (max(r["start_ns"] + r["duration_ns"] for r in records) - min(r["start_ns"] for r in records)) if records else None
        state["failed_calls"] = sum(r["result"] != 0 for r in records)
    return {"tools": grouped, "boundary": "application syscall entry to return, observed by strace; includes tracing overhead",
            "scope": "decoded .perf-fio dataset FDs only; excludes loader/proc closes",
            "limitation": "io_uring fsync requests may not issue fsync syscalls; no observed syscall is not proof of no fsync",
            "p99_requirement": "1000 independent client probe calls; standard fio completion samples alone are insufficient"}


def summarize(artifact: pathlib.Path) -> dict[str, Any]:
    paths = stats_paths(artifact)
    observations = [one_stats(path) for path in paths]
    result: dict[str, Any] = {
        "artifact": str(artifact.resolve()),
        "manifest": read_manifest(artifact),
        "stats": observations,
        "status": "unavailable",
        "baseline_accepted": False,
        "scope": "request observation validation; full WS-0 acceptance requires all independent correctness, environment and performance gates",
        "collection": collect_requests(artifact),
        "object_ledger": object_ledger(artifact),
        "syscall_completion": syscall_completion(artifact),
    }
    result["measurement_windows"] = measurement_windows(artifact, result["collection"]["instances"])
    invalid_markers = sorted(str(path.relative_to(artifact)) for path in artifact.glob("*.invalid"))
    if invalid_markers:
        result["status"] = "invalid"
        result["reason"] = "runner recorded invalid observations"
        result["invalid_markers"] = invalid_markers
        return result
    if result["manifest"] is None or "error" in result["manifest"]:
        result["reason"] = "missing or unreadable JSON manifest"
        return result
    if not paths:
        result["status"] = "unavailable"
        result["reason"] = "missing diagnostics/stats-*-after.txt"
    elif any(item["status"] == "invalid" for item in observations):
        result["status"] = "invalid"
    elif any(item["status"] == "unavailable" for item in observations):
        result["status"] = "unavailable"
    elif all(item["status"] == "disabled" for item in observations):
        result["status"] = "disabled"
    elif any(item["status"] == "snapshot" for item in observations):
        result["status"] = result["collection"]["status"]
    else:
        result["status"] = "valid"
    return result


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=pathlib.Path)
    parser.add_argument("-o", "--output", type=pathlib.Path)
    parser.add_argument("--format", choices=("json", "markdown"), default="json")
    parser.add_argument(
        "--require-enabled",
        action="store_true",
        help="fail when the artifact is a legitimate observation-off run",
    )
    return parser.parse_args(argv)


def latency_summary(values: list[int]) -> dict[str, Any]:
    """Exact nearest-rank percentiles; never average job percentiles."""
    ordered = sorted(values)
    return {"n": len(values), "unit": "ns", "p99_reliable": len(values) >= 1000,
            "percentiles": {str(p): ordered[max(0, math.ceil(len(ordered) * p / 100) - 1)] if ordered else None for p in (50, 95, 99, 99.9)},
            "histogram": sorted(Counter(values).items())}


def timed_call(samples: dict[str, list[int]], name: str, function: Any, *args: Any) -> Any:
    start = time.monotonic_ns()
    result = function(*args)
    end = time.monotonic_ns()
    samples.setdefault(name, []).append(end - start)
    return result


def client_io_probe(dataset: pathlib.Path, calls: int, fsync_every: int) -> dict[str, Any]:
    samples: dict[str, list[int]] = {}
    payload = bytes(range(256)) * 16
    path = dataset / "buffered-4k"
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    try:
        os.write(fd, payload * 256)
        os.fsync(fd)
    finally:
        os.close(fd)
    # Each close is an independent open/write/close invocation. These closes
    # can leave writeback debt; the caller must perform the same strict drain.
    logical_bytes = 0
    for _ in range(calls):
        fd = os.open(path, os.O_RDWR)
        try:
            written = timed_call(samples, "write", os.write, fd, payload)
            if written != len(payload):
                raise OSError("short write")
            logical_bytes += written
        finally:
            timed_call(samples, "close_dirty", os.close, fd)
    for _ in range(calls):
        fd = os.open(path, os.O_RDWR)
        try:
            for offset in range(fsync_every):
                written = timed_call(samples, "pwrite", os.pwrite, fd, payload, (offset % 256) * 4096)
                if written != len(payload):
                    raise OSError("short pwrite")
                logical_bytes += written
            timed_call(samples, "fsync", os.fsync, fd)
        finally:
            timed_call(samples, "close_after_fsync", os.close, fd)
    return {"buffered": True, "block_bytes": 4096, "fsync_every": fsync_every,
            "prefill_bytes": 256 * 4096, "logical_write_bytes": logical_bytes,
            "operations": {name: latency_summary(values) for name, values in samples.items()}}


def metadata_probe_worker(directory: str, worker: int, jobs: int, calls: int) -> dict[str, Any]:
    base = pathlib.Path(directory)
    samples: dict[str, list[int]] = {}
    for index in range(calls):
        # All workers use the same 256-file hot set; rename ownership is
        # disjoint so a concurrent rename cannot fabricate a negative hit.
        path = base / f"file-{(index * jobs + worker) % 256}"
        fd = timed_call(samples, "open", os.open, path, os.O_RDONLY)
        os.close(fd)
        timed_call(samples, "path_stat", os.stat, path)
        missing = base / f"missing-{worker}"
        start = time.monotonic_ns()
        try:
            os.stat(missing)
        except FileNotFoundError:
            samples.setdefault("negative", []).append(time.monotonic_ns() - start)
        else:
            raise RuntimeError("negative lookup unexpectedly found a file")
        renamed = base / f"renamed-{worker}"
        timed_call(samples, "rename", os.rename, path, renamed)
        os.rename(renamed, path)
    return {"worker": worker, "pid": os.getpid(), "samples": samples}


def client_metadata_probe(dataset: pathlib.Path, calls: int, jobs: int, depth: int) -> dict[str, Any]:
    base = dataset
    for index in range(depth):
        base = base / f"level-{index}"
    base.mkdir(parents=True)
    for index in range(256):
        path = base / f"file-{index}"
        path.write_bytes(bytes(4096))
        with path.open("rb") as handle:
            os.fsync(handle.fileno())
    # Full hot-set warmup, same mount. The directory preparation is outside
    # per-operation timers and remains part of this probe's wall time.
    for path in base.iterdir():
        with path.open("rb") as handle:
            handle.read()
        path.stat()
    with ProcessPoolExecutor(max_workers=jobs) as pool:
        futures = [pool.submit(metadata_probe_worker, str(base), worker, jobs, calls) for worker in range(jobs)]
        workers = [future.result() for future in futures]
    merged: dict[str, list[int]] = {}
    for worker in workers:
        for name, values in worker["samples"].items():
            merged.setdefault(name, []).extend(values)
        worker["operations"] = {name: latency_summary(values) for name, values in worker.pop("samples").items()}
    return {"jobs": jobs, "depth": depth, "hot_files": 256, "calls_per_worker_per_operation": calls,
            "workers": workers, "operations": {name: latency_summary(values) for name, values in merged.items()}}


def run_client_probe(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Buffered application syscall diagnostic; strict drain is required separately")
    parser.add_argument("mount", type=pathlib.Path)
    parser.add_argument("--kind", choices=("io", "metadata"), required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--calls", type=int)
    parser.add_argument("--fsync-every", choices=(1, 256), type=int, default=1)
    parser.add_argument("--jobs", choices=(1, 4, 16), type=int, default=1)
    parser.add_argument("--depth", choices=(1, 8), type=int, default=1)
    args = parser.parse_args(argv)
    if args.output.resolve().is_relative_to(args.mount.resolve()):
        parser.error("output must be outside the measured mount")
    calls = args.calls if args.calls is not None else (1000 if args.kind == "io" else 10000)
    if calls <= 0:
        parser.error("calls must be positive")
    dataset = pathlib.Path(tempfile.mkdtemp(prefix="ws0-client-", dir=args.mount))
    started = time.monotonic_ns()
    report: dict[str, Any] = {"argv": sys.argv, "dataset": str(dataset), "pid": os.getpid(),
                              "boundary": "application syscall entry to return; includes kernel and filesystem",
                              "clock": "CLOCK_MONOTONIC ns", "started_ns": started,
                              "strict_drain": "required from compose runner; not established by this probe",
                              "status": "invalid", "baseline_accepted": False}
    status = 0
    try:
        report["probe"] = client_io_probe(dataset, calls, args.fsync_every) if args.kind == "io" else client_metadata_probe(dataset, calls, args.jobs, args.depth)
        report["status"] = "diagnostic"
    except (OSError, RuntimeError) as exc:
        report["error"] = repr(exc)
        status = 1
    finally:
        report["ended_ns"] = time.monotonic_ns()
        report["wall_ns"] = report["ended_ns"] - started
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    return status


def main(argv: list[str]) -> int:
    if argv and argv[0] == "compose-run":
        return compose_run(argv[1:])
    if argv and argv[0] == "backend-snapshot":
        return backend_snapshot(argv[1:])
    if argv and argv[0] == "serve-otlp":
        return serve_otlp(argv[1:])
    if argv and argv[0] == "system-sample":
        return system_sample(argv[1:])
    if argv and argv[0] == "capture-mount":
        return capture_mount(argv[1:])
    if argv == ["protocol-env"]:
        import shlex
        print("\n".join(f"export {key}={shlex.quote(value)}" for key, value in PROTOCOL_ENV.items()))
        return 0
    if argv and argv[0] == "probe":
        return run_client_probe(argv[1:])
    args = parse_args(argv)
    if not args.artifact.is_dir():
        warn(f"missing artifact directory: {args.artifact}")
        return 2
    result = summarize(args.artifact)
    if args.require_enabled and any(item.get("status") == "disabled" for item in result["stats"]):
        result["status"] = "invalid"
        result["reason"] = "request observation is disabled"
    if args.format == "markdown":
        lines = [f"# Request Observation: `{result['status']}`", "", f"Artifact: `{result['artifact']}`", ""]
        for item in result["stats"]:
            lines.append(f"- `{pathlib.Path(item['path']).name}`: **{item['status']}**")
            if item.get("reason"):
                lines.append(f"  - {item['reason']}")
            if item.get("missing"):
                lines.append(f"  - missing: {', '.join(item['missing'])}")
        output = "\n".join(lines) + "\n"
    else:
        output = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(output)
    else:
        sys.stdout.write(output)
    return 0 if result["status"] in ("valid", "disabled") else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
