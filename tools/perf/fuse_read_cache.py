#!/usr/bin/env python3
"""Supplemental read-cache benchmark, separate from /home/hxy/bench2.py.

Uses 256 MiB files and three repetitions by default. Cold means the Linux page
cache was evicted with fsync + POSIX_FADV_DONTNEED and verified with mincore.
Filesystem and object-store caches remain warm. Sequential read timings exclude
open, close, and mincore; each warm read reopens the file. fio uses 1 MiB sync
reads, one job, ten seconds, and invalidate=0. Buffered fio is seeded before
opening. The optional mixed-I/O guard uses 4 KiB blocks and 70% reads.

Requires Linux, fio, mountpoint, pgrep, and a mounted BrewFS or JuiceFS volume.
Run after builds finish, with per-operation logging and tracing disabled. Supply
the regular-file FUSE reply flags observed on a separate diagnostic mount using
--fopen-flags; this script records those flags but does not trace them itself.
Use --cold-fopen-flags when the first read open differs from later opens (for
example, JuiceFS may reply 0 first and KEEP_CACHE=2 on subsequent opens).
All rates use MiB/s. Application O_DIRECT still bypasses the page cache when
the FUSE reply contains KEEP_CACHE. Each operation samples /proc for Cargo/rustc
every 100 ms, in addition to endpoint checks. Detected compiler overlap rejects
the operation, preserves evidence, and stops the run without an automatic retry.
Selected GETATTR/OPEN/READ deltas include the .stats observer itself; immediate
stats-only calibration samples are recorded separately. Zero data READs do not
imply zero FUSE requests. No observer overhead is silently subtracted.
"""

import argparse
import ctypes
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import threading
import time
import uuid


if not __debug__:
    raise SystemExit(
        "Refusing optimized Python: all benchmark validity assertions are required"
    )

MIB = 1024 * 1024
PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")
LIBC = ctypes.CDLL(None, use_errno=True)
LIBC.mmap.restype = ctypes.c_void_p
LIBC.mmap.argtypes = [
    ctypes.c_void_p,
    ctypes.c_size_t,
    ctypes.c_int,
    ctypes.c_int,
    ctypes.c_int,
    ctypes.c_longlong,
]
LIBC.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p]
LIBC.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]


def compiler_processes():
    found = []
    with os.scandir("/proc") as entries:
        for entry in entries:
            if not entry.name.isdecimal():
                continue
            try:
                stat = (Path(entry.path) / "stat").read_text()
            except (FileNotFoundError, ProcessLookupError):
                continue
            end = stat.rfind(")")
            name = stat[stat.find("(") + 1 : end]
            if name not in ("cargo", "rustc"):
                continue
            fields = stat[end + 1 :].split()
            found.append(
                {
                    "pid": int(entry.name),
                    "name": name,
                    "state": fields[0],
                    "start_ticks": int(fields[19]),
                }
            )
    return found


class CompilerOverlap(RuntimeError):
    """A timed operation cannot be accepted because compilation overlapped it."""


class CompilerWatch:
    """Watch one operation, including setup and teardown, without subprocesses."""

    interval = 0.1

    def __init__(self):
        self.stop = threading.Event()
        self.thread = None
        self.processes = {}
        self.errors = []
        self.samples = 0
        self.last_sample = None
        self.max_sample_interval = 0.0

    def sample(self):
        now = time.monotonic()
        if self.last_sample is not None:
            self.max_sample_interval = max(
                self.max_sample_interval, now - self.last_sample
            )
        self.last_sample = now
        self.samples += 1
        for process in compiler_processes():
            identity = (process["pid"], process["start_ticks"])
            if identity not in self.processes:
                self.processes[identity] = {
                    **process,
                    "first_seen_unix_seconds": time.time(),
                }

    def run(self):
        while not self.stop.wait(self.interval):
            try:
                self.sample()
            except Exception as error:
                self.errors.append(repr(error))
                break

    def report(self):
        return {
            "interval_seconds": self.interval,
            "samples": self.samples,
            "max_sample_interval_seconds": self.max_sample_interval,
            "compiler_processes": list(self.processes.values()),
            "monitor_errors": self.errors,
        }

    def check(self):
        if self.processes or self.errors:
            raise CompilerOverlap(
                "Rejecting operation: compiler overlap or monitor failure: "
                + json.dumps(self.report())
            )

    def __enter__(self):
        self.sample()
        self.check()
        self.thread = threading.Thread(target=self.run, name="compiler-watch")
        self.thread.start()
        return self

    def __exit__(self, exception_type, exception, traceback):
        self.stop.set()
        self.thread.join()
        try:
            self.sample()
        except Exception as error:
            self.errors.append(repr(error))
        # Reject overlap even when the operation also failed a later endpoint
        # check. The caller retains both the original exception and this report.
        self.check()
        return False


def residency(fd, size):
    # PROT_READ, MAP_SHARED. Mapping and mincore do not fault file pages in.
    address = LIBC.mmap(None, size, 1, 1, fd, 0)
    if address == ctypes.c_void_p(-1).value:
        raise OSError(ctypes.get_errno(), "mmap for mincore failed")
    pages = (size + PAGE_SIZE - 1) // PAGE_SIZE
    vector = (ctypes.c_ubyte * pages)()
    try:
        if LIBC.mincore(address, size, vector):
            raise OSError(ctypes.get_errno(), "mincore failed")
        return {
            "resident_pages": sum(bool(value & 1) for value in vector),
            "total_pages": pages,
        }
    finally:
        LIBC.munmap(address, size)


def assert_ready(mount):
    subprocess.run(["mountpoint", "-q", str(mount)], check=True)
    for process in ("cargo", "rustc"):
        check = subprocess.run(
            ["pgrep", "-x", process], capture_output=True, text=True
        )
        assert check.returncode == 1, (
            f"{process} exclusion check failed: {check.stdout}{check.stderr}"
        )


def read_counter(mount):
    values = {}
    for line in (mount / ".stats").read_text().splitlines():
        fields = line.split()
        if len(fields) == 2 and not line.startswith("#"):
            values[fields[0]] = float(fields[1])
    for name in (
        "brewfs_fuse_read_ops_total",
        "juicefs_fuse_read_size_bytes_total",
    ):
        if name in values:
            # JuiceFS fuse_ops_total_read includes .stats reads. Its read-size
            # histogram counts regular data reads and avoids that observer cost.
            prefix = "brewfs" if name.startswith("brewfs_") else "juicefs"
            metadata_names = (
                {
                    "getattr": "brewfs_fuse_getattr_ops_total",
                    "open": "brewfs_fuse_open_ops_total",
                }
                if prefix == "brewfs"
                else {
                    "getattr": "juicefs_fuse_ops_total_getattr",
                    "open": "juicefs_fuse_ops_total_open",
                }
            )
            return {
                "counter": name,
                "count": int(values[name]),
                "opcode_counter_names": {"read": name, **metadata_names},
                "opcode_counts": {
                    "read": int(values[name]),
                    **{
                        operation: int(values[counter])
                        for operation, counter in metadata_names.items()
                        if counter in values
                    },
                },
            }
    raise AssertionError("No regular-file FUSE READ counter found")


def opcode_deltas(before, after):
    return {
        operation: after["opcode_counts"][operation] - count
        for operation, count in before["opcode_counts"].items()
        if operation in after["opcode_counts"]
    }


def calibrate_stats_observer(mount):
    assert_ready(mount)
    before = read_counter(mount)
    deltas = []
    for _ in range(5):
        after = read_counter(mount)
        deltas.append(opcode_deltas(before, after))
        before = after
    return {
        "description": (
            "Five immediate stats-only snapshot deltas; no data-file operation. "
            "Metadata TTL expiry can change observer overhead over longer gaps. "
            "Operation deltas remain raw, without subtraction."
        ),
        "counter_names": before["opcode_counter_names"],
        "snapshot_deltas": deltas,
    }


def drain_read(fd, size):
    count = 0
    while count < size:
        data = os.read(fd, min(MIB, size - count))
        assert data, f"short read: {count}/{size}"
        count += len(data)
    return count


def measure_seq(mount, path, size, cold):
    assert_ready(mount)
    before = read_counter(mount)
    open_started_ns = time.monotonic_ns()
    fd = os.open(path, os.O_RDONLY)
    open_finished_ns = time.monotonic_ns()
    try:
        if cold:
            os.fsync(fd)
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        start_residency = residency(fd, size)
        if cold:
            assert start_residency["resident_pages"] == 0, start_residency
        assert_ready(mount)
        read_started_ns = time.monotonic_ns()
        start = time.perf_counter()
        count = drain_read(fd, size)
        elapsed = time.perf_counter() - start
        read_finished_ns = time.monotonic_ns()
        end_residency = residency(fd, size)
    finally:
        os.close(fd)
    assert_ready(mount)
    after = read_counter(mount)
    return {
        "operation": "seq_read_cold" if cold else "seq_read_warm",
        "bytes": count,
        "seconds": elapsed,
        "open_monotonic_start_ns": open_started_ns,
        "open_monotonic_end_ns": open_finished_ns,
        "read_monotonic_start_ns": read_started_ns,
        "read_monotonic_end_ns": read_finished_ns,
        "MiBps": count / elapsed / MIB,
        "kernel_page_cache_eligible": True,
        "residency_before_read": start_residency,
        "residency_after_read": end_residency,
        "fuse_read_counter": before["counter"],
        "fuse_read_delta": after["count"] - before["count"],
        "fuse_opcode_deltas_including_observer": opcode_deltas(before, after),
    }


def measure_fio(mount, path, size, direct, randrw, output, runtime):
    assert_ready(mount)
    if not direct:
        fd = os.open(path, os.O_RDONLY)
        try:
            drain_read(fd, size)
        finally:
            os.close(fd)
    before = read_counter(mount)
    scene = "fio_randrw" if randrw else f"fio_seqread_direct{direct}"
    argv = [
        "fio",
        f"--name={scene}",
        f"--filename={path}",
        f"--size={size}",
        "--ioengine=sync",
        "--iodepth=1",
        "--numjobs=1",
        f"--direct={direct}",
        "--invalidate=0",
        "--time_based=1",
        f"--runtime={runtime}",
        "--group_reporting=1",
        "--output-format=json",
        f"--output={output}",
        "--rw=randrw" if randrw else "--rw=read",
        "--bs=4k" if randrw else "--bs=1m",
    ]
    if randrw:
        argv += [
            "--rwmixread=70",
            "--randrepeat=1",
            "--randseed=47",
            "--end_fsync=1",
        ]
    assert_ready(mount)
    fio_started_ns = time.monotonic_ns()
    start = time.perf_counter()
    subprocess.run(
        argv, check=True, timeout=runtime + 180, stdout=subprocess.DEVNULL
    )
    elapsed = time.perf_counter() - start
    fio_finished_ns = time.monotonic_ns()
    assert_ready(mount)
    after = read_counter(mount)
    job = json.loads(output.read_text())["jobs"][0]
    assert job["error"] == 0, job["error"]
    count = job["read"]["io_bytes"] + job["write"]["io_bytes"]
    return {
        "operation": scene,
        "argv": argv,
        "bytes": count,
        "wall_seconds": elapsed,
        "fio_monotonic_start_ns": fio_started_ns,
        "fio_monotonic_end_ns": fio_finished_ns,
        "MiBps": (job["read"]["bw_bytes"] + job["write"]["bw_bytes"]) / MIB,
        "effective_wall_MiBps": count / elapsed / MIB,
        "read_MiBps": job["read"]["bw_bytes"] / MIB,
        "write_MiBps": job["write"]["bw_bytes"] / MIB,
        "kernel_page_cache_eligible": not bool(direct),
        "seeded_before_open": not bool(direct),
        "fuse_read_counter": before["counter"],
        "fuse_read_delta": after["count"] - before["count"],
        "fuse_opcode_deltas_including_observer": opcode_deltas(before, after),
    }


def positive_integer(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def run_operation_batch(operations, record_success, record_failure, collect_guard_failure):
    """Collect only failed fio randrw exits when explicitly requested.

    Read failures, compiler overlap, monitoring errors, and non-fio failures
    remain fatal. Every failed guard is reported and contributes to final failure.
    """
    failed_guards = 0
    for name, operation in operations:
        watch = CompilerWatch()
        row = None
        started_ns = time.monotonic_ns()
        try:
            with watch:
                row = operation()
        except BaseException as error:
            record_failure(
                {
                    "operation": name,
                    "operation_monotonic_start_ns": started_ns,
                    "operation_monotonic_end_ns": time.monotonic_ns(),
                    "status": "rejected",
                    "error": repr(error),
                    "compiler_watch": watch.report(),
                    "unaccepted_result": row,
                }
            )
            fio_guard_exit = (
                name == "fio_randrw"
                and isinstance(error, subprocess.CalledProcessError)
                and isinstance(error.cmd, (list, tuple))
                and error.cmd
                and Path(error.cmd[0]).name == "fio"
            )
            if not (collect_guard_failure and fio_guard_exit):
                raise
            failed_guards += 1
            continue
        row.update(
            {
                "operation_monotonic_start_ns": started_ns,
                "operation_monotonic_end_ns": time.monotonic_ns(),
                "compiler_watch": watch.report(),
            }
        )
        record_success(name, row)
    return failed_guards


def fail_if_guards_failed(failed_guards):
    if failed_guards:
        print(
            f"FAILED: {failed_guards} mixed-I/O guard(s); read medians are preserved, "
            "but this run did not pass its guard.",
            file=sys.stderr,
        )
        raise SystemExit(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mount", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--fopen-flags", type=lambda value: int(value, 0), required=True)
    parser.add_argument(
        "--cold-fopen-flags",
        type=lambda value: int(value, 0),
        help="reply flags for the first cold-read open; defaults to --fopen-flags",
    )
    parser.add_argument("--size-mib", type=positive_integer, default=256)
    parser.add_argument("--repeats", type=positive_integer, default=3)
    parser.add_argument("--fio-runtime", type=positive_integer, default=10)
    parser.add_argument("--no-randrw", action="store_true")
    parser.add_argument(
        "--collect-after-guard-failure",
        action="store_true",
        help="collect later repetitions after fio randrw exits unsuccessfully; still exit nonzero",
    )
    args = parser.parse_args()
    if args.no_randrw and args.collect_after_guard_failure:
        parser.error("--collect-after-guard-failure requires the mixed-I/O guard")
    cold_flags = (
        args.fopen_flags if args.cold_fopen_flags is None else args.cold_fopen_flags
    )
    if (args.fopen_flags | cold_flags) & 1:
        parser.error("this mincore probe requires buffered FUSE handles (DIRECT_IO=0)")
    mount = args.mount.resolve()
    assert_ready(mount)
    args.output.mkdir(parents=True, exist_ok=False)
    device = os.stat(mount).st_dev
    bdi = f"{os.major(device)}:{os.minor(device)}"
    readahead = int((Path("/sys/class/bdi") / bdi / "read_ahead_kb").read_text())
    metadata = {
        "label": args.label,
        "protocol": __doc__,
        "mount": str(mount),
        "bdi": bdi,
        "read_ahead_kb": readahead,
        "fopen_flags": args.fopen_flags,
        "cold_fopen_flags": cold_flags,
        "fopen_flags_source": "stage-matched separate diagnostic mount",
        "collect_after_guard_failure": args.collect_after_guard_failure,
        "stats_observer_calibration": calibrate_stats_observer(mount),
        "size_mib": args.size_mib,
        "fio_version": subprocess.check_output(["fio", "--version"], text=True).strip(),
        "mountinfo": [
            line
            for line in Path("/proc/self/mountinfo").read_text().splitlines()
            if line.split()[4] == str(mount)
        ],
    }
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    rows = []
    failed_guards = 0
    size = args.size_mib * MIB
    block = os.urandom(MIB)
    for repetition in range(1, args.repeats + 1):
        path = mount / ("readcache-" + uuid.uuid4().hex)
        assert_ready(mount)
        try:
            with path.open("xb", buffering=0) as file:
                for _ in range(args.size_mib):
                    assert file.write(block) == len(block)
                os.fsync(file.fileno())
                inode = os.fstat(file.fileno()).st_ino
            operations = [
                ("seq_read_cold", lambda: measure_seq(mount, path, size, True)),
                ("seq_read_warm", lambda: measure_seq(mount, path, size, False)),
            ]
            for direct in (0, 1):
                operations.append(
                    (
                        f"fio_seqread_direct{direct}",
                        lambda direct=direct: measure_fio(
                            mount,
                            path,
                            size,
                            direct,
                            False,
                            args.output / f"fio-seqread-direct{direct}-{repetition}.json",
                            args.fio_runtime,
                        ),
                    )
                )
            if not args.no_randrw:
                operations.append(
                    (
                        "fio_randrw",
                        lambda: measure_fio(
                            mount,
                            path,
                            size,
                            0,
                            True,
                            args.output / f"fio-randrw-{repetition}.json",
                            args.fio_runtime,
                        ),
                    )
                )
            def record_failure(rejected):
                rejected.update(
                    {"label": args.label, "repetition": repetition, "inode": inode}
                )
                with (args.output / "rejected-measurements.jsonl").open("a") as file:
                    file.write(json.dumps(rejected) + "\n")
                    file.flush()
                    os.fsync(file.fileno())

            def record_success(name, row):
                row.update(
                    {
                        "label": args.label,
                        "inode": inode,
                        "benchmark_pid": os.getpid(),
                        "repetition": repetition,
                        "read_ahead_kb": readahead,
                        "fopen_flags": (
                            cold_flags if name == "seq_read_cold" else args.fopen_flags
                        ),
                    }
                )
                rows.append(row)
                with (args.output / "measurements.jsonl").open("a") as file:
                    file.write(json.dumps(row) + "\n")
                    file.flush()
                    os.fsync(file.fileno())
                print(json.dumps(row), flush=True)
            failed_guards += run_operation_batch(
                operations,
                record_success,
                record_failure,
                args.collect_after_guard_failure,
            )
        finally:
            # Cleanup must still run after a compiler-overlap rejection.
            subprocess.run(["mountpoint", "-q", str(mount)], check=True)
            path.unlink(missing_ok=True)
    summary = {
        operation: statistics.median(
            row["MiBps"] for row in rows if row["operation"] == operation
        )
        for operation in sorted({row["operation"] for row in rows})
        if sum(row["operation"] == operation for row in rows) == args.repeats
    }
    (args.output / "medians.json").write_text(json.dumps(summary, indent=2) + "\n")
    (args.output / "result-status.json").write_text(
        json.dumps(
            {
                "status": "failed" if failed_guards else "passed",
                "failed_mixed_io_guards": failed_guards,
                "repetitions_attempted": args.repeats,
                "medians_require_complete_repetitions": True,
            },
            indent=2,
        )
        + "\n"
    )
    print("MEDIANS", json.dumps(summary), flush=True)
    fail_if_guards_failed(failed_guards)


if __name__ == "__main__":
    main()
