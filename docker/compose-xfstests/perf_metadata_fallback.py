#!/usr/bin/env python3
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


def env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


def emit(tool: str, op: str, ops: int, seconds: float, errors: int = 0, bytes_done: int = 0) -> None:
    ops_per_sec = ops / seconds if seconds > 0 else 0.0
    usec_per_op = (seconds * 1_000_000 / ops) if ops > 0 else 0.0
    print(f"{op}: ops/sec={ops_per_sec:.3f}, usec/op={usec_per_op:.3f}")
    print(
        "metadata_summary "
        f"tool={tool} op={op} ops={ops} errors={errors} bytes={bytes_done} "
        f"seconds={seconds:.6f} ops_per_sec={ops_per_sec:.3f} "
        f"usec_per_op={usec_per_op:.3f}"
    )


def timed(tool: str, op: str, func) -> None:
    start = time.perf_counter()
    ops, errors, bytes_done = func()
    emit(tool, op, ops, time.perf_counter() - start, errors, bytes_done)


def write_file(path: Path, size: int, seed: int = 0) -> int:
    data = bytes([(seed + i) & 0xFF for i in range(min(size, 8192))])
    remaining = size
    with path.open("wb") as fh:
        while remaining > 0:
            chunk = data[: min(len(data), remaining)]
            fh.write(chunk)
            remaining -= len(chunk)
    return size


def run_metaperf(root: Path) -> None:
    tool = "metaperf"
    count = env_int("PERF_METAPERF_OP_FILES", 200)
    size = env_int("PERF_METAPERF_FILE_SIZE", 4096)
    root.mkdir(parents=True, exist_ok=True)

    files = [root / f"meta-{idx:06d}" for idx in range(count)]

    def create():
        errors = 0
        bytes_done = 0
        for idx, path in enumerate(files):
            try:
                bytes_done += write_file(path, size, idx)
            except OSError:
                errors += 1
        return count, errors, bytes_done

    def open_read():
        errors = 0
        bytes_done = 0
        for path in files:
            try:
                with path.open("rb") as fh:
                    bytes_done += len(fh.read(1))
            except OSError:
                errors += 1
        return count, errors, bytes_done

    def stat():
        errors = 0
        for path in files:
            try:
                path.stat()
            except OSError:
                errors += 1
        return count, errors, 0

    def readdir():
        ops = max(1, count)
        errors = 0
        for _ in range(ops):
            try:
                list(root.iterdir())
            except OSError:
                errors += 1
        return ops, errors, 0

    def rename():
        errors = 0
        for path in files:
            tmp = path.with_suffix(".renamed")
            try:
                path.rename(tmp)
                tmp.rename(path)
            except OSError:
                errors += 1
        return count * 2, errors, 0

    timed(tool, "create", create)
    timed(tool, "open", open_read)
    timed(tool, "stat", stat)
    timed(tool, "readdir", readdir)
    timed(tool, "rename", rename)


def run_dirperf(root: Path) -> None:
    tool = "dirperf"
    first = env_int("PERF_DIRPERF_FIRST", 100)
    last = env_int("PERF_DIRPERF_LAST", 1000)
    step = max(1, env_int("PERF_DIRPERF_ADDSTEP", 100))
    dirs = max(1, env_int("PERF_DIRPERF_DIRS", 2))
    size = env_int("PERF_DIRPERF_FILE_SIZE", 128)
    total = max(first, last)
    root.mkdir(parents=True, exist_ok=True)
    subdirs = [root / f"dir-{idx:03d}" for idx in range(dirs)]
    for d in subdirs:
        d.mkdir(parents=True, exist_ok=True)
    files = [subdirs[idx % dirs] / f"file-{idx:06d}" for idx in range(total)]

    def create():
        errors = 0
        bytes_done = 0
        for idx, path in enumerate(files):
            try:
                bytes_done += write_file(path, size, idx)
            except OSError:
                errors += 1
        return total, errors, bytes_done

    def stat_windows():
        ops = 0
        errors = 0
        for end in range(first, total + 1, step):
            for path in files[:end]:
                try:
                    path.stat()
                except OSError:
                    errors += 1
                ops += 1
        return ops, errors, 0

    def readdir_windows():
        ops = 0
        errors = 0
        for _end in range(first, total + 1, step):
            for d in subdirs:
                try:
                    list(d.iterdir())
                except OSError:
                    errors += 1
                ops += 1
        return ops, errors, 0

    timed(tool, "dirperf_create", create)
    timed(tool, "dirperf_stat", stat_windows)
    timed(tool, "dirperf_readdir", readdir_windows)


def run_dirstress(root: Path) -> None:
    tool = "dirstress"
    procs = max(1, env_int("PERF_DIRSTRESS_PROCS", 4))
    files = max(1, env_int("PERF_DIRSTRESS_FILES", 200))
    root.mkdir(parents=True, exist_ok=True)

    def worker(worker_id: int):
        work = root / f"worker-{worker_id:03d}"
        work.mkdir(parents=True, exist_ok=True)
        ops = 0
        errors = 0
        for idx in range(files):
            path = work / f"file-{idx:06d}"
            renamed = work / f"renamed-{idx:06d}"
            try:
                write_file(path, 128, idx)
                ops += 1
                path.stat()
                ops += 1
                path.rename(renamed)
                ops += 1
                renamed.unlink()
                ops += 1
            except OSError:
                errors += 1
        return ops, errors

    start = time.perf_counter()
    ops = 0
    errors = 0
    with ThreadPoolExecutor(max_workers=procs) as pool:
        for worker_ops, worker_errors in pool.map(worker, range(procs)):
            ops += worker_ops
            errors += worker_errors
    emit(tool, "dirstress_ops", ops, time.perf_counter() - start, errors, 0)


def run_looptest(root: Path) -> None:
    tool = "looptest"
    iters = max(1, env_int("PERF_LOOPTEST_ITERS", 200))
    buf_size = max(1, env_int("PERF_LOOPTEST_BUF_SIZE", 1_048_576))
    root.mkdir(parents=True, exist_ok=True)
    path = root / "looptest.dat"
    buf = b"L" * min(buf_size, 1024 * 1024)

    def loop_ops():
        ops = 0
        errors = 0
        bytes_done = 0
        for _ in range(iters):
            try:
                with path.open("wb") as fh:
                    remaining = buf_size
                    while remaining > 0:
                        chunk = buf[: min(len(buf), remaining)]
                        fh.write(chunk)
                        remaining -= len(chunk)
                        bytes_done += len(chunk)
                with path.open("rb") as fh:
                    while fh.read(len(buf)):
                        pass
                with path.open("r+b") as fh:
                    fh.truncate(buf_size // 2)
                path.unlink()
                ops += 4
            except OSError:
                errors += 1
        return ops, errors, bytes_done

    timed(tool, "looptest_ops", loop_ops)


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: perf_metadata_fallback.py <tool> <work_dir>", file=sys.stderr)
        return 2
    tool = sys.argv[1]
    root = Path(sys.argv[2])
    if tool == "metaperf":
        run_metaperf(root)
    elif tool == "dirperf":
        run_dirperf(root)
    elif tool == "dirstress":
        run_dirstress(root)
    elif tool == "looptest":
        run_looptest(root)
    else:
        print(f"unknown metadata fallback tool: {tool}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
