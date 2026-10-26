#!/usr/bin/env python3
"""Measure an existing, verified FUSE mount; count kernel requests with BPF.

Run on the SAME binary/configuration at TTL 0 and 1000. Requires root, bpftrace,
Linux BTF, and an idle compiler. Output is raw JSONL, three repetitions by
default. This focused local diagnostic is not the compose acceptance suite.
"""
import argparse
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import signal
import subprocess
import time


def checked_mount(path):
    subprocess.run(["mountpoint", "-q", str(path)], check=True)
    rows = subprocess.check_output(["ps", "-C", "rustc", "-o", "stat="], text=True) if subprocess.run(
        ["pgrep", "-x", "rustc"], stdout=subprocess.DEVNULL).returncode == 0 else ""
    if any(not row.strip().startswith("Z") for row in rows.splitlines()):
        raise RuntimeError("compiler running; refusing contaminated measurement")


class RequestCounts:
    def __init__(self, mount, output):
        self.mount, self.output = mount, output

    def __enter__(self):
        checked_mount(self.mount)
        dev = os.stat(self.mount).st_dev
        kernel_dev = (os.major(dev) << 20) | os.minor(dev)
        pid = os.getpid()
        program = f'''
BEGIN {{ printf("ready\\n"); }}
tracepoint:syscalls:sys_enter_getpgid /pid == {pid}/ {{
    if (args->pid == 0) {{ @active = 1; }}
    if (args->pid == {pid}) {{ @active = 0; }}
}}
kprobe:fuse_request_end /@active/ {{
    $r = (struct fuse_req *)arg0;
    if ($r->fm->sb->s_dev == {kernel_dev}) {{
        @requests[$r->in.h.opcode] = count();
        if ($r->in.h.opcode == 15) {{
            $read = (struct fuse_read_in *)$r->args->in_args[0].value;
            @read_sizes[$read->size] = count();
        }}
    }}
}}
tracepoint:syscalls:sys_enter_read /@active && pid == {pid}/ {{
    @read_requested[args->count] = count();
}}
tracepoint:syscalls:sys_exit_read /@active && pid == {pid}/ {{
    @read_returned[args->ret] = count();
}}
kprobe:fuse_finish_open /@active/ {{
    $inode = (struct inode *)arg0;
    if ($inode->i_sb->s_dev == {kernel_dev}) {{
        $file = (struct file *)arg1;
        $ff = (struct fuse_file *)$file->private_data;
        @open_flags[$ff->open_flags] = count();
    }}
}}
END {{
    print(@requests); print(@read_sizes); print(@read_requested);
    print(@read_returned); print(@open_flags);
    clear(@requests); clear(@read_sizes); clear(@read_requested);
    clear(@read_returned); clear(@open_flags); clear(@active);
}}
'''
        self.output.with_suffix(".bt").write_text(program)
        self.log = self.output.open("w")
        self.proc = subprocess.Popen(["bpftrace", "-q", "-f", "json", "-B", "line", "-e", program],
                                     stdout=self.log, stderr=self.log)
        deadline = time.monotonic() + 20
        while "ready" not in self.output.read_text():
            if self.proc.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError(f"BPF failed: {self.output.read_text()}")
            time.sleep(.05)
        return self

    def start(self):
        checked_mount(self.mount)
        os.getpgid(0)

    def stop(self):
        os.getpgid(os.getpid())

    def __exit__(self, *exc):
        self.stop()
        self.proc.send_signal(signal.SIGINT)
        returncode = self.proc.wait(timeout=20)
        self.log.close()
        if returncode != 0:
            raise RuntimeError(f"BPF exited {returncode}: {self.output.read_text()}")
        self.maps = {}
        for line in self.output.read_text().splitlines():
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if item.get("type") == "map":
                self.maps.update(item.get("data", {}))
        self.counts = self.maps.get("@requests", {})
        checked_mount(self.mount)


def verify_contents(path, expected_digest, expected_bytes):
    """Verify the entire multi-object corpus outside the throughput window."""
    digest = hashlib.sha256()
    total = 0
    with path.open("rb", buffering=0) as file:
        while chunk := file.read(1024 * 1024):
            digest.update(chunk)
            total += len(chunk)
    assert total == expected_bytes, "read byte count differs from written byte count"
    assert digest.hexdigest() == expected_digest, "read content differs from written content"
    return dict(bytes=total, sha256=digest.hexdigest())


def stat_work(args):
    paths, operations = args
    for i in range(operations):
        os.stat(paths[i % len(paths)])
    return operations


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mount", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--label", required=True)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--corpus", type=int, default=256)
    parser.add_argument("--stat-operations", type=int, default=8192)
    parser.add_argument("--create-count", type=int, default=256)
    parser.add_argument("--size-mib", type=int, default=256)
    parser.add_argument("--reads-only", action="store_true",
                        help="Measure reads first, skipping metadata and write timings")
    parser.add_argument("--read-diagnostics", action="store_true",
                        help="Also measure reopen and page-cache-evicted reads and verify SHA256")
    args = parser.parse_args()
    mount, out = args.mount.resolve(), args.output.resolve()
    checked_mount(mount)
    out.mkdir(parents=True, exist_ok=True)
    work = mount / "metadata-cache-measurement"
    work.mkdir(exist_ok=True)
    paths = [str(work / f"stat-{i}") for i in range(args.corpus)]
    if not args.reads_only:
        for path in paths:
            Path(path).touch(exist_ok=True)
    buffer = os.urandom(1024 * 1024)
    read_path = work / "read-corpus"
    written_hash = hashlib.sha256()
    with read_path.open("wb", buffering=0) as file:
        for index in range(args.size_mib):
            payload = index.to_bytes(8, "little") + buffer[8:]
            assert file.write(payload) == len(payload)
            written_hash.update(payload)
        os.fsync(file.fileno())
    (out / "mountinfo.txt").write_text("\n".join(line for line in Path("/proc/self/mountinfo").read_text().splitlines()
                                               if line.split()[4] == str(mount)) + "\n")
    def measure(name, repetition, operations, action, warm=lambda: None, **extra):
        with RequestCounts(mount, out / f"{name}-{repetition}.bpf.jsonl") as trace:
            warm()
            trace.start()
            started = time.perf_counter()
            action()
            seconds = time.perf_counter() - started
            trace.stop()
        counts = {str(key): value for key, value in trace.counts.items()}
        row = dict(label=args.label, operation=name, repetition=repetition, seconds=seconds,
                   operations=operations, throughput=operations / seconds, fuse_requests=counts,
                   trace_maps=trace.maps,
                   lookup_per_op=counts.get("1", 0) / operations,
                   getattr_per_op=counts.get("3", 0) / operations,
                   corpus=args.corpus, **extra)
        with (out / "measurements.jsonl").open("a") as file:
            file.write(json.dumps(row) + "\n")
        print(json.dumps(row), flush=True)
    with multiprocessing.get_context("fork").Pool(8) as pool:
        for rep in range(1, args.repetitions + 1):
            for processes in (() if args.reads_only else (1, 8)):
                task = (paths, args.stat_operations)
                action = (lambda: stat_work(task)) if processes == 1 else (lambda: pool.map(stat_work, [task] * 8))
                measure(f"stat-{processes}p", rep, args.stat_operations * processes, action,
                        warm=lambda: stat_work((paths, len(paths))),
                        kernel_metadata_cache_eligible=args.label != "off", processes=processes, unit="ops/s")
            names = [work / f"create-{i}" for i in range(args.create_count)]
            def create():
                for path in names:
                    os.close(os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600))
            def unlink():
                for path in names:
                    path.unlink()
            if not args.reads_only:
                create()
                unlink()
                measure("create", rep, len(names), create, unit="ops/s", kernel_can_complete_operation=False)
                measure("unlink", rep, len(names), unlink, unit="ops/s", kernel_can_complete_operation=False)
            write_path = work / "write-corpus"
            def write():
                with write_path.open("wb", buffering=0) as file:
                    for _ in range(args.size_mib):
                        assert file.write(buffer) == len(buffer)
                    os.fsync(file.fileno())
            if not args.reads_only:
                measure("seq_write_MBps", rep, args.size_mib * len(buffer) / 1e6, write,
                        unit="MB/s", direct=0, kernel_page_cache_eligible=True, timing="open+write+fsync+close")
            with read_path.open("rb", buffering=0) as file:
                def read():
                    total = 0
                    while chunk := file.read(len(buffer)):
                        total += len(chunk)
                    assert total == args.size_mib * len(buffer)
                def warm():
                    file.seek(0)
                    read()
                    file.seek(0)
                measure("seq_read_warm_MBps", rep, args.size_mib * len(buffer) / 1e6, read, warm=warm,
                        unit="MB/s", direct=0, kernel_page_cache_eligible=True, timing="read syscalls on same warmed fd")
            if args.read_diagnostics:
                def reopen_read():
                    with read_path.open("rb", buffering=0) as stream:
                        total = 0
                        while chunk := stream.read(len(buffer)):
                            total += len(chunk)
                        assert total == args.size_mib * len(buffer)
                def evict():
                    with read_path.open("rb", buffering=0) as stream:
                        os.posix_fadvise(stream.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
                measure("seq_read_reopen_MBps", rep, args.size_mib * len(buffer) / 1e6,
                        reopen_read, unit="MB/s", direct=0, timing="open+read+close; no eviction")
                measure("seq_read_evicted_MBps", rep, args.size_mib * len(buffer) / 1e6,
                        reopen_read, warm=evict, unit="MB/s", direct=0,
                        timing="open+read+close; POSIX_FADV_DONTNEED; daemon caches retained")
                verification = verify_contents(read_path, written_hash.hexdigest(), args.size_mib * len(buffer))
                with (out / "verification.jsonl").open("a") as file:
                    file.write(json.dumps(dict(repetition=rep, **verification)) + "\n")
    for path in work.iterdir():
        path.unlink()
    work.rmdir()


if __name__ == "__main__":
    if not __debug__:
        raise SystemExit("Python assertions must be enabled")
    main()
