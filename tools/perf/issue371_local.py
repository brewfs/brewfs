#!/usr/bin/env python3
"""Local workspace-v1 native/packed lower write comparison for issue #371.

Requires a local Redis server, /dev/fuse, fio, a workspace-overlay release
binary and packed_v3_snapshot_fixture. This is diagnosis, not compose/EC
performance acceptance. Both arms overwrite files created in their upper.
Each cell has a new catalog namespace, object directory, cache and mount.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import random
import statistics
import subprocess
import threading
import time


def command(argv, *, output=None, env=None, timeout=300, check=True):
    result = subprocess.run(argv, env=env, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, timeout=timeout, check=False)
    if output:
        Path(output).write_bytes(result.stdout)
    if result.returncode and check:
        raise RuntimeError(f"exit {result.returncode}: {argv!r}\n{result.stdout[-4000:]!r}")
    return result.stdout.decode(errors="replace")


def stats(mount, destination):
    text = (mount / ".stats").read_bytes().replace(b"\0", b"").decode()
    destination.write_text(text)
    values = {}
    for line in text.splitlines():
        if not line.startswith("#") and len(line.split()) == 2:
            key, value = line.split()
            try:
                values[key] = float(value)
            except ValueError:
                pass
    return values


def pwrite_probe(args, mount, directory):
    """Open before the timed interval; capture counters before any close/fsync."""
    fds = [os.open(mount / f"upper-{0 if args.shared_file else job}.bin", os.O_RDWR)
           for job in range(args.jobs)]
    barrier = threading.Barrier(args.jobs)

    def worker(job):
        rng = random.Random(371 + job)
        data = bytes((value + 17 + job) % 256 for value in range(256)) * 16
        latencies = []
        errors = []
        offsets = []
        barrier.wait()
        for _ in range(args.pwrite_count):
            offset = rng.randrange(args.size_mib * 256) * 4096
            start = time.perf_counter_ns()
            try:
                count = os.pwrite(fds[job], data, offset)
                if count != len(data):
                    raise RuntimeError(f"short pwrite: {count}")
            except (OSError, RuntimeError) as error:
                errors.append(repr(error))
                break
            latencies.append(time.perf_counter_ns() - start)
            offsets.append(offset)
        return {"latencies_ns": latencies, "errors": errors, "offsets": offsets}

    before = stats(mount, directory / "pwrite-before.stats")
    epoch_start_ns = time.time_ns()
    start = time.monotonic()
    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        workers = list(pool.map(worker, range(args.jobs)))
    active_s = time.monotonic() - start
    epoch_end_ns = time.time_ns()
    after = stats(mount, directory / "pwrite-after.stats")
    sync_times = []
    sync_errors = []
    for fd in fds:
        start = time.perf_counter_ns()
        try:
            os.fsync(fd)
        except OSError as error:
            sync_errors.append(repr(error))
        sync_times.append(time.perf_counter_ns() - start)
    stats(mount, directory / "pwrite-after-fsync.stats")
    for fd in fds:
        os.close(fd)
    latencies = sorted(value for worker in workers for value in worker["latencies_ns"])
    result = {
        "workers": workers, "active_s": active_s, "count": len(latencies),
        "epoch_start_ns": epoch_start_ns, "epoch_end_ns": epoch_end_ns,
        "mean_us": statistics.mean(latencies) / 1e3 if latencies else None,
        "p99_us": latencies[(99 * len(latencies) + 99) // 100 - 1] / 1e3 if latencies else None,
        "min_us": min(latencies) / 1e3 if latencies else None,
        "max_us": max(latencies) / 1e3 if latencies else None,
        "fsync_ns": sync_times, "fsync_errors": sync_errors,
        "stats_delta": {key: value - before.get(key, 0) for key, value in after.items()},
    }
    (directory / "pwrite.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({k: v for k, v in result.items() if k not in ("workers", "stats_delta")}), flush=True)
    return result


class Mount:
    def __init__(self, binary, config, mount, log, env):
        self.mount = mount
        self.log = log.open("wb")
        self.proc = subprocess.Popen([str(binary), "mount", "--config", str(config),
                                      str(mount)], stdout=self.log,
                                     stderr=subprocess.STDOUT, env=env)

    def __enter__(self):
        for _ in range(600):
            if os.path.ismount(self.mount):
                return self
            if self.proc.poll() is not None:
                raise RuntimeError(f"mount exited: {self.proc.returncode}; see {self.log.name}")
            time.sleep(0.1)
        raise RuntimeError(f"mount timeout: pid={self.proc.pid}; see {self.log.name}")

    def __exit__(self, *_):
        # Unmount only this runner's mount. Never kill a process by name.
        start = time.monotonic()
        command(["fusermount3", "-u", str(self.mount)], timeout=60)
        self.proc.wait(timeout=60)
        self.log.close()
        (self.mount.parent / (Path(self.log.name).stem + "-teardown.json")).write_text(
            json.dumps({"unmount_s": time.monotonic() - start,
                        "exit": self.proc.returncode,
                        "still_mounted": os.path.ismount(self.mount)}, indent=2) + "\n")
        if self.proc.returncode or os.path.ismount(self.mount):
            raise RuntimeError("unclean mount teardown")


def run_cell(args, arm, rep):
    directory = args.output / f"r{rep}-{arm}"
    directory.mkdir()
    mount = directory / "mnt"
    objects = directory / "objects"
    cache = directory / "cache"
    for path in (mount, objects, cache):
        path.mkdir()
    namespace = f"issue371-{args.output.name}-{arm}-{rep}"
    data = f"data:\n  backend: local-fs\n  localfs:\n    data_dir: {objects}\n"
    data_config = directory / "data.yaml"
    data_config.write_text(data)
    init = [str(args.binary), "workspace", "--meta-backend", "redis", "--meta-url",
            args.redis, "--workspace-namespace", namespace]
    if arm == "packed":
        source = directory / "empty-source"
        source.mkdir()
        manifest = directory / "manifest-key.txt"
        command([str(args.fixture), "--source-directory", str(source),
                 "--source-consistency", "best-effort-detected", "--source-hardlink-policy",
                 "reject-external", "--output-dir", str(objects), "--prefix", "issue371",
                 "--metadata-codec", "raw", "--data-codec", "raw", "--frame-policy",
                 "size-only", "--inline-data", "off", "--manifest-output", str(manifest)],
                output=directory / "fixture.log")
        init += ["init-packed-volume", "--manifest-key", manifest.read_text().strip(),
                 "--data-config", str(data_config)]
    else:
        init += ["init-volume"]
    init_text = command(init, output=directory / "init.json")
    workspace = json.loads(init_text[init_text.index("{\n"):])["workspace_id"]
    config = directory / "mount.yaml"
    config.write_text(
        f"mount_point: {mount}\nvolume_format: workspace-v1\nworkspace: {workspace}\n"
        f"workspace_namespace: {namespace}\n" + data +
        f"meta:\n  backend: redis\n  redis:\n    url: {args.redis}\n"
        "layout: {chunk_size: 67108864, block_size: 4194304}\n"
        "fuse: {workers: 16, max_background: 512, privileged: false}\n"
        f"cache:\n  root: {cache}\n  read_memory_bytes: 0\n  read_ssd_bytes: 0\n"
        "  prefetch_enabled: false\n  range_background_prefetch: false\n  compression: none\n")
    env = os.environ | {"RUST_LOG": "warn", "BREWFS_CACHE_TTL_MS": "0",
                        "BREWFS_FUSE_DIRECT_IO": str(args.fuse_direct),
                        "BREWFS_FUSE_KEEP_CACHE": "0"}
    file_jobs = range(1 if args.shared_file else args.jobs)
    start = time.monotonic()
    with Mount(args.binary, config, mount, directory / "setup.log", env):
        chunk = bytes(range(256)) * 4096
        for job in file_jobs:
            with (mount / f"upper-{job}.bin").open("wb", buffering=0) as file:
                for _ in range(args.size_mib):
                    file.write(chunk)
                os.fsync(file.fileno())
        stats(mount, directory / "setup.stats")
    setup_s = time.monotonic() - start
    os.sync()
    Path("/proc/sys/vm/drop_caches").write_text("3\n")
    measured_env = env | {"RUST_LOG": args.log_filter}
    if args.log_filter != "warn":
        measured_env["BREWFS_LOG_FILE"] = str(directory / "trace.log")
    with Mount(args.binary, config, mount, directory / "measured.log", measured_env):
        if args.pwrite_count:
            probe = pwrite_probe(args, mount, directory)
            return {"arm": arm, "rep": rep, "pwrite": {k: v for k, v in probe.items()
                    if k not in ("workers", "stats_delta")}}
        before = stats(mount, directory / "before.stats")
        command(["redis-cli", "-u", args.redis, "INFO", "commandstats"],
                output=directory / "redis-before.txt")
        fio = ["/usr/bin/fio", "--name=randwrite", f"--directory={mount}",
               "--filename=upper-0.bin" if args.shared_file else "--filename_format=upper-$jobnum.bin",
               "--rw=randwrite", "--bs=4k",
               f"--size={args.size_mib}m", "--ioengine=libaio", "--iodepth=32",
               f"--numjobs={args.jobs}", f"--direct={args.direct}", "--time_based=1",
               f"--runtime={args.runtime}", "--randrepeat=1", "--randseed=371",
               "--refill_buffers=1", "--group_reporting=1", "--allow_file_create=0",
               "--end_fsync=0", "--lat_percentiles=1", "--output-format=json",
               f"--output={directory / 'fio.json'}"]
        (directory / "fio-command.json").write_text(json.dumps(fio, indent=2) + "\n")
        start = time.monotonic()
        command(fio, output=directory / "fio.log", timeout=300, check=False)
        fio_wall_s = time.monotonic() - start
        after_fio = stats(mount, directory / "after-fio.stats")
        drain_start = time.monotonic()
        drain_errors = []
        for job in file_jobs:
            try:
                fd = os.open(mount / f"upper-{job}.bin", os.O_RDWR)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            except OSError as error:
                drain_errors.append(f"upper-{job}.bin: {error}")
        after = stats(mount, directory / "after-drain.stats")
        drain_s = time.monotonic() - drain_start
        command(["redis-cli", "-u", args.redis, "INFO", "commandstats"],
                output=directory / "redis-after.txt")
        fio_text = (directory / "fio.json").read_text()
        fio_result, _ = json.JSONDecoder().raw_decode(fio_text[fio_text.index("{\n"):])
        job = fio_result["jobs"][0]
        write = job["write"]
        row = {"arm": arm, "rep": rep, "setup_s": setup_s,
               "fio_error": job["error"], "drain_errors": drain_errors,
               "bw_mib_s": write["bw_bytes"] / 2**20, "iops": write["iops"],
               "p99_ms": write["lat_ns"]["percentile"]["99.000000"] / 1e6,
               "mean_us": write["lat_ns"]["mean"] / 1e3,
               "fio_wall_s": fio_wall_s, "drain_s": drain_s,
               "wall_bw_mib_s": write["io_bytes"] / 2**20 / (fio_wall_s + drain_s),
               "stats_delta": {key: value - before.get(key, 0) for key, value in after.items()},
               "after_fio_debt": {key: value for key, value in after_fio.items()
                                  if "dirty_bytes" in key or "pending_upload_bytes" in key
                                  or "remote_upload_inflight_bytes" in key},
               "after_drain_debt": {key: value for key, value in after.items()
                                    if "dirty_bytes" in key or "pending_upload_bytes" in key
                                    or "remote_upload_inflight_bytes" in key}}
        io_valid = not any(job["error"] for job in fio_result["jobs"]) and not drain_errors
        if any(row["after_drain_debt"].values()):
            io_valid = False
        row["valid"] = False
        row["teardown_pending"] = True
        (directory / "result.json").write_text(json.dumps(row, indent=2) + "\n")
    row["teardown_pending"] = False
    row["valid"] = io_valid
    (directory / "result.json").write_text(json.dumps(row, indent=2) + "\n")
    print(json.dumps({k: v for k, v in row.items() if not isinstance(v, dict)}), flush=True)
    return row


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--redis", default="redis://127.0.0.1:6379/13")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--arms", nargs="+", choices=("native", "packed"),
                        default=["native", "packed"],
                        help="lower representations to run; alternate order across repeats")
    parser.add_argument("--runtime", type=int, default=20)
    parser.add_argument("--size-mib", type=int, default=64)
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--shared-file", action="store_true",
                        help="all fio jobs overwrite one inode instead of one inode per job")
    parser.add_argument("--log-filter", default="warn",
                        help="enable existing span-close diagnostics only in the measured mount")
    parser.add_argument("--direct", type=int, choices=(0, 1), default=0)
    parser.add_argument("--fuse-direct", type=int, choices=(0, 1), default=0)
    parser.add_argument("--pwrite-count", type=int, default=0,
                        help="diagnose this many synchronous pwrite calls per worker instead of fio")
    args = parser.parse_args()
    args.binary = args.binary.resolve()
    args.fixture = args.fixture.resolve()
    args.output = args.output.resolve()
    args.output.mkdir()
    profile = {key: str(value) if isinstance(value, Path) else value
               for key, value in vars(args).items()}
    profile["binary_sha256"] = hashlib.file_digest(args.binary.open("rb"), "sha256").hexdigest()
    profile["fixture_sha256"] = hashlib.file_digest(args.fixture.open("rb"), "sha256").hexdigest()
    profile["runner_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    profile["revision"] = command(["git", "rev-parse", "HEAD"]).strip()
    profile["fio_version"] = command(["/usr/bin/fio", "--version"]).strip()
    profile["kernel"] = command(["uname", "-a"]).strip()
    profile["uid"] = os.getuid()
    profile["file_origin"] = "upper; each lower contains only its root directory"
    (args.output / "profile.json").write_text(json.dumps(profile, indent=2) + "\n")
    rows = []
    for rep in range(1, args.repeats + 1):
        for arm in (args.arms if rep % 2 else reversed(args.arms)):
            print(json.dumps({"starting": {"arm": arm, "rep": rep}}), flush=True)
            try:
                rows.append(run_cell(args, arm, rep))
            except Exception as error:
                failure = {"arm": arm, "rep": rep, "failure": repr(error)}
                (args.output / f"r{rep}-{arm}" / "failure.json").write_text(
                    json.dumps(failure, indent=2) + "\n")
                print(json.dumps(failure), flush=True)
                rows.append(failure)
                if any(os.path.ismount(path) for path in args.output.glob("*/mnt")):
                    raise RuntimeError("a mount is still active; preserve diagnostics and stop") from error
    summary = {"rows": rows, "groups": {}}
    for arm in args.arms:
        arm_rows = [row for row in rows if row["arm"] == arm]
        values = [row for row in arm_rows if row.get("valid")]
        if not values:
            summary["groups"][arm] = {"completed": 0, "invalid": len(arm_rows)}
            continue
        summary["groups"][arm] = {
            metric: {"min": min(row[metric] for row in values),
                     "median": statistics.median(row[metric] for row in values),
                     "max": max(row[metric] for row in values)}
            for metric in ("bw_mib_s", "iops", "p99_ms", "mean_us", "fio_wall_s",
                           "drain_s", "wall_bw_mib_s")}
        summary["groups"][arm].update(completed=len(values), invalid=len(arm_rows) - len(values))
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary["groups"], indent=2))
    if not args.pwrite_count and any(
            group["completed"] != args.repeats for group in summary["groups"].values()):
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
