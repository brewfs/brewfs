#!/usr/bin/env python3
"""Pack real volumes and read them through a separately launched FUSE reader.

Example (requires Linux /dev/fuse and fusermount3):
  python3 tools/testing/packed_frame_default_compat.py --fixture ./fixture \
    --legacy-fixture ./old-fixture --reader ./brewfs --artifact /tmp/frame-compat
The artifact must be new. All commands, manifests, objects and hashes survive.
"""

import argparse
import hashlib
import json
import mmap
import os
from pathlib import Path
import signal
import struct
import subprocess
import sys
import time


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def verify_reads(source, mount, output):
    results = []
    for original in sorted(source.iterdir()):
        target = mount / original.name
        expected = digest(original)
        actual = digest(target)
        assert actual == expected, original.name
        size = original.stat().st_size
        assert target.stat().st_size == size
        # Repeat boundary/EOF reads through O_DIRECT with an aligned buffer.
        reads = 0
        fd = os.open(target, os.O_RDONLY | os.O_DIRECT)
        try:
            with original.open("rb") as oracle, mmap.mmap(-1, 8192) as buf:
                for boundary in [0, 65536, 262144, 4194304, 16777216, size]:
                    offset = max(0, boundary - 4096) // 4096 * 4096
                    oracle.seek(offset)
                    wanted = oracle.read(8192)
                    count = os.preadv(fd, [buf], offset)
                    assert bytes(buf[:count]) == wanted, (original.name, offset)
                    reads += 1
                assert os.preadv(fd, [buf], (size + 4095) // 4096 * 4096) == 0
                reads += 1
        finally:
            os.close(fd)
        results.append(dict(file=original.name, bytes=size, sha256=actual,
                            direct_checks=reads))
    write_json(output, results)


def inventory(objects, manifest, cap, sizes):
    key = manifest.read_text().strip()
    raw = (objects / key).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == key.rsplit("/", 1)[1]
    body = raw[4096:-64]
    assert body[:4] == b"PM11"
    table = struct.unpack_from("<QQQ", body, 96)
    assert table == (min(262144, cap), cap, 8388608), table
    frame_sizes = []
    for path in objects.rglob("*"):
        if not path.is_file() or path.parent.name != "3":
            continue
        raw = path.read_bytes()
        assert hashlib.sha256(raw).hexdigest() == path.name
        body = raw[4096:-64]
        assert body[:4] == b"FD06"
        assert struct.unpack_from("<QQQ", body, 48) == table
        count = struct.unpack_from("<I", body, 76)[0]
        assert len(body) == 80 + count * 48
        for index in range(count):
            stored, length = struct.unpack_from("<II", body, 80 + index * 48 + 16)
            assert 0 < length <= cap
            assert stored == length  # --data-codec raw
            frame_sizes.append(length)
    expected = sum((size + cap - 1) // cap for size in sizes)
    assert len(frame_sizes) == expected, (len(frame_sizes), expected)
    assert sum(frame_sizes) == sum(sizes)
    provenance = json.loads(manifest.with_suffix(".source.json").read_text())
    build = provenance["build_provenance"]
    assert build["frame_count"] == expected
    assert build["size_classes"]["max_random_frame_raw_bytes"] == cap
    return dict(manifest_key=key, size_classes=table, frames=expected,
                frame_bytes=sum(frame_sizes), largest_frame=max(frame_sizes))


def run_arm(args, source, label, cap):
    arm = args.artifact / label
    arm.mkdir()
    objects, mount = arm / "objects", arm / "mnt"
    objects.mkdir()
    mount.mkdir()
    manifest = arm / "manifest.txt"
    fixture = args.legacy_fixture if label == "legacy4m" else args.fixture
    command = [str(fixture), "--source-directory", str(source),
               "--source-consistency", "best-effort-detected",
               "--source-hardlink-policy", "visible-links",
               "--output-dir", str(objects), "--manifest-output", str(manifest),
               "--metadata-codec", "raw", "--data-codec", "raw"]
    if label != "default":
        command += ["--max-random-frame-raw-bytes", str(cap)]
    write_json(arm / "pack-command.json", command)
    with (arm / "pack.log").open("w") as log:
        subprocess.run(command, stdout=log, stderr=subprocess.STDOUT,
                       check=True, timeout=120)
    result = inventory(objects, manifest, cap,
                       [path.stat().st_size for path in source.iterdir()])
    write_json(arm / "inventory.json", result)
    config = dict(volume_format="packed-metadata-v3",
                  packed_manifest_key=manifest.read_text().strip(),
                  data=dict(backend="local-fs", localfs=dict(data_dir=str(objects))),
                  layout=dict(chunk_size=67108864, block_size=4194304),
                  cache=dict(root=str(arm / "cache"), read_memory_bytes=0,
                             read_ssd_bytes=0, prefetch_enabled=False,
                             range_background_prefetch=False, compression="none"))
    write_json(arm / "mount.json", config)
    command = [str(args.reader), "mount", "--privileged", "--config",
               str(arm / "mount.json"), str(mount)]
    write_json(arm / "mount-command.json", command)
    env = os.environ | {"BREWFS_FUSE_KEEP_CACHE": "0", "RUST_LOG": "warn"}
    with (arm / "mount.log").open("w") as log:
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=env)
        try:
            deadline = time.monotonic() + 30
            while not os.path.ismount(mount):
                assert process.poll() is None, "mount exited; inspect mount.log"
                assert time.monotonic() < deadline, "mount timed out"
                time.sleep(0.1)
            subprocess.run([sys.executable, __file__, "--verify", str(source),
                            str(mount), str(arm / "reads.json")], check=True, timeout=120)
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
            try:
                status = process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                subprocess.run(["fusermount3", "-u", str(mount)], check=True, timeout=10)
                status = process.wait(timeout=10)
            write_json(arm / "cleanup.json", dict(exit_code=status, mounted=os.path.ismount(mount)))
            assert status == 0 and not os.path.ismount(mount), "unclean teardown"
    result["reads"] = json.loads((arm / "reads.json").read_text())
    return result


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--verify":
        verify_reads(*map(Path, sys.argv[2:]))
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--legacy-fixture", type=Path)
    parser.add_argument("--reader", type=Path, required=True)
    parser.add_argument("--artifact", type=Path, required=True)
    args = parser.parse_args()
    args.fixture, args.reader = args.fixture.resolve(), args.reader.resolve()
    args.legacy_fixture = (args.legacy_fixture or args.fixture).resolve()
    args.artifact = args.artifact.resolve()
    args.artifact.mkdir()  # Never overwrite evidence from a previous run.
    write_json(args.artifact / "binaries.json", {str(p): digest(p) for p in
               [args.fixture, args.legacy_fixture, args.reader]})
    source = args.artifact / "source"
    source.mkdir()
    for name, size in [("group", 5 * 1048576 + 123), ("external", 17 * 1048576 + 7), ("empty", 0)]:
        (source / name).write_bytes(hashlib.shake_256(name.encode()).digest(size))
    results = {}
    for label, cap in [("legacy4m", 4194304), ("default", 262144), ("explicit64k", 65536)]:
        results[label] = run_arm(args, source, label, cap)
        write_json(args.artifact / "results.json", results)
        print(label, "PASS", results[label]["frames"], "frames", flush=True)


if __name__ == "__main__":
    main()
