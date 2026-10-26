#!/usr/bin/env python3
"""Check reopen caching and same-mount visibility on an idle BrewFS mount.

Run once with the default policy (--expect-cache off), then on a mount using
--fuse-keep-cache true (--expect-cache on). Requires Linux and root for the
permission checks. This is a correctness probe, not a throughput benchmark.
"""

import argparse
import errno
import json
import mmap
import os
from pathlib import Path
import subprocess
import tempfile

if not __debug__:
    raise RuntimeError("Run this correctness probe without Python -O / PYTHONOPTIMIZE")


def read_ops(mount):
    for line in (mount / ".stats").read_text().splitlines():
        if line.startswith("brewfs_fuse_read_ops_total "):
            return int(line.split()[1])
    raise AssertionError("BrewFS FUSE READ counter missing")


def as_user(uid, gid, operation, allowed):
    pid = os.fork()
    if pid == 0:
        try:
            os.setgroups([])
            os.setgid(gid)
            os.setuid(uid)
            try:
                operation()
            except OSError as error:
                assert not allowed and error.errno in (errno.EACCES, errno.EPERM), error
            else:
                assert allowed, "operation unexpectedly succeeded"
        except BaseException:
            import traceback

            traceback.print_exc()
            os._exit(1)
        os._exit(0)
    _, status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(status) == 0, (uid, gid, allowed)


def probe(mount, expect_cache):
    subprocess.run(["mountpoint", "-q", str(mount)], check=True)
    assert os.geteuid() == 0, "root is required for permission checks"
    with tempfile.TemporaryDirectory(prefix="read-cache-probe-", dir=mount) as name:
        directory = Path(name)
        directory.chmod(0o755)
        path = directory / "data"
        expected = os.urandom(4 * 1024 * 1024)
        with path.open("wb", buffering=0) as writer:
            assert writer.write(expected) == len(expected)
            os.fsync(writer.fileno())
        assert path.read_bytes() == expected
        before = read_ops(mount)
        for _ in range(3):
            assert path.read_bytes() == expected
        delta = read_ops(mount) - before
        if expect_cache:
            assert delta == 0, f"warm reopens issued {delta} FUSE READs"
        else:
            assert delta > 0, "default reopen invalidation was not exercised"

        with path.open("rb", buffering=0) as reader:
            assert reader.read() == expected
            patch = b"overwrite" * 127
            with path.open("r+b", buffering=0) as writer:
                writer.seek(137)
                assert writer.write(patch) == len(patch)
                expected = expected[:137] + patch + expected[137 + len(patch) :]
                reader.seek(0)
                assert reader.read() == expected, "open reader missed buffered write"
                assert path.read_bytes() == expected, "reopen missed buffered write"
            with path.open("ab", buffering=0) as writer:
                writer.write(b"append")
            expected += b"append"
            assert path.read_bytes() == expected, "append not visible"
            reader.seek(0)
            assert reader.read() == expected, "open reader missed append"
            os.truncate(path, 12345)
            expected = expected[:12345]
            assert path.read_bytes() == expected, "shrink not visible"
            reader.seek(0)
            assert reader.read() == expected, "open reader missed shrink"
            os.truncate(path, 20000)
            expected += bytes(20000 - len(expected))
            assert path.read_bytes() == expected, "extend exposed old tail"
            reader.seek(0)
            assert reader.read() == expected, "open reader saw old tail after extend"
            with path.open("wb", buffering=0) as writer:
                writer.write(b"replacement")
            assert path.read_bytes() == b"replacement", "O_TRUNC not visible"
            reader.seek(0)
            assert reader.read() == b"replacement", "open reader missed O_TRUNC"

        path.write_bytes(b"A" * 8192)
        assert path.read_bytes() == b"A" * 8192
        with path.open("rb", buffering=0) as reader:
            assert reader.read() == b"A" * 8192
            direct = os.open(path, os.O_WRONLY | os.O_DIRECT)
            try:
                with mmap.mmap(-1, 4096) as aligned:
                    aligned[:] = b"D" * 4096
                    assert os.pwrite(direct, aligned, 0) == 4096
            finally:
                os.close(direct)
            reader.seek(0)
            assert reader.read() == b"D" * 4096 + b"A" * 4096, (
                "open reader missed direct write"
            )
        assert path.read_bytes() == b"D" * 4096 + b"A" * 4096, "direct write not visible"
        with path.open("r+b", buffering=0) as writer:
            with mmap.mmap(writer.fileno(), 8192) as shared:
                shared[4096:4100] = b"MMAP"
                shared.flush()
        assert path.read_bytes() == b"D" * 4096 + b"MMAP" + b"A" * 4092, (
            "shared mmap write not visible"
        )
        replacement = directory / "replacement"
        replacement.write_bytes(b"new inode")
        os.replace(replacement, path)
        assert path.read_bytes() == b"new inode", "rename replacement not visible"

        os.chown(path, 65530, 65530)
        path.chmod(0o600)
        assert path.read_bytes() == b"new inode"  # Warm pages as root first.
        as_user(65531, 65530, path.read_bytes, False)
        for mode, gid in [(0o660, 65530), (0o666, 65531)]:
            path.chmod(mode)
            as_user(65531, gid, lambda: path.write_bytes(b"permitted"), True)
            as_user(65531, gid, lambda: os.utime(path, None), True)
            as_user(65531, gid, lambda: os.utime(path, (1, 1)), False)
            assert path.read_bytes() == b"permitted"
        path.chmod(0o600)
        as_user(65531, 65530, path.read_bytes, False)
    return {"mount": str(mount), "expect_cache": expect_cache,
            "three_warm_reopens_fuse_reads": delta, "visibility_and_permissions": "passed"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mount", type=Path)
    parser.add_argument("--expect-cache", choices=("on", "off"), required=True)
    args = parser.parse_args()
    print(json.dumps(probe(args.mount.resolve(), args.expect_cache == "on"), indent=2))
