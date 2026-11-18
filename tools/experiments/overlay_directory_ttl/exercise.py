"""Exercise the sealed-to-writable transition with independent Linux FUSE mounts."""

import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import uuid

import redis

from directory import Directory


def wait_for(predicate, description, timeout=15):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return
        time.sleep(0.015)
    raise TimeoutError(description)


def exercise(temp, changed, sibling, existing, sibling_file):
    missing = "upper-" + uuid.uuid4().hex
    second_name = "other-" + uuid.uuid4().hex
    assert not (changed / missing).exists() and not (changed / second_name).exists()
    initial = (changed / existing).read_bytes()
    upper = b"created in the Redis transaction"
    with ExitStack() as stack:
        sock = temp / "redis.sock"
        server = subprocess.Popen(
            ["redis-server", "--port", "0", "--unixsocket", str(sock), "--save", "",
             "--appendonly", "no", "--dir", str(temp)],
            stdout=stack.enter_context((temp / "redis.log").open("wb")), stderr=subprocess.STDOUT,
        )

        def stop_server():
            server.terminate()
            server.wait(timeout=10)

        stack.callback(stop_server)
        wait_for(sock.exists, "Redis socket")
        db = redis.Redis(unix_socket_path=str(sock))
        db.ping()
        directory = Directory(db, "changed")

        def mount(reader):
            point = temp / reader
            point.mkdir()
            log = stack.enter_context((temp / f"{reader}.log").open("w+b"))
            proc = subprocess.Popen(
                [sys.executable, str(Path(__file__).with_name("mount.py")),
                 "--socket", str(sock), "--reader", reader, "--mount", str(point),
                 "--changed", str(changed), "--sibling", str(sibling)],
                stdout=log, stderr=subprocess.STDOUT,
            )

            def stop():
                if proc.poll() is None:
                    proc.send_signal(signal.SIGCONT)
                if os.path.ismount(point):
                    subprocess.run(["fusermount3", "-u", str(point)], check=True, timeout=10)
                if proc.poll() is None:
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.terminate()
                        proc.wait(timeout=5)

            stack.callback(stop)

            def ready():
                if proc.poll() is not None:
                    log.seek(0)
                    raise RuntimeError(log.read().decode())
                return os.path.ismount(point)

            wait_for(ready, f"mount {reader}")
            return point, proc

        a, _ = mount("a")
        b, b_proc = mount("b")
        for name, root in [("a", a), ("b", b)]:
            counts = f"counts:{name}"
            assert (root / "changed" / existing).read_bytes() == initial
            assert not (root / "changed" / missing).exists()
            names = os.listdir(root / "changed")
            assert existing in names and missing not in names
            assert (root / "sibling" / sibling_file).is_file()
            before = db.hgetall(counts)
            assert (root / "changed" / existing).is_file()
            assert not (root / "changed" / missing).exists()
            assert (root / "sibling" / sibling_file).is_file()
            assert before == db.hgetall(counts), "warm stat reached the daemon"
        assert db.smembers(directory.readers) == {b"a", b"b"}
        bypass = "raw-" + uuid.uuid4().hex
        assert not (a / "changed" / bypass).exists()
        cached = db.hget("counts:a", "lookup:changed")
        db.hset(directory.files, bypass, b"uncoordinated")
        assert not (a / "changed" / bypass).exists()
        assert db.hget("counts:a", "lookup:changed") == cached
        db.hdel(directory.files, bypass)
        sibling_before = db.hget("counts:a", "lookup:sibling")
        directory_mtime = (a / "changed").stat().st_mtime_ns

        b_proc.send_signal(signal.SIGSTOP)
        wait_for(lambda: "\nState:\tT" in Path(f"/proc/{b_proc.pid}/status").read_text(), "pause B")
        op = uuid.uuid4().hex
        assert directory.begin_create(op, missing, upper) == "draining"
        assert directory.begin_create(op, missing, upper) == "draining"
        assert directory.begin_create(op, second_name, upper) == "conflict"
        assert directory.begin_create("competing-writer", second_name, upper) == "busy"
        wait_for(lambda: db.smembers(directory.readers) == {b"b"}, "A invalidated its long replies")
        assert directory.publish(op) == "draining" and not db.hexists(directory.files, missing)
        c, _ = mount("late")
        assert not (c / "changed" / missing).exists()
        assert db.smembers(directory.readers) == {b"b"}

        resumed = time.monotonic()
        b_proc.send_signal(signal.SIGCONT)
        wait_for(lambda: not db.smembers(directory.readers), "B acknowledged invalidation")
        start = time.monotonic()
        assert directory.publish(op) == "writable"
        resume_to_publish_ms = 1000 * (time.monotonic() - resumed)
        modified = db.hget(directory.state, "modified_ns")
        assert directory.publish(op) == "writable" and db.hget(directory.state, "modified_ns") == modified
        for root in (a, b, c):
            assert (root / "changed" / missing).read_bytes() == upper
            assert (root / "changed" / existing).read_bytes() == initial
            assert (root / "changed").stat().st_mtime_ns != directory_mtime
        visible_ms = 1000 * (time.monotonic() - start)
        for root in (a, b, c):
            assert missing in os.listdir(root / "changed")
        assert (a / "sibling" / sibling_file).is_file()
        assert db.hget("counts:a", "lookup:sibling") == sibling_before
        assert db.smembers(Directory(db, "sibling").readers) == {b"a", b"b"}

        assert not (a / "changed" / second_name).exists()
        assert directory.begin_create("second-create", second_name, b"second") == "writable"
        wait_for(lambda: (a / "changed" / second_name).is_file(), "short negative TTL", timeout=3)
        assert (a / "changed" / second_name).read_bytes() == b"second"
        return {"mounts": 3, "long_ttl_seconds": 3600, "writable_ttl_seconds": 1,
                "paused_reader_blocked_publication": True, "warm_lookup_callbacks": 0,
                "all_readers_observed_first_create_ms": round(visible_ms, 3),
                "resume_to_publication_ms": round(resume_to_publish_ms, 3),
                "revoked_names": [int(db.hget(f"counts:{n}", "revoked-names:changed")) for n in ("a", "b")],
                "directory_listings_observed_create": True, "uncoordinated_create_was_stale": True,
                "sibling_lookup_callbacks_after_change": 0, "second_create_observed": True}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--changed", type=Path)
    parser.add_argument("--sibling", type=Path)
    parser.add_argument("--existing")
    parser.add_argument("--sibling-file")
    options = parser.parse_args()
    supplied = [options.changed, options.sibling, options.existing, options.sibling_file]
    if any(supplied) and not all(supplied):
        parser.error("supply both lower directories and one direct child filename in each")
    with tempfile.TemporaryDirectory(prefix="pack-ttl-") as tmp:
        root = Path(tmp)
        if not any(supplied):
            for name in ("changed", "sibling"):
                (root / name).mkdir()
                (root / name / "lower").write_bytes(b"immutable lower")
            supplied = [root / "changed", root / "sibling", "lower", "lower"]
        result = exercise(root, *supplied)
        print(json.dumps(result, indent=2))
