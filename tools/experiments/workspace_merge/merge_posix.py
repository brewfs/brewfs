#!/usr/bin/env python3
"""Three-way merge of quiesced POSIX directory trees."""
import argparse
import errno
import heapq
import itertools
import json
import os
from pathlib import Path
import shutil
import stat
import tempfile
import time
from dataclasses import dataclass, field


@dataclass
class Entry:
    path: Path
    mode: int
    size: int
    children: list = field(default_factory=list)


def scan(path, metrics):
    info = path.lstat()
    metrics["entries_scanned"] += 1
    kind = stat.S_IFMT(info.st_mode)
    if kind not in (stat.S_IFDIR, stat.S_IFREG, stat.S_IFLNK):
        raise ValueError(f"unsupported file type: {path}")
    if kind == stat.S_IFREG and info.st_nlink != 1:
        raise ValueError(f"hard links are unsupported: {path}")
    entry = Entry(path, info.st_mode, info.st_size)
    if kind == stat.S_IFDIR:
        entry.children = [scan(p, metrics) for p in sorted(path.iterdir())]
    return entry


def directory(entry):
    return entry is not None and stat.S_ISDIR(entry.mode)


def equal(left, right, metrics):
    if left is None or right is None:
        return left is right
    if left.mode != right.mode:
        return False
    if directory(left):
        return len(left.children) == len(right.children) and all(
            a.path.name == b.path.name and equal(a, b, metrics)
            for a, b in zip(left.children, right.children)
        )
    if stat.S_ISLNK(left.mode):
        return os.readlink(left.path) == os.readlink(right.path)
    if left.size != right.size:
        return False
    with left.path.open("rb") as a, right.path.open("rb") as b:
        while True:
            x, y = a.read(1048576), b.read(1048576)
            metrics["comparison_bytes_read"] += len(x) + len(y)
            if x != y:
                return False
            if not x:
                return True


def plan(base, child, target, relative, edits, conflicts, metrics):
    if all(map(directory, (base, child, target))):
        if child.mode != base.mode:
            if target.mode != base.mode and target.mode != child.mode:
                conflicts.append(relative)
            elif child.mode != target.mode:
                edits.append(("mode", relative, child))
        streams = [tagged(entry.children, i) for i, entry in enumerate((base, child, target))]
        for name, group in itertools.groupby(heapq.merge(*streams), key=lambda row: row[0]):
            entries = [None, None, None]
            for _, index, entry in group:
                entries[index] = entry
            plan(*entries, relative / name, edits, conflicts, metrics)
    elif not equal(base, child, metrics):
        if equal(base, target, metrics):
            edits.append(("replace", relative, child))
        elif not equal(child, target, metrics):
            conflicts.append(relative)


def tagged(children, index):
    for entry in children:
        yield entry.path.name, index, entry


def copy_file(source, dest, size, metrics):
    with source.open("rb") as src:
        fd, tmp = tempfile.mkstemp(dir=dest.parent)
        try:
            with os.fdopen(fd, "wb") as out:
                position = 0
                while position < size:
                    try:
                        start = os.lseek(src.fileno(), position, os.SEEK_DATA)
                        end = min(size, os.lseek(src.fileno(), start, os.SEEK_HOLE))
                    except OSError as error:
                        if error.errno == errno.ENXIO:
                            break
                        if error.errno not in (errno.EINVAL, errno.ENOTSUP, errno.ENOSYS):
                            raise
                        start, end = position, size
                    src.seek(start)
                    out.seek(start)
                    position = start
                    while position < end:
                        data = src.read(min(1048576, end - position))
                        if not data:
                            raise OSError(f"source truncated: {source}")
                        out.write(data)
                        position += len(data)
                        metrics["payload_bytes_copied"] += len(data)
                out.truncate(size)
                os.fchmod(out.fileno(), stat.S_IMODE(source.stat().st_mode))
                out.flush()
                os.fsync(out.fileno())
            os.replace(tmp, dest)
        finally:
            if os.path.lexists(tmp):
                os.unlink(tmp)


def install(entry, dest, metrics):
    if directory(entry):
        dest.mkdir(mode=0o700)
        for node in entry.children:
            install(node, dest / node.path.name, metrics)
        dest.chmod(stat.S_IMODE(entry.mode))
    elif stat.S_ISLNK(entry.mode):
        dest.symlink_to(os.readlink(entry.path))
    else:
        copy_file(entry.path, dest, entry.size, metrics)


def merge(base, child, target):
    roots = [p.resolve(strict=True) for p in (base, child, target)]
    if any(not p.is_dir() for p in roots) or any(
        a == b or a in b.parents or b in a.parents for a, b in itertools.combinations(roots, 2)
    ):
        raise ValueError("roots must be disjoint directories")
    metrics = dict(entries_scanned=0, comparison_bytes_read=0, payload_bytes_copied=0)
    start = time.monotonic()
    trees = [scan(root, metrics) for root in roots]
    metrics["scan_seconds"] = time.monotonic() - start
    start = time.monotonic()
    edits, conflicts = [], []
    plan(*trees, Path("."), edits, conflicts, metrics)
    metrics["plan_seconds"] = time.monotonic() - start
    start = time.monotonic()
    if not conflicts:
        for operation, relative, entry in reversed(edits):
            dest = roots[2] / relative
            if operation == "mode":
                dest.chmod(stat.S_IMODE(entry.mode))
                continue
            if dest.is_dir() and not dest.is_symlink():
                shutil.rmtree(dest)
            elif os.path.lexists(dest):
                dest.unlink()
            if entry is not None:
                install(entry, dest, metrics)
    metrics["apply_seconds"] = time.monotonic() - start
    return dict(conflicts=[str(p) for p in conflicts], planned_edits=len(edits), **metrics)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("base", "child", "target"):
        parser.add_argument(name, type=Path)
    args = parser.parse_args()
    try:
        result = merge(args.base, args.child, args.target)
    except (OSError, ValueError) as error:
        print(json.dumps({"error": str(error)}))
        return 1
    print(json.dumps(result))
    return 2 if result["conflicts"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
