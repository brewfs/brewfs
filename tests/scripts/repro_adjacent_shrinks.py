#!/usr/bin/env python3
"""Issue #335: run against a mounted workspace with chunk size >= 8192."""
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory


def reproduce(mountpoint: str) -> None:
    with TemporaryDirectory(prefix="adjacent-shrinks-335-", dir=mountpoint) as directory:
        path = Path(directory) / "sparse"
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
        try:
            os.ftruncate(fd, 8192)
            for size in range(8191, 4094, -1):
                os.ftruncate(fd, size)
        finally:
            os.close(fd)
        # No reads or data writes have populated a cached plan or data page.
        fd = os.open(path, os.O_RDONLY)
        try:
            assert os.fstat(fd).st_size == 4095
            data = os.pread(fd, 4095, 0)
            assert data == bytes(4095), (len(data), data[:32])
        finally:
            os.close(fd)
    print("4097 adjacent shrinks: reopened read returns 4095 zero bytes")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(f"Usage: {sys.argv[0]} MOUNTPOINT")
    reproduce(sys.argv[1])
