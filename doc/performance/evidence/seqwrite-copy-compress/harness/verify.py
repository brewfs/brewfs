#!/usr/bin/env python3
"""Full 512 MiB sequential content, per-4KiB SHA-256, immediate crash/restart."""
import argparse
import hashlib
import json
import os
import random
import time

import campaign as c
from crash_helpers import start, stop

BLOCK = 4096
UNIT = 4 << 20


def run_case(variant, direct, boundary):
    label = f'{variant}-direct{direct}-{boundary}'
    binary = c.ROOT / 'bin' / f'brewfs-{variant}'
    c.h.ENV['BREWFS_FUSE_DIRECT_IO'] = str(direct)
    c.reset()
    mount, process, directory = start(label + '-before-kill', binary)
    fd = os.open(mount / 'sequential', os.O_CREAT | os.O_RDWR, 0o600)
    rng = random.Random(20261102)
    expected = []
    whole = hashlib.sha256()
    for index in range(128):
        if index % 8 == 0:
            data = bytes(UNIT)
        elif index % 8 == 1:
            pattern = b'SF\x00\x00sequential compression verification\n'
            data = (pattern * (UNIT // len(pattern) + 1))[:UNIT]
        elif index % 8 == 2:
            # An incompressible prefix must not hide a compressible remainder.
            data = rng.randbytes(8192) + bytes(UNIT - 8192)
        else:
            data = rng.randbytes(UNIT)
        whole.update(data)
        expected.extend(hashlib.sha256(data[offset:offset + BLOCK]).hexdigest()
                        for offset in range(0, UNIT, BLOCK))
        if index % 16 == 15:
            # Exercise 4 KiB appends alongside the usual 1 MiB FUSE stream.
            write_unit = BLOCK
        else:
            write_unit = 1 << 20
        for offset in range(0, UNIT, write_unit):
            part = data[offset:offset + write_unit]
            assert os.write(fd, part) == len(part)
    (mount / 'empty').touch()
    with (mount / 'single-byte').open('wb') as one:
        one.write(b'Z')
    if boundary == 'fsync':
        os.fsync(fd)
    else:
        os.close(fd)
        fd = None
    boundary_time = time.time()
    # No drain, sleep, verification read or mount cleanup before SIGKILL.
    stop(mount, process, directory, kill=True, fd_to_close=fd)
    c.h.save(directory / 'expected-blocks-sha256.json', expected)
    expected_whole = whole.hexdigest()
    mount, process, directory = start(label + '-after-restart', binary)
    actual = []
    actual_whole = hashlib.sha256()
    with (mount / 'sequential').open('rb', buffering=0) as stream:
        while data := stream.read(1 << 20):
            actual_whole.update(data)
            actual.extend(hashlib.sha256(data[offset:offset + BLOCK]).hexdigest()
                          for offset in range(0, len(data), BLOCK))
    c.h.save(directory / 'actual-blocks-sha256.json', actual)
    assert actual == expected, f'4 KiB SHA-256 mismatch: {label}'
    assert actual_whole.hexdigest() == expected_whole
    assert (mount / 'sequential').stat().st_size == 512 << 20
    assert (mount / 'empty').stat().st_size == 0
    assert (mount / 'single-byte').read_bytes() == b'Z'
    check = {'label': label, 'bytes': 512 << 20, 'blocks': len(actual),
             'all_block_sha256_match': True, 'whole_sha256': expected_whole,
             'boundary_unix': boundary_time, 'direct': direct, 'boundary': boundary,
             'binary_sha256': hashlib.file_digest(binary.open('rb'), 'sha256').hexdigest(),
             'empty_and_single_byte': True, 'signal': 'SIGKILL', 'fresh_cache_restart': True}
    c.h.save(directory / 'verification.json', check)
    stop(mount, process, directory)
    return check


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--variant', choices=['baseline', 'candidate'], default='candidate')
    args = parser.parse_args()
    checks = [run_case(args.variant, direct, boundary)
              for direct in [0, 1] for boundary in ['fsync', 'close']]
    c.h.save(c.ROOT / 'correctness' / f'{args.variant}-summary.json', {
        'checks': checks, 'verified_bytes': sum(check['bytes'] for check in checks),
        'all_sha256_match': True, 'all_restarts_passed': True})
    c.reset()
