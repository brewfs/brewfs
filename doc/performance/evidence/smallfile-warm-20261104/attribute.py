#!/usr/bin/env python3
"""Instrument the existing Compose scanner's stat/open/read/close sequence."""
import collections
import hashlib
import json
import os
import pathlib
import sys
import time
import urllib.request

root = pathlib.Path('/mnt/brewfs')
out = pathlib.Path(sys.argv[1])
out.mkdir(parents=True, exist_ok=True)


def capture(name, before):
    def stats():
        (out / (name + '.stats')).write_bytes((root / '.stats').read_bytes())
    if before:
        stats()
    with urllib.request.urlopen('http://127.0.0.1:9567/metrics') as response:
        (out / (name + '.metrics')).write_bytes(response.read())
    if not before:
        stats()


def scan(mode, label):
    capture(label + '-before', True)
    stages = collections.Counter()
    t0 = time.perf_counter_ns()
    started = t0
    walks = list(os.walk(root))
    stages['readdir'] += time.perf_counter_ns() - started
    files = 0
    payload_bytes = 0
    checksum = 0
    latencies = []
    for directory, dirs, names in walks:
        assert not dirs
        names = sorted(name for name in names if not name.startswith('.'))
        assert len(names) == 8192
        if mode == 'tree':
            files += len(names)
            continue
        for name in names:
            path = pathlib.Path(directory) / name
            file_started = time.perf_counter_ns()
            if mode == 'compose':
                started = time.perf_counter_ns()
                st = path.stat()
                stages['stat'] += time.perf_counter_ns() - started
                assert st.st_size == 4096
            started = time.perf_counter_ns()
            handle = path.open('rb')
            stages['open'] += time.perf_counter_ns() - started
            if mode != 'openclose':
                started = time.perf_counter_ns()
                payload = handle.read()
                stages['read'] += time.perf_counter_ns() - started
                assert len(payload) == 4096
                payload_bytes += len(payload)
                checksum = (checksum + payload[0]) & 0xffffffff
            started = time.perf_counter_ns()
            handle.close()
            stages['close'] += time.perf_counter_ns() - started
            files += 1
            latencies.append(time.perf_counter_ns() - file_started)
    total = time.perf_counter_ns() - t0
    stages['python_other'] = total - sum(stages.values())
    time.sleep(0.05)  # Let asynchronous RELEASE finish before the counter snapshot.
    capture(label + '-after', False)
    result = dict(label=label, mode=mode, files=files, total_ns=total,
                  ms_per_file=total / files / 1e6, stages_ns=dict(stages),
                  payload_bytes=payload_bytes, checksum=checksum,
                  latency_ns=latencies)
    (out / (label + '.json')).write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({k: v for k, v in result.items() if k != 'latency_ns'}), flush=True)


scan('tree', 'prewarm')
for round_number in range(1, 4):
    for mode in ('compose', 'tree', 'openclose', 'openread'):
        scan(mode, f'{mode}-r{round_number}')

# Content validation is untimed and verifies the fixture's deterministic bytes.
manifest = {}
for path in sorted(root.glob('f*')):
    data = path.read_bytes()
    number = int(path.name[1:])
    seed = (number + 1).to_bytes(8, 'little')
    pattern = bytes(byte ^ (0x80 if i & 1 else 0) for i, byte in enumerate(seed))
    assert data == pattern * 512, path
    manifest[path.name] = hashlib.sha256(data).hexdigest()
assert len(manifest) == 8192
(out / 'sha256.json').write_text(json.dumps(manifest, sort_keys=True) + '\n')
