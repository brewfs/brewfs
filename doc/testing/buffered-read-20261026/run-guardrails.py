import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent
REPO = Path('/data/metacache/repo')
sys.path.insert(0, str(REPO / 'tools/perf'))
from workspace_metadata_cache import RequestCounts, checked_mount

for label in ('off', 'on'):
    subprocess.run([sys.executable, str(ROOT / 'mount.py'), 'start', label], check=True)
    mount = ROOT / label / 'mnt'
    output = ROOT / label / 'guardrails'
    output.mkdir(exist_ok=True)
    try:
        # Keep the original campaign's full metadata/write protocol and counts.
        subprocess.run([sys.executable, str(REPO / 'tools/perf/workspace_metadata_cache.py'),
                        str(mount), str(output), '--label', label], check=True, timeout=1200)
        corpus = mount / 'buffered-read-smallfiles'
        corpus.mkdir()
        for index in range(256):
            (corpus / f'f{index:06d}').write_bytes(bytes(4096))
        for rep in range(1, 4):
            for name in ('smallfiles-stat', 'dirperf'):
                work = mount / 'buffered-read-dirperf'
                if name == 'smallfiles-stat':
                    command = [sys.executable, str(REPO / 'tools/perf/smallfiles_scan.py'),
                               '--root', str(corpus), '--mode', 'stat', '--label', label,
                               '--expected-files', '256', '--min-size', '4096', '--max-size', '4096',
                               '--dir-levels', '0', '--dirs-per-level', '1', '--files-per-leaf', '256',
                               '--workers', '1', '--epochs', '32',
                               '--json-output', str(output / f'{name}-{rep}.json')]
                else:
                    work.mkdir()
                    command = [str(ROOT / 'dirperf'), '-d', str(work), '-a', '100', '-f', '100',
                               '-l', '100', '-c', '16', '-n', '2', '-s', '5']
                with RequestCounts(mount, output / f'{name}-{rep}.bpf.jsonl') as trace:
                    checked_mount(mount)
                    with (output / f'{name}-{rep}.log').open('w') as log:
                        trace.start()
                        start = time.perf_counter()
                        subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=180)
                        seconds = time.perf_counter() - start
                        trace.stop()
                row = dict(label=label, operation=name, repetition=rep, seconds=seconds,
                           fuse_requests=trace.counts, command=command)
                with (output / 'supplemental.jsonl').open('a') as file:
                    file.write(json.dumps(row) + '\n')
                print(json.dumps(row), flush=True)
                if work.exists():
                    shutil.rmtree(work)
        shutil.rmtree(corpus)
    finally:
        subprocess.run([sys.executable, str(ROOT / 'mount.py'), 'stop', label], check=True, timeout=90)
