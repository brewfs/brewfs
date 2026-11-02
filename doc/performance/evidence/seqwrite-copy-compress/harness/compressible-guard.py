#!/usr/bin/env python3
"""Matched mount-level zero-data guard for the compression probe."""
import hashlib
import json
import os
import subprocess
import time

import yaml
import campaign

root = campaign.ROOT
subprocess.run(['python3', str(root / 'harness/setup-compose-guard.py')], check=True)
config = yaml.safe_load((root / 'harness/guard-compose.yml').read_text())
config['services']['perf']['environment']['PERF_FIO_SEQWRITE_ARGS'] = (
    '--zero_buffers=1 --scramble_buffers=0 --refill_buffers=1')
config_path = root / 'harness/compressible-compose.yml'
config_path.write_text(yaml.safe_dump(config, sort_keys=False))
for pair in range(1, 4):
    order = ['baseline', 'candidate'] if pair % 2 else ['candidate', 'baseline']
    for variant in order:
        campaign.reset()
        label = f'compressible-{pair:02d}-{variant}'
        destination = root / 'compose' / label
        destination.mkdir(parents=True, exist_ok=False)
        binary = root / 'bin' / f'brewfs-{variant}'
        env = dict(os.environ, SEQWRITE_ARM=label, SEQWRITE_BINARY=str(binary),
                   SEQWRITE_SOURCE_DIRTY='true' if variant == 'candidate' else 'false',
                   SEQWRITE_TOOLS='fio-seqwrite')
        command = ['docker', 'compose', '-f', str(config_path),
                   'run', '--rm', '--no-deps', 'perf']
        started = time.time()
        with (destination / 'runner-console.log').open('w') as log:
            result = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT)
        (destination / 'status.json').write_text(json.dumps({
            'status': result.returncode, 'started': started, 'finished': time.time(),
            'command': command,
            'binary_sha256': hashlib.file_digest(binary.open('rb'), 'sha256').hexdigest(),
            'pair': pair, 'variant': variant, 'stage': 'compressible',
            'fio_extra': config['services']['perf']['environment']['PERF_FIO_SEQWRITE_ARGS'],
        }, indent=2) + '\n')
        if result.returncode:
            raise RuntimeError(f'{label} failed: inspect {destination}')
campaign.reset()
