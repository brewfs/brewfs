#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import subprocess
import time

import campaign

root = campaign.ROOT
parser = argparse.ArgumentParser()
parser.add_argument('stage', choices=['broad', 'ab', 'direct'])
args = parser.parse_args()
# Broad acceptance includes the repository runner's own direct-IO matrix.
# Keep the earlier mounted-host guard as additional diagnostic evidence.
if args.stage == 'broad':
    paths = [root / 'compose' / f'direct-01-{variant}' / 'status.json'
             for variant in ['baseline', 'candidate']]
    if not all(path.exists() and json.loads(path.read_text())['status'] == 0 for path in paths):
        subprocess.run(['python3', __file__, 'direct'], check=True)
subprocess.run(['python3', str(root / 'harness/setup-compose-guard.py')], check=True)
tools = 'fio-seqwrite fio-randwrite fio-randread fio-seqread fio-randrw'
for pair in range(1, (3 if args.stage == 'ab' else 1) + 1):
    order = ['baseline', 'candidate'] if pair % 2 else ['candidate', 'baseline']
    for variant in order:
        campaign.reset()
        label = f'{args.stage}-{pair:02d}-{variant}'
        destination = root / 'compose' / label
        destination.mkdir(parents=True, exist_ok=False)
        binary = root / 'bin' / f'brewfs-{variant}'
        env = dict(os.environ, SEQWRITE_ARM=label, SEQWRITE_BINARY=str(binary),
                   SEQWRITE_SOURCE_DIRTY='true' if variant == 'candidate' else 'false')
        if args.stage == 'ab':
            env['SEQWRITE_TOOLS'] = tools
        elif args.stage == 'direct':
            env['SEQWRITE_TOOLS'] = 'fio-seqwrite fio-randwrite fio-randrw'
            env['SEQWRITE_DIRECT'] = '1'
        command = ['docker', 'compose', '-f', str(root / 'harness/guard-compose.yml'),
                   'run', '--rm', '--no-deps', 'perf']
        started = time.time()
        with (destination / 'runner-console.log').open('w') as log:
            result = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT)
        (destination / 'status.json').write_text(json.dumps({
            'status': result.returncode, 'started': started, 'finished': time.time(),
            'command': command, 'binary_sha256': hashlib.file_digest(binary.open('rb'), 'sha256').hexdigest(),
            'pair': pair, 'variant': variant, 'stage': args.stage}, indent=2) + '\n')
        if result.returncode:
            raise RuntimeError(f'{label} failed: inspect {destination}')
campaign.reset()
