#!/usr/bin/env python3
"""Matched current-main baseline/candidate, with original fio and strict drain."""
import argparse
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import time
import urllib.request

import boto3
import legacy_run as h

ROOT = pathlib.Path('/home/hxy/perf-seqwrite-20261102')
STATE = pathlib.Path('/home/hxy/perf-seqwrite-state-20261102')
SOURCE = pathlib.Path('/home/hxy/perf-full-state-20261101/source/large')
h.STATE = STATE
h.ENDPOINT = 'http://127.0.0.1:29020'
h.BUCKET = 'brewfs-seqwrite-20261102'
h.ENV['BREWFS_WRITE_COALESCE_BYTES'] = '65536'
h.ENV['BREWFS_WRITE_COALESCE_MS'] = '0'
original_config = h.config
original_fio_args = h.fio_args
runtime = 20
direct_guard = False


def config(*args, **kwargs):
    cfg = original_config(*args, **kwargs)
    cfg['meta']['redis']['url'] = 'redis://127.0.0.1:26381/0'
    return cfg


def fio_args(*args, **kwargs):
    result = original_fio_args(*args, **kwargs)
    result = [f'--runtime={runtime}' if arg.startswith('--runtime=') else arg for arg in result]
    if direct_guard:
        result = ['--direct=1' if arg.startswith('--direct=') else arg for arg in result]
    return result


h.config = config
h.fio_args = fio_args


def backend_up():
    for name in ['objects', 'redis', 'mounts', 'cache']:
        (STATE / name).mkdir(parents=True, exist_ok=True)
    os.chown(STATE / 'objects', 10001, 10001)
    subprocess.run(['docker', 'compose', '-f', str(ROOT / 'harness/compose.yml'),
                    'up', '-d', '--wait', 'redis', 'rustfs'], check=True)


def reset():
    assert str(STATE) == '/home/hxy/perf-seqwrite-state-20261102'
    assert str(STATE / 'mounts') not in pathlib.Path('/proc/self/mountinfo').read_text()
    compose = ['docker', 'compose', '-f', str(ROOT / 'harness/compose.yml')]
    subprocess.run([*compose, 'stop', '-t', '10', 'rustfs'], check=True, capture_output=True)
    objects = STATE / 'objects'
    if objects.exists():
        shutil.rmtree(objects)
    objects.mkdir()
    os.chown(objects, 10001, 10001)
    subprocess.run(['redis-cli', '-p', '26381', 'FLUSHDB'], check=True, capture_output=True)
    subprocess.run([*compose, 'up', '-d', '--no-deps', '--wait', 'rustfs'], check=True, capture_output=True)
    s3 = boto3.client('s3', endpoint_url=h.ENDPOINT, aws_access_key_id='rustfsadmin',
                      aws_secret_access_key='rustfsadmin', region_name='us-east-1')
    s3.create_bucket(Bucket=h.BUCKET)


def seed(mount, workload):
    readonly = workload in ['seqread', 'randread']
    destination = mount / ('large' if readonly else 'writes')
    destination.mkdir(exist_ok=True)
    jobs = 4 if readonly or workload != 'seqwrite' else 1
    for job in range(jobs):
        name = f'read.{job}' if readonly else f'{workload}.{job}'
        with (SOURCE / f'read.{job}').open('rb') as src, (destination / name).open('wb') as dst:
            shutil.copyfileobj(src, dst, 1 << 20)
            dst.flush()
            os.fsync(dst.fileno())
    h.drain(mount, h.ROOT / 'preparation/drain')


def run_arm(variant, workload, phase, pair, seconds=20, direct=False):
    global runtime, direct_guard
    runtime, direct_guard = seconds, direct
    binary = ROOT / 'bin' / f'brewfs-{variant}'
    label = workload + ('-direct' if direct else '')
    h.ROOT = ROOT / 'rounds' / label / f'{phase}-{pair:02d}-{variant}'
    h.ROOT.mkdir(parents=True, exist_ok=False)
    reset()
    with h.mounted('flat-v1', f'{label}-{phase}-{pair}-{variant}', binary=binary) as (mount, pid, directory):
        seed(mount, workload)
        if phase == 'profile':
            # Warm the exact same mounted workload; diagnostic samples are separate
            # from the 20-second A/B acceptance rounds.
            h.run_round(variant, workload, mount, pid, 'warmup', 0)
        measured_phase = 'measure' if phase == 'profile' else phase
        result = h.run_round(variant, workload, mount, pid, measured_phase, pair)
    h.save(h.ROOT / 'complete.json', {
        'binary': str(binary), 'binary_sha256': hashlib.file_digest(binary.open('rb'), 'sha256').hexdigest(),
        'variant': variant, 'workload': label, 'phase': phase, 'pair': pair,
        'runtime': runtime, 'direct': direct, 'summary': result, 'completed_unix': time.time()})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', choices=['profile', 'ab', 'guard', 'up', 'reset'])
    parser.add_argument('--variant', choices=['baseline', 'candidate'], default='baseline')
    parser.add_argument('--index', type=int, default=1)
    parser.add_argument('--workloads', nargs='+', default=['seqwrite', 'randwrite', 'randread', 'seqread', 'randrw'])
    args = parser.parse_args()
    if args.stage == 'up':
        backend_up()
    elif args.stage == 'reset':
        reset()
    elif args.stage == 'profile':
        run_arm(args.variant, 'seqwrite', 'profile', args.index, seconds=30)
    elif args.stage == 'ab':
        # Alternate order to reduce temporal bias. Exactly three pairs per scene.
        h.save(ROOT / 'ab-protocol.json', {'pairs': 3, 'runtime': 20,
                'order': [['baseline', 'candidate'], ['candidate', 'baseline'], ['baseline', 'candidate']],
                'workloads': args.workloads, 'confidence': 'paired log-ratio Student t, df=2; 95% two-sided'})
        for workload in args.workloads:
            for pair in range(1, 4):
                order = ['baseline', 'candidate'] if pair % 2 else ['candidate', 'baseline']
                for variant in order:
                    run_arm(variant, workload, 'measure', pair)
            (ROOT / f'{workload}-ab.finished').write_text(str(time.time()) + '\n')
    else:
        for workload in ['seqwrite', 'randwrite', 'randrw']:
            for variant in ['baseline', 'candidate']:
                run_arm(variant, workload, 'guard', 1, direct=True)


if __name__ == '__main__':
    main()
