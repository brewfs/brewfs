#!/usr/bin/env python3
"""Compare pre-change, default, and observability binaries via compose.

Run only after the local CI gates, with no concurrent compiler/benchmark.
Three rounds use rotating order and the same deterministic, SHA256-verified
512 MiB corpus. The on build receives an HTTP scrape every second. The two
read profiles use direct I/O to exercise FUSE rather than the kernel cache.
LocalFS object storage and upload-before-commit make completion independent
of diagnostic debt counters, which are absent from the default build.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import socket
import statistics
import subprocess
import tempfile
import time


WRAPPER = r'''
import hashlib, json, os, pathlib, subprocess, threading, time, urllib.request
root = pathlib.Path(os.environ['BREWFS_ARTIFACT_DIR'])
root.mkdir(parents=True, exist_ok=True)
# The outer runner reserves BREWFS_WRITEBACK_MODE for its S3 profiles.
# Set the explicit LocalFS default inside the container instead.
os.environ['BREWFS_WRITEBACK_MODE'] = 'upload_before_commit'
with open('/usr/local/bin/brewfs', 'rb') as binary:
    (root / 'binary-sha256.txt').write_text(hashlib.file_digest(binary, 'sha256').hexdigest() + '\n')
(root / 'fio-version.txt').write_text(subprocess.check_output(['fio', '--version'], text=True))
stop = threading.Event()
thread = None
def scrape():
    successes = errors = 0
    while not stop.is_set():
        started = time.monotonic()
        try:
            with urllib.request.urlopen('http://127.0.0.1:9567/metrics', timeout=2) as response:
                payload = response.read()
            (root / 'http-metrics-last.prom').write_bytes(payload)
            successes += 1
        except Exception:
            errors += 1
        (root / 'http-scrapes.json').write_text(json.dumps({'successes': successes, 'errors': errors, 'interval_seconds': 1}))
        stop.wait(max(0, 1 - (time.monotonic() - started)))
if os.environ.get('BREWFS_METRICS') == 'true':
    thread = threading.Thread(target=scrape, daemon=True)
    thread.start()
result = subprocess.run(['bash', '/usr/local/bin/run_perf_in_container.sh'])
stop.set()
if thread is not None:
    thread.join(timeout=3)
raise SystemExit(result.returncode)
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('baseline', type=Path)
    parser.add_argument('default', type=Path)
    parser.add_argument('enabled', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--runtime', type=int, default=30)
    parser.add_argument('--rounds', type=int, default=3)
    parser.add_argument('--tools', default='fio-randread fio-seqread fio-randrw metaperf')
    parser.add_argument('--dockerfile', type=Path)
    args = parser.parse_args()
    if args.runtime < 1 or args.rounds < 3:
        parser.error('runtime must be positive; at least three rounds are required')
    binaries = {mode: getattr(args, mode).resolve() for mode in ['baseline', 'default', 'enabled']}
    if not all(binary.is_file() for binary in binaries.values()):
        parser.error('all three binaries must exist')
    repo = Path(__file__).resolve().parents[2]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if (output / 'runs.json').exists():
        parser.error('choose a new output directory; existing artifacts are preserved')
    compose_dir = repo / 'docker/compose-xfstests'
    artifact_dir = compose_dir / 'artifacts'
    target = repo / 'target/docker/brewfs'
    target.parent.mkdir(parents=True, exist_ok=True)
    wrapper = output / 'container-wrapper.py'
    wrapper.write_text(WRAPPER)
    config = (compose_dir / 'docker-compose.redis-perf.yml').read_text()
    config = re.sub(r'^    container_name:.*\n', '', config, flags=re.M)
    project = f'brewfs-obs-{os.getpid()}'
    config = config.replace('image: brewfs-xfstests-perf:local', f'image: {project}:local')
    config = config.replace('entrypoint: ["/usr/bin/tini", "--", "bash", "/usr/local/bin/run_perf_in_container.sh"]',
                            'entrypoint: ["/usr/bin/tini", "--", "python3", "/observability-wrapper.py"]')
    config = config.replace('      - ./artifacts:/artifacts',
                            '      - ./artifacts:/artifacts\n      - ' + json.dumps(f'{wrapper}:/observability-wrapper.py:ro'))
    with tempfile.NamedTemporaryFile(mode='w', prefix='.observability-', suffix='.yml', dir=compose_dir, delete=False) as file:
        file.write(config)
        compose = Path(file.name)
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        redis_port = listener.getsockname()[1]
    env = dict(COMPOSE_PROJECT_NAME=project, BREWFS_PERF_COMPOSE_FILE=str(compose),
               BREWFS_REUSE_HOST_BINARY='1', PERF_LOG_TO_CONSOLE='false', REDIS_HOST_PORT=str(redis_port),
               PERF_FIO_SIZE='512m', PERF_FIO_RUNTIME=str(args.runtime), PERF_FIO_DIRECT='1',
               PERF_FIO_RANDREAD_BS='4k', PERF_FIO_SEQREAD_BS='1m', PERF_FIO_RANDRW_BS='4k',
               PERF_FIO_NUMJOBS='1', PERF_FIO_IODEPTH='16', PERF_FIO_IOENGINE='libaio',
               PERF_FIO_PREP_BS='1m', PERF_FIO_PREP_BUFFER_PATTERN='0x0123456789abcdef',
               PERF_FIO_PREP_VERIFY_SHA256='true', BREWFS_COMPRESSION='none',
               BREWFS_WRITEBACK_MODE='', BREWFS_READ_MEMORY_BYTES='1073741824',
               BREWFS_READ_SSD_BYTES='0', BREWFS_MEMORY_BUDGET_BYTES='2147483648',
               PERF_FIO_PREFILL_DRAIN='false', PERF_FIO_POST_WRITE_DRAIN='false',
               PERF_METADATA_POST_TOOL_DRAIN='false', PERF_FIO_COLD_READ='false',
               PERF_FIO_PREFILL_REMOUNT='false', PERF_METAPERF_SECONDS='5')
    if args.dockerfile:
        env['BREWFS_PERF_DOCKERFILE'] = str(args.dockerfile.resolve())
    parameters = dict(env)
    env = dict(os.environ, **env)
    source_names = subprocess.check_output(
        ['git', 'ls-files', '--cached', '--others', '--exclude-standard'], cwd=repo, text=True).splitlines()
    source_hashes = {name: hashlib.sha256((repo / name).read_bytes()).hexdigest()
                     for name in source_names
                     if name.startswith('src/') or name in ['Cargo.toml', 'Cargo.lock']}
    manifest = {'base_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip(),
                'tracked_diff_sha256': hashlib.sha256(subprocess.check_output(['git', 'diff', 'HEAD'], cwd=repo)).hexdigest(),
                'candidate_sources_sha256': source_hashes,
                'host': {'hostname': platform.node(), 'kernel': platform.release(), 'machine': platform.machine(), 'logical_cpus': os.cpu_count()},
                'binaries': {mode: {'path': str(path), 'sha256': hashlib.file_digest(path.open('rb'), 'sha256').hexdigest()} for mode, path in binaries.items()},
                'parameters': parameters, 'effective_writeback_mode': 'upload_before_commit',
                'tools': args.tools, 'rounds': args.rounds, 'compose_project': project}
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    records = []
    try:
        for round_index in range(args.rounds):
            modes = ['baseline', 'default', 'enabled']
            modes = modes[round_index % 3:] + modes[:round_index % 3]
            for mode in modes:
                env.pop('BREWFS_METRICS', None)
                env.pop('BREWFS_METRICS_ADDR', None)
                if mode == 'enabled': env['BREWFS_METRICS'] = 'true'
                shutil.copy2(binaries[mode], target)
                before = set(artifact_dir.glob('perf-run-*'))
                started = time.time()
                with (output / f'round-{round_index + 1}-{mode}.log').open('w') as log:
                    status = subprocess.run(['bash', 'docker/compose-xfstests/run_redis_perf.sh', '--local-fs', '--tools', args.tools], cwd=repo, env=env, stdout=log, stderr=subprocess.STDOUT).returncode
                artifacts = sorted(set(artifact_dir.glob('perf-run-*')) - before)
                record = {'round': round_index + 1, 'mode': mode, 'status': status, 'seconds': time.time() - started,
                          'artifacts': [str(path) for path in artifacts]}
                records.append(record)
                (output / 'runs.json').write_text(json.dumps(records, indent=2) + '\n')
                print(json.dumps(record), flush=True)
                if status != 0 or len(artifacts) != 1:
                    raise RuntimeError('compose run failed or produced an ambiguous artifact set')
                for scene in args.tools.split():
                    if not scene.startswith('fio-'):
                        continue
                    result_path = artifacts[0] / 'results' / f'{scene}.json'
                    jobs = json.loads(result_path.read_text())['jobs']
                    if not jobs or any(job['error'] != 0 or
                                       max(job['read']['runtime'], job['write']['runtime']) < args.runtime * 990
                                       for job in jobs):
                        raise RuntimeError(f'{scene} failed or was interrupted: {result_path}')
                if 'metaperf' in args.tools.split():
                    log = (artifacts[0] / 'tools/metaperf.log').read_text()
                    operations = re.findall(r'^(create|open|stat|readdir|rename):.*?ops/sec=', log, re.M)
                    if set(operations) != {'create', 'open', 'stat', 'readdir', 'rename'}:
                        raise RuntimeError('metaperf did not complete all five operations')
                actual_hash = (artifacts[0] / 'binary-sha256.txt').read_text().strip()
                if actual_hash != manifest['binaries'][mode]['sha256']:
                    raise RuntimeError('container did not execute the selected binary')
                if mode == 'enabled':
                    scrapes = json.loads((artifacts[0] / 'http-scrapes.json').read_text())
                    if scrapes['successes'] == 0 or 'brewfs_fuse_op_total' not in (artifacts[0] / 'http-metrics-last.prom').read_text():
                        raise RuntimeError('enabled run did not serve metrics during measurement')
    finally:
        subprocess.run(['docker', 'compose', '-f', str(compose), 'down', '-v', '--remove-orphans'], cwd=repo, env=env, check=False)
        compose.unlink(missing_ok=True)
    summary = {}
    corpus = set()
    for record in records:
        artifact = Path(record['artifacts'][0])
        checksums = list(artifact.glob('tools/*-prepare.sha256'))
        if len(checksums) != len([tool for tool in args.tools.split() if tool.startswith('fio-')]):
            raise RuntimeError(f'missing corpus verification in {artifact}')
        for checksum in checksums:
            corpus.update(line.split()[0] for line in checksum.read_text().splitlines())
        for scene in ['fio-randread', 'fio-seqread', 'fio-randrw']:
            path = artifact / 'results' / f'{scene}.json'
            if not path.exists(): continue
            jobs = json.loads(path.read_text())['jobs']
            bandwidth = sum(job['read']['bw_bytes'] + job['write']['bw_bytes'] for job in jobs) / 1048576
            summary.setdefault(scene, {}).setdefault(record['mode'], []).append(bandwidth)
    if len(corpus) != 1:
        raise RuntimeError(f'corpus hash mismatch or missing verification: {sorted(corpus)}')
    for values in summary.values():
        if any(len(values.get(mode, [])) != args.rounds for mode in binaries):
            raise RuntimeError('missing measurement rounds')
        values['median_mib_s'] = {mode: statistics.median(values[mode]) for mode in binaries}
        base = values['median_mib_s']['baseline']
        values['throughput_delta_pct'] = {mode: (values['median_mib_s'][mode] / base - 1) * 100 for mode in ['default', 'enabled']}
    (output / 'summary.json').write_text(json.dumps({'corpus_sha256': list(corpus), 'scenes': summary}, indent=2) + '\n')


if __name__ == '__main__':
    main()
