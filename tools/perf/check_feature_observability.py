#!/usr/bin/env python3
"""Check feature gating, real FUSE I/O, stats, and a real Prometheus scrape.

Run as root with Docker and fusermount3 available; fresh metadata roots are
owned by UID 0. Both binaries must be built from the same candidate sources.
Every mount, process and Prometheus container created here is cleaned up.
Use a new output directory to preserve all evidence from earlier attempts.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import threading
import time
import urllib.request

parser = argparse.ArgumentParser()
parser.add_argument('--default', type=Path, required=True)
parser.add_argument('--enabled', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--modes', nargs='+', choices=['default', 'feature-runtime-off', 'feature-on', 'custom-address'])
args = parser.parse_args()
root = args.output.resolve()
if root.exists() and any(root.iterdir()):
    parser.error('choose an empty output directory; earlier evidence is preserved')
root.mkdir(parents=True, exist_ok=True)
binaries = {'default': args.default.resolve(), 'enabled': args.enabled.resolve()}
env = dict(os.environ, RUST_LOG='warn', BREWFS_FUSE_READ_DIRECT_IO='1')
for name in ['BREWFS_METRICS', 'BREWFS_METRICS_ADDR']:
    env.pop(name, None)

def open_port(port):
    with socket.socket() as connection:
        connection.settimeout(.2)
        return connection.connect_ex(('127.0.0.1', port)) == 0

assert not open_port(9567)
summary = []
for mode, binary, enabled, port in [
    ('default', 'default', False, 9567),
    ('feature-runtime-off', 'enabled', False, 9567),
    ('feature-on', 'enabled', True, 9567),
    ('custom-address', 'enabled', True, 19567),
]:
    if args.modes and mode not in args.modes:
        continue
    directory = root / mode
    mount = directory / 'mount'
    mount.mkdir(parents=True, exist_ok=True)
    config = directory / 'mount.yaml'
    config.write_text(f'''mount_point: {mount}
data:
  backend: local-fs
  localfs:
    data_dir: {directory / 'objects'}
meta:
  backend: sqlx
  sqlx:
    url: "sqlite://{directory / 'metadata.db'}?mode=rwc"
cache:
  cache_root: {directory / 'cache'}
  read_memory_bytes: 0
  read_ssd_bytes: 0
  prefetch_enabled: false
  populate_write_cache_after_upload: false
  writeback_mode: upload_before_commit
''')
    command = [str(binaries[binary]), 'mount', '--config', str(config)]
    if enabled:
        command += ['--metrics', '--metrics-addr', f'127.0.0.1:{port}']
    prometheus = None
    stop = threading.Event()
    io_thread = None
    io_errors = []
    with (directory / 'mount.log').open('w') as log:
        process = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            for _ in range(600):
                if os.path.ismount(mount): break
                if process.poll() is not None: raise RuntimeError((directory / 'mount.log').read_text())
                time.sleep(.1)
            assert os.path.ismount(mount), 'mount did not become ready'
            assert open_port(port) == enabled
            if port != 9567: assert not open_port(9567)
            ss = subprocess.check_output(['ss', '-ltnp'], text=True)
            if not enabled:
                assert f'pid={process.pid},' not in ss, 'disabled mount opened a TCP listener'
            (directory / 'ss.txt').write_text(ss)
            (directory / 'ss-9567.txt').write_text('\n'.join(line for line in ss.splitlines() if ':9567 ' in line))
            payload = bytes(range(256)) * 256
            with (mount / 'smoke-data').open('wb') as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            assert (mount / 'smoke-data').read_bytes() == payload
            try: (mount / 'missing-file').stat()
            except FileNotFoundError: pass
            else: raise AssertionError('missing lookup did not fail')
            if mode == 'default':
                assert not (mount / '.stats').exists()
                (mount / '.stats').write_text('ordinary user file')
                assert (mount / '.stats').read_text() == 'ordinary user file'
            item = {'mode': mode, 'mount_pid': process.pid, 'port': port, 'listening': enabled}
            if enabled:
                time.sleep(.2)
                subprocess.run(['curl', '--fail', '--silent', '--show-error', '-D', str(directory / 'headers.txt'), '-o', str(directory / 'metrics.prom'), f'http://127.0.0.1:{port}/metrics'], check=True)
                text = (directory / 'metrics.prom').read_text()
                item['brewfs_lines'] = len([line for line in text.splitlines() if 'brewfs_' in line])
                assert item['brewfs_lines'] > 0
                assert 'brewfs_fuse_op_errors_total{op="lookup",errno="2"}' in text
                for family in ['brewfs_fuse_op_latency_seconds_bucket', 'brewfs_object_get_total', 'brewfs_metadata_op_total', 'brewfs_writeback_dirty_bytes', 'brewfs_mount_info']:
                    assert family in text, family
                samples = {line.rsplit(' ', 1)[0]: float(line.rsplit(' ', 1)[1]) for line in text.splitlines() if line and not line.startswith('#')}
                for family in ['brewfs_fuse_op_total', 'brewfs_object_get_total', 'brewfs_object_get_bytes_total', 'brewfs_object_put_total', 'brewfs_object_put_bytes_total', 'brewfs_metadata_op_total']:
                    assert sum(value for key, value in samples.items() if key == family or key.startswith(family + '{')) > 0, family
                with (directory / 'promtool.txt').open('w') as output:
                    subprocess.run(['docker', 'run', '--rm', '-i', '--entrypoint', '/bin/promtool', 'prom/prometheus:v3.5.0', 'check', 'metrics'], input=text, text=True, stdout=output, stderr=subprocess.STDOUT, check=True)
                def read_loop():
                    try:
                        while not stop.is_set():
                            assert (mount / 'smoke-data').read_bytes() == payload
                            time.sleep(.005)
                    except Exception as error:
                        io_errors.append(error)
                io_thread = threading.Thread(target=read_loop)
                io_thread.start()
                with (directory / 'stats.txt').open('w') as output:
                    subprocess.run([str(binaries[binary]), 'stats', '--address', f'127.0.0.1:{port}', '--interval', '.2', '--count', '2'], env=env, stdout=output, stderr=subprocess.STDOUT, check=True, timeout=15)
                assert 'P99 ms~' in (directory / 'stats.txt').read_text()
                if mode == 'feature-on':
                    prom_config = directory / 'prometheus.yml'
                    prom_config.write_text('global:\n  scrape_interval: 1s\nscrape_configs:\n  - job_name: brewfs\n    static_configs:\n      - targets: ["127.0.0.1:9567"]\n')
                    prometheus = f'brewfs-obs-prom-{os.getpid()}'
                    subprocess.run(['docker', 'run', '--rm', '-d', '--name', prometheus, '--network', 'host', '-v', f'{prom_config}:/etc/prometheus/prometheus.yml:ro', 'prom/prometheus:v3.5.0', '--config.file=/etc/prometheus/prometheus.yml', '--web.listen-address=127.0.0.1:19090'], check=True, stdout=subprocess.DEVNULL)
                    for _ in range(100):
                        try:
                            with urllib.request.urlopen('http://127.0.0.1:19090/api/v1/query?query=up', timeout=1) as response:
                                value = json.load(response)
                            rows = value['data']['result']
                            if any(row['metric'].get('job') == 'brewfs' and row['value'][1] == '1' for row in rows):
                                (directory / 'prometheus-up.json').write_text(json.dumps(value, indent=2) + '\n')
                                item['prometheus_up'] = 1
                                break
                        except Exception: pass
                        time.sleep(.2)
                    assert item.get('prometheus_up') == 1
            summary.append(item)
        finally:
            stop.set()
            if io_thread: io_thread.join(timeout=5)
            if prometheus: subprocess.run(['docker', 'stop', prometheus], stdout=subprocess.DEVNULL, check=False)
            if os.path.ismount(mount):
                subprocess.run(['fusermount3', '-u', str(mount)], check=False)
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try: process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
            assert not os.path.ismount(mount)
            assert not open_port(port)
            assert not io_errors, io_errors
(root / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary, indent=2))
