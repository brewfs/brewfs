"""Stock JuiceFS on the same owned object service and application payloads."""
from collections import Counter
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import traceback
import uuid

import perf_payload

BINARY = os.environ.get('ENTITY_JUICEFS_BINARY', str(Path(__file__).resolve().parent.parent / 'bin/juicefs'))


def dump(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def mounted(path):
    return subprocess.run(['mountpoint', '-q', str(path)]).returncode == 0


def wait_mount(proc, mount, log):
    for _ in range(2400):
        if proc.poll() is not None: raise RuntimeError('JuiceFS mount exited: ' + log.read_text())
        if mounted(mount): return
        time.sleep(.025)
    raise TimeoutError('JuiceFS mount readiness')


def metrics(mount):
    path = next((mount / name for name in ['.stats', '.jfs.stats'] if (mount / name).exists()), None)
    assert path is not None, 'JuiceFS statistics missing'
    result = {}
    for line in path.read_text().replace('\0', '').splitlines():
        parts = line.split()
        if len(parts) != 2 or line.startswith('#'): continue
        name = parts[0].split('{', 1)[0]
        result[name] = result.get(name, 0) + float(parts[1])
    required = ['juicefs_staging_blocks', 'juicefs_staging_block_bytes', 'juicefs_object_request_uploading']
    assert all(name in result for name in required), 'Never treat a missing drain metric as zero: ' + str(result.keys())
    return result


def pending(mount):
    current = metrics(mount)
    return dict(objects=int(current['juicefs_staging_blocks']), bytes=int(current['juicefs_staging_block_bytes']),
        uploading=int(current['juicefs_object_request_uploading']))


def wait_drain(mount, cache, timeout=180):
    deadline = time.monotonic() + timeout
    consecutive = 0
    while True:
        debt = pending(mount)
        assert debt['bytes'] <= 1073741824, 'JuiceFS exceeded this experiment staging budget'
        if not any(debt.values()):
            leftovers = [path for directory in cache.rglob('rawstaging') for path in directory.rglob('*') if path.is_file()]
            if not leftovers:
                consecutive += 1
                if consecutive == 2: return debt
        else: consecutive = 0
        if time.monotonic() > deadline: raise TimeoutError('JuiceFS strong drain: ' + str(debt))
        time.sleep(.002)


def ordinary_unmount(mount, log=None):
    started = time.monotonic()
    deadline = started + 5
    attempts = []
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('JuiceFS ordinary unmount remained busy for five seconds')
        try:
            result = subprocess.run([BINARY, 'umount', str(mount)],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=remaining,
                env={**os.environ, 'LC_ALL':'C'})
        except subprocess.TimeoutExpired as error:
            output = error.stdout or b''
            if isinstance(output, bytes): output = output.decode(errors='replace')
            attempts.append(dict(returncode=None, output=output, timed_out=True,
                                 elapsed_s=time.monotonic()-started))
            if log is not None: dump(log, attempts)
            raise TimeoutError('JuiceFS ordinary unmount command exceeded its deadline') from error
        output = result.stdout.decode(errors='replace')
        attempts.append(dict(returncode=result.returncode, output=output,
                             elapsed_s=time.monotonic()-started))
        if log is not None: dump(log, attempts)
        if result.returncode == 0: return
        # Only retry ordinary EBUSY; no force/lazy unmount and no hidden failure.
        if 'Device or resource busy' not in output:
            raise AssertionError(output)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('JuiceFS ordinary unmount remained busy for five seconds')
        time.sleep(min(.05, remaining))


def stop(proc, mount, *, drained=False, unmount_log=None):
    # Capture the child's exact connection before unmount hides it. Never
    # infer ownership from a host-wide connection inventory.
    connections = perf_payload.fuse_connections(proc.pid, mount) if mounted(mount) else []
    if unmount_log is not None: dump(unmount_log, [])
    if mounted(mount):
        ordinary_unmount(mount, unmount_log)
    assert not mounted(mount)
    started = time.monotonic()
    assisted = []
    while proc.poll() is None:
        now = time.monotonic()
        if drained and now-started >= .1 and not assisted:
            for connection in connections:
                control = Path('/sys/fs/fuse/connections') / connection
                try:
                    waiting = int((control / 'waiting').read_text())
                    assert waiting == 0, 'never abort a connection with pending kernel requests'
                    (control / 'abort').write_text('1\n')
                    assisted.append(dict(connection=connection, idle_kernel_requests=waiting,
                        after_unmount_s=now-started))
                except FileNotFoundError: pass
        if now-started > 120: raise TimeoutError('JuiceFS process exit after unmount; invalid cell')
        time.sleep(.002)
    returncode = proc.wait()
    assert returncode == 0 and not mounted(mount), ('JuiceFS exit', returncode)
    return assisted


def mount_command(meta, mount, cache, mode, workers, readonly=False, read_cache_bytes=0):
    # The minimum positive disk cache keeps writeback enabled but cannot retain
    # any complete block as a clean read cache. Staging is separately observed.
    command = [BINARY, 'mount', '--no-usage-report', '--no-bgjob', '--no-syslog',
        '--backup-meta=0', '--attr-cache=0', '--entry-cache=0', '--dir-entry-cache=0',
        '--open-cache=0', '--prefetch=0', '--max-readahead=0', '--buffer-size=128M',
        '--cache-size=' + ('0' if readonly or mode == 'upload_before_commit' else f'{read_cache_bytes}B' if read_cache_bytes else '1B'),
        '--cache-dir=' + ('memory' if read_cache_bytes else str(cache)),
        '--verify-cache-checksum=full', '--max-fuse-io=1M',
        '--max-uploads=' + str(8 if mode == 'commit_before_upload' else workers),
        '--max-stage-write=16', '--upload-delay=0', '--metrics=127.0.0.1:0']
    if read_cache_bytes:
        assert mode == 'upload_before_commit' and not readonly
    if readonly: command.append('--read-only')
    elif mode == 'commit_before_upload': command.append('--writeback')
    return command + [meta, str(mount)]
