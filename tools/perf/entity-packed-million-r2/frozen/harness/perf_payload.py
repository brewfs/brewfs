"""Identical application work for BrewFS and JuiceFS; mount/drain stays external."""
import concurrent.futures
import json
import mmap
import os
from pathlib import Path
import subprocess
import time


def fio_base(corpus, profile, workers):
    return ['/usr/bin/fio', '--name=entity-randrw', '--directory=' + str(corpus),
        '--filename_format=randrw.$jobnum', '--size=' + str(profile['file_bytes']),
        '--ioengine=psync', '--iodepth=1', '--numjobs=' + str(workers),
        '--fallocate=none', '--direct=' + str(profile['direct']), '--group_reporting=1',
        '--verify=crc32c', '--verify_interval=4k',
        '--verify_state_save=0', '--verify_state_load=0',
        '--verify_fatal=1', '--output-format=json']


def fio_execute(command, output):
    command = command + ['--output=' + str(output)]
    output.with_suffix('.command.json').write_text(json.dumps(command, indent=2) + '\n')
    result = subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=180)
    if result.returncode:
        raise RuntimeError('fio failed: ' + result.stderr.decode(errors='replace') + '\n' + (output.read_text() if output.exists() else ''))
    text = output.read_text()
    # fio 3.36 can prefix JSON with its generic multi-writer verification warning.
    # Keep the original file, parse the following JSON, and still require error=0.
    job = json.loads(text[text.index('{'):])['jobs'][0]
    assert job['error'] == 0
    return job


def seed(corpus, profile, workers, art):
    # 1 MiB requests preserve the previous seeding geometry; each 4 KiB sector
    # carries a CRC header so random reads and the remote-only check can verify it.
    fio_execute(fio_base(corpus, profile, workers) +
        ['--rw=write', '--bs=' + profile.get('seed_request_bytes', '1m'), '--do_verify=0'] + (['--fsync=1'] if profile.get('seed_fsync_each_write', True) else ['--end_fsync=1']), art / 'fio-seed.json')
    assert {path.name for path in corpus.glob('randrw.*')} == {f'randrw.{index}' for index in range(workers)}


def run(workload, corpus, profile, workers, art):
    if profile.get('kind') == 'fio':
        job = fio_execute(fio_base(corpus, profile, workers) + [
            '--rw=' + profile.get('rw', 'randrw'), '--rwmixread=70', '--bs=4k', '--allow_file_create=0',
            '--time_based=1', '--runtime=' + str(profile['runtime_s']),
            '--randrepeat=1', '--randseed=42', '--fsync=1'], art / 'fio.json')
        direction = 'read' if profile.get('rw') == 'randread' else 'write'
        return [job[direction]['clat_ns']['percentile'][percentile]/1e6
            for percentile in ['50.000000', '95.000000']], job
    def write(index):
        begin = time.perf_counter()
        if profile.get('direct_app_io'):
            data = workload.payload(index, profile['file_bytes'])
            with mmap.mmap(-1, len(data)) as aligned:
                aligned[:] = data
                fd = os.open(workload.file_path(corpus, index), os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_DIRECT, 0o644)
                try:
                    view = memoryview(aligned)
                    try:
                        offset = 0
                        while offset < len(view): offset += os.write(fd, view[offset:])
                        os.fsync(fd)
                    finally: view.release()
                finally: os.close(fd)
        else:
            workload.write_one(corpus, index, profile['file_bytes'], True)
        return (time.perf_counter() - begin) * 1000
    with concurrent.futures.ThreadPoolExecutor(workers) as pool:
        return list(pool.map(write, range(profile['files']))), None


def verify_files(workload, corpus, profile, workers, art):
    if profile.get('kind') == 'fio':
        job = fio_execute(fio_base(corpus, profile, workers) +
            ['--rw=read', '--bs=' + ('1m' if profile.get('half_warm') else '4k'), '--verify_only=1', '--allow_file_create=0'], art / 'fio-remote-verify.json')
        assert job['read']['io_bytes'] == workers * profile['file_bytes']
    else:
        with concurrent.futures.ThreadPoolExecutor(workers) as pool:
            list(pool.map(lambda index: workload.read_one(corpus, index, profile['file_bytes']), range(profile['files'])))


def warmup(corpus, profile, workers, art):
    if not profile.get('half_warm'):
        return None
    assert workers == profile['files'] and workers % 2 == 0
    count = workers // 2
    started = time.perf_counter()
    job = fio_execute(fio_base(corpus, profile, count) + [
        '--rw=read', '--bs=1m', '--verify_only=1', '--allow_file_create=0'], art / 'fio-warmup.json')
    warmed = count * profile['file_bytes']
    assert job['read']['io_bytes'] == warmed and job['write']['io_bytes'] == 0
    result = dict(files=[f'randrw.{index}' for index in range(count)], bytes=warmed,
        working_set_bytes=workers * profile['file_bytes'], fraction=0.5,
        direct=profile['direct'], verified=True, elapsed_s=time.perf_counter()-started,
        note='Half of the complete files pre-read; this does not promise a 50% measured hit ratio.')
    (art / 'warmup.json').write_text(json.dumps(result, indent=2) + '\n')
    return result


def host_snapshot(pid):
    result = {'monotonic_s': time.monotonic()}
    for name in ['stat', 'meminfo', 'loadavg', 'pressure/cpu', 'pressure/io', 'pressure/memory',
                 f'{pid}/stat', f'{pid}/status', f'{pid}/io']:
        try:
            result[name] = (Path('/proc') / name).read_text()
        except FileNotFoundError:
            result[name] = None
    return result


def fuse_connections(pid, mount):
    """Identify only this exact newly created mount; never enumerate connections.

    Linux 6.8 lacks the newer fuse_connection fdinfo field. Its fusectl directory
    uses fc->dev (fs/fuse/control.c); for an ordinary FUSE mount the major is zero.
    Require both the exact mountinfo record and an owned process's FUSE fd.
    """
    owned = Path(f'/proc/{pid}')
    assert owned.stat().st_uid == os.getuid()
    matches = [line.split() for line in Path('/proc/self/mountinfo').read_text().splitlines()
               if line.split()[4] == str(mount)]
    assert len(matches) == 1, ('missing or ambiguous exact mount', mount)
    fields = matches[0]
    filesystem = fields[fields.index('-') + 1]
    assert filesystem == 'fuse' or filesystem.startswith('fuse.')
    major, minor = map(int, fields[2].split(':'))
    device = os.stat(mount).st_dev
    assert major == os.major(device) == 0 and minor == os.minor(device)
    descriptors = []
    for fd in (owned / 'fd').iterdir():
        try:
            if os.readlink(fd) == '/dev/fuse':
                descriptors.append(fd.name)
        except FileNotFoundError:
            pass
    assert descriptors, ('mount process must hold its FUSE fd', pid)
    advertised = set()
    for fd in descriptors:
        for line in (owned / 'fdinfo' / fd).read_text().splitlines():
            if line.startswith('fuse_connection:'):
                advertised.add(line.split(':', 1)[1].strip())
    connection = str(minor)
    if advertised:
        assert advertised == {connection}
    control = Path('/sys/fs/fuse/connections') / connection
    assert control.is_dir() and control.stat().st_uid == os.getuid()
    return [connection]
