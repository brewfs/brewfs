#!/usr/bin/env python3
"""Full block SHA-256 verification, including immediate fsync/close -> SIGKILL."""
import argparse, errno, hashlib, json, os, pathlib, random, signal, subprocess, time
import yaml
import campaign as c

h = c.h
ROOT = c.ROOT
STATE = c.STATE
BLOCK = 4096

def start(label, binary, metrics=True):
    directory = ROOT / 'correctness' / label
    directory.mkdir(parents=True, exist_ok=False)
    mount = STATE / 'mounts' / label
    mount.mkdir(parents=True, exist_ok=True)
    path = directory / 'mount.yaml'
    path.write_text(yaml.safe_dump(c.config('flat-v1', mount, STATE / 'cache' / label)))
    argv = [str(binary), 'mount', '--config', str(path)]
    if metrics:
        argv.append('--metrics')
    with (directory / 'daemon.log').open('w') as log:
        process = subprocess.Popen(argv, env=h.ENV, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    until = time.monotonic() + 60
    while not os.path.ismount(mount):
        if process.poll() is not None or time.monotonic() > until:
            raise RuntimeError(f'mount failed: {directory}')
        time.sleep(.05)
    h.save(directory / 'invocation.json', {'argv': argv, 'pid': process.pid})
    return mount, process, directory

def stop(mount, process, directory, kill=False, fd_to_close=None):
    if kill:
        os.kill(process.pid, signal.SIGKILL)
        process.wait(timeout=30)
        if fd_to_close is not None:
            try:
                os.close(fd_to_close)
            except OSError as error:
                # Linux can return ECONNABORTED from descriptor close after
                # the FUSE daemon was deliberately killed. The independent
                # remount and full SHA-256 readback remain mandatory below.
                if error.errno not in (errno.ENOTCONN, errno.ENODEV, errno.EIO, errno.ECONNABORTED):
                    raise
                h.save(directory / 'fd-close-after-kill.json', {
                    'errno': error.errno, 'message': str(error),
                    'expected_after_sigkill': True})
    result = subprocess.run(['fusermount3', '-u', str(mount)], capture_output=True, text=True, timeout=30)
    if not kill:
        process.wait(timeout=30)
    outcome = {'signal': 'SIGKILL' if kill else None, 'returncode': process.returncode,
               'unmount_status': result.returncode, 'unmount_stderr': result.stderr,
               'still_mounted': subprocess.run(['findmnt', '-rn', '-M', str(mount)],
                    stdout=subprocess.DEVNULL).returncode == 0}
    h.save(directory / 'teardown.json', outcome)
    assert result.returncode == 0 and not outcome['still_mounted'], outcome
    assert process.returncode == (-9 if kill else 0), outcome

def verify_file(path, expected):
    actual = []
    with path.open('rb', buffering=0) as stream:
        while data := stream.read(1 << 20):
            for offset in range(0, len(data), BLOCK):
                actual.append(hashlib.sha256(data[offset:offset+BLOCK]).hexdigest())
    assert actual == expected, f'block SHA-256 mismatch: {path}'
    return {'blocks': len(actual), 'all_sha256_match': True,
            'digest_list_sha256': hashlib.sha256(''.join(actual).encode()).hexdigest()}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=pathlib.Path, default=ROOT / 'bin/brewfs-candidate')
    parser.add_argument('--variant', choices=['off', 'on'], default='on')
    parser.add_argument('--label-prefix', default='')
    parser.add_argument('--no-metrics', action='store_true')
    parser.add_argument('--fuse-direct', choices=['0', '1'], default='1')
    args = parser.parse_args()
    h.ENV['BREWFS_WRITE_COALESCE_BYTES'] = '65536' if args.variant == 'on' else '0'
    h.ENV['BREWFS_WRITE_COALESCE_MS'] = '0'
    # The direct arm bypasses kernel page-cache copies. The buffered arm
    # additionally checks the usual FUSE close/fsync and remount path.
    h.ENV['BREWFS_FUSE_DIRECT_IO'] = args.fuse_direct
    c.reset()
    result = {'variant': args.variant, 'cases': {}, 'started_unix': time.time(),
              'fuse_direct': args.fuse_direct,
              'binary': str(args.binary),
              'binary_sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest()}
    for mode in ['fsync', 'close']:
        expected = [None] * 8192
        mount, process, directory = start(f'{args.label_prefix}{args.variant}-{mode}-before-kill', args.binary, not args.no_metrics)
        fd = os.open(mount / 'random', os.O_CREAT | os.O_RDWR, 0o600)
        rng = random.Random(20261102)
        offsets = list(range(8192))
        rng.shuffle(offsets)
        for index in offsets:
            data = rng.randbytes(BLOCK)
            assert os.pwrite(fd, data, index * BLOCK) == BLOCK
            assert os.pread(fd, BLOCK, index * BLOCK) == data, ('read-your-writes', index)
            expected[index] = hashlib.sha256(data).hexdigest()
        (mount / 'empty').touch()
        single = os.open(mount / 'single-byte', os.O_CREAT | os.O_WRONLY, 0o600)
        assert os.write(single, b'Z') == 1
        os.close(single)
        # No drain, sleep, or extra read between the successful durability
        # boundary and SIGKILL. The fsync case deliberately leaves fd open.
        if mode == 'fsync':
            os.fsync(fd)
        else:
            os.close(fd)
            fd = None
        boundary_unix = time.time()
        stop(mount, process, directory, kill=True, fd_to_close=fd)
        h.save(directory / 'expected-blocks-sha256.json', expected)
        mount, process, directory = start(f'{args.label_prefix}{args.variant}-{mode}-after-restart', args.binary, not args.no_metrics)
        check = verify_file(mount / 'random', expected)
        assert (mount / 'empty').stat().st_size == 0
        assert (mount / 'single-byte').read_bytes() == b'Z'
        h.save(directory / 'verification.json', check)
        stop(mount, process, directory)
        result['cases'][mode] = {**check, 'durability_boundary_unix': boundary_unix,
                                 'immediate_reads': 8192, 'empty_and_single_byte': True}
        c.reset()
    result['finished_unix'] = time.time()
    h.save(ROOT / 'correctness' / f'{args.label_prefix}{args.variant}-summary.json', result)

if __name__ == '__main__':
    main()
