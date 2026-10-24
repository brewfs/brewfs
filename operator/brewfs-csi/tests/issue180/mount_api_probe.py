"""Diagnostic only: run as root inside a new private mount namespace."""
import ctypes
import os
from pathlib import Path
import subprocess
import tempfile

libc = ctypes.CDLL(None, use_errno=True)
assert os.geteuid() == 0
assert os.readlink('/proc/self/ns/mnt') != os.readlink('/proc/1/ns/mnt')
assert not any(' shared:' in row.split(' - ')[0]
               for row in Path('/proc/self/mountinfo').read_text().splitlines())
assert os.uname().machine == 'x86_64', 'syscall numbers below are x86_64'


def call(*args):
    subprocess.run(args, check=True, capture_output=True)


def mount_id(fd):
    return int(next(row.split(':')[1] for row in
                    Path(f'/proc/self/fdinfo/{fd}').read_text().splitlines()
                    if row.startswith('mnt_id:')))


def mounts():
    return {int(row.split()[0]): row.split()[4]
            for row in Path('/proc/self/mountinfo').read_text().splitlines()}


failures = 0
for mode in ['proc_fd', 'move_then_unmount', 'move_shared_parent', 'beneath_overmount']:
    with tempfile.TemporaryDirectory(prefix='180-mount-api-') as base:
        root = Path(base)
        fds = []
        try:
            if mode == 'move_shared_parent':
                call('mount', '--bind', base, base)
                call('mount', '--make-shared', base)
            for name in ['owned', 'foreign', 'target', 'quarantine']:
                (root / name).mkdir()
            for name in ['owned', 'foreign']:
                call('mount', '-t', 'tmpfs', name, str(root / name))
            call('mount', '--bind', str(root / 'owned'), str(root / 'target'))
            target_fd = os.open(root / 'target', os.O_PATH | os.O_NOFOLLOW)
            fds.append(target_fd)
            owned_id = mount_id(target_fd)
            # Linux 5.2 open_tree with AT_EMPTY_PATH pins the fd directly.
            tree_fd = libc.syscall(428, target_fd, b'', os.O_CLOEXEC | 0x1000)
            assert tree_fd >= 0, ctypes.get_errno()
            fds.append(tree_fd)
            assert mount_id(tree_fd) == owned_id
            foreign_id = None
            if mode != 'move_shared_parent':
                call('mount', '--bind', str(root / 'foreign'), str(root / 'target'))
                foreign_fd = os.open(root / 'target', os.O_PATH)
                foreign_id = mount_id(foreign_fd)
                os.close(foreign_fd)
            if mode == 'proc_fd':
                result = libc.umount2(f'/proc/self/fd/{target_fd}'.encode(), 0)
                print(f'{mode}: umount2={result} foreign_present={foreign_id in mounts()}', flush=True)
                failures += foreign_id not in mounts()
            else:
                flags = 0x4  # MOVE_MOUNT_F_EMPTY_PATH
                if mode == 'beneath_overmount':
                    call('mount', '-t', 'tmpfs', 'quarantine-top', str(root / 'quarantine'))
                    flags |= 0x200  # Linux 6.5 MOVE_MOUNT_BENEATH
                result = libc.syscall(429, tree_fd, b'', -100, os.fsencode(root / 'quarantine'), flags)
                error = ctypes.get_errno() if result < 0 else 0
                print(f'{mode}: move_mount={result} errno={error} owned_at={mounts().get(owned_id)} foreign_at={mounts().get(foreign_id)}', flush=True)
                if mode == 'move_then_unmount' and result == 0:
                    for fd in fds:
                        os.close(fd)
                    fds = []
                    result = libc.umount2(os.fsencode(root / 'quarantine'), 0)
                    print(f'{mode}: umount2={result} foreign_present={foreign_id in mounts()}', flush=True)
                    failures += foreign_id not in mounts()
                elif mode == 'move_shared_parent':
                    assert result == -1 and error == 22
                elif mode == 'beneath_overmount':
                    assert result == -1 and error == 22 and foreign_id in mounts()
        finally:
            for fd in fds:
                os.close(fd)
            for _ in range(64):
                paths = [path for path in mounts().values()
                         if path == base or path.startswith(base + '/')]
                if not paths:
                    break
                path = max(paths, key=lambda p: (p.count('/'), len(p)))
                assert libc.umount2(path.encode(), 2) == 0, ctypes.get_errno()
            else:
                raise RuntimeError('fixture cleanup failed')
print(f'Unsafe candidate cases: {failures}/2; shared-parent and BENEATH restrictions confirmed.', flush=True)
raise SystemExit(1 if failures else 0)
