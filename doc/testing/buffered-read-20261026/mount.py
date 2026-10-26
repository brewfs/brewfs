import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

root = Path('/data/metacache/artifacts/buffered-read-20261026')
p = argparse.ArgumentParser()
p.add_argument('action', choices=['start', 'stop'])
p.add_argument('label', choices=['off', 'on'])
a = p.parse_args()
run = root / a.label
mount = run / 'mnt'
if a.action == 'start':
    assert not os.path.ismount(mount)
    credentials = json.loads(Path('/data/readcache/artifacts/services/credentials.json').read_text())
    env = os.environ.copy()
    for key in list(env):
        if key.lower().endswith('_proxy') or key.startswith('BREWFS_FUSE_') or key == 'BREWFS_CACHE_TTL_MS':
            env.pop(key)
    env.update(AWS_ACCESS_KEY_ID=credentials['access'], AWS_SECRET_ACCESS_KEY=credentials['secret'],
               AWS_REGION='us-east-1', AWS_DEFAULT_REGION='us-east-1', RUST_LOG='warn', NO_PROXY='127.0.0.1,localhost')
    with (run / 'mount.log').open('a') as log:
        proc = subprocess.Popen([str(root / 'brewfs'), 'mount', '--config', str(run / 'mount.yaml')],
                                env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    (run / 'mount.pid').write_text(str(proc.pid))
    deadline = time.monotonic() + 60
    while not os.path.ismount(mount):
        assert proc.poll() is None, (run / 'mount.log').read_text()
        assert time.monotonic() < deadline, 'mount timeout'
        time.sleep(.1)
    subprocess.run(['mountpoint', '-q', str(mount)], check=True)
    (run / 'binary.sha256').write_text(hashlib.file_digest((root / 'brewfs').open('rb'), 'sha256').hexdigest() + '\n')
    print('Verified', mount)
else:
    pid = int((run / 'mount.pid').read_text())
    subprocess.run(['mountpoint', '-q', str(mount)], check=True)
    subprocess.run(['fusermount3', '-u', str(mount)], check=True, timeout=30)
    deadline = time.monotonic() + 60
    while Path(f'/proc/{pid}/stat').exists():
        if Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()[0] == 'Z':
            break
        assert time.monotonic() < deadline, 'daemon drain timeout'
        time.sleep(.1)
    assert not os.path.ismount(mount)
    print('Cleanly unmounted', mount)
