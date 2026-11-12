# Measurement script archive
Executable files used only outside the repository, under `/mnt/brewfs-data/home/hxy/smallfile-warm/ttl-20261104/`. They are recorded as documentation here; no product, test, or runner source is changed.
Restore each fenced block using its filename into that directory on a fresh reproduction host with the preserved parent harness and main executable. Original artifacts are never overwritten: both drivers refuse existing run directories. Use unique run IDs when repeating on the evidence host.
Run `prepare-raw.py`, then `run-matrix.sh`, `summarize.py`, `run-probes.sh`, and `collect.py` in that order. Start Redis/RustFS with the original compose.sh first; finish with `compose.sh down -v --remove-orphans`. No compilation, drop_caches, payload cache, or keep-cache changes are involved.

**prepare-raw.py**

```python
#!/usr/bin/env python3
"""Derive D from the exact Compose scanner; preserve its traversal/checks."""
import pathlib

source = pathlib.Path('/home/hxy/brewfs-main/docker/compose-xfstests/run_perf_in_container.sh').read_text()
source = source.split('    packed_smallfiles_scan() {\n', 1)[1].split("<<'PY'\n", 1)[1].split('\nPY\n', 1)[0]
old = '''            st = path.stat()
            if st.st_size != file_size:
                raise OSError(f"unexpected size {st.st_size}, expected {file_size}")
            with path.open("rb") as handle:
                payload = handle.read() if read_bytes <= 0 else handle.read(read_bytes)'''
new = '''            # D: no explicit stat or Python buffered-I/O fstat/isatty.
            assert read_bytes == 0
            fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
            try:
                chunks = []
                while True:
                    chunk = os.read(fd, file_size)
                    if not chunk:
                        break
                    chunks.append(chunk)
                payload = b"".join(chunks)
            finally:
                os.close(fd)'''
assert source.count(old) == 1
source = source.replace(old, new).replace('logical_bytes += st.st_size', 'logical_bytes += len(payload)')
pathlib.Path(__file__).with_name('raw-scanner.py').write_text(source + '\n')
```

**raw-scanner.py**

```python
import os
import pathlib
import sys
import time

root = pathlib.Path(sys.argv[1])
expected = int(sys.argv[2])
file_size = int(sys.argv[3])
read_bytes = int(sys.argv[4])
dir_levels = int(sys.argv[5])
dirs_per_level = int(sys.argv[6])
files_per_dir = int(sys.argv[7])
scan_mode = sys.argv[8]
expected_leaf_dirs = dirs_per_level ** dir_levels
if expected % expected_leaf_dirs:
    raise SystemExit(f"file count {expected} is not divisible by leaf directory count {expected_leaf_dirs}")
expected_files_per_leaf = expected // expected_leaf_dirs
if expected_files_per_leaf != files_per_dir:
    raise SystemExit(f"fixture layout mismatch files_per_leaf={expected_files_per_leaf} configured={files_per_dir}")
expected_tree_dirs = sum(dirs_per_level ** level for level in range(1, dir_levels + 1))
started = time.monotonic()
files = 0
tree_dirs = 0
leaf_dirs = 0
logical_bytes = 0
payload_bytes = 0
checksum = 0
errors = 0
for directory, dirs, names in os.walk(root):
    dirs.sort()
    relative = pathlib.Path(directory).relative_to(root)
    depth = len(relative.parts)
    child_dirs = dirs
    names = [name for name in names if not name.startswith(".")]
    if depth:
        tree_dirs += 1
    expected_children = dirs_per_level if depth < dir_levels else 0
    if len(child_dirs) != expected_children:
        raise SystemExit(f"directory fanout mismatch path={directory} depth={depth} actual={len(child_dirs)} expected={expected_children}")
    if depth < dir_levels:
        if names:
            raise SystemExit(f"unexpected files above leaf level path={directory} files={len(names)}")
        continue
    if depth != dir_levels:
        raise SystemExit(f"directory tree is deeper than configured levels path={directory} depth={depth} expected={dir_levels}")
    leaf_dirs += 1
    if len(names) != files_per_dir:
        raise SystemExit(f"leaf file count mismatch path={directory} actual={len(names)} expected={files_per_dir}")
    if scan_mode == "tree":
        files += len(names)
        continue
    for name in sorted(names):
        path = pathlib.Path(directory) / name
        try:
            # D: no explicit stat or Python buffered-I/O fstat/isatty.
            assert read_bytes == 0
            fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
            try:
                chunks = []
                while True:
                    chunk = os.read(fd, file_size)
                    if not chunk:
                        break
                    chunks.append(chunk)
                payload = b"".join(chunks)
            finally:
                os.close(fd)
            if read_bytes <= 0 and len(payload) != file_size:
                raise OSError(f"short read {len(payload)}, expected {file_size}")
            files += 1
            logical_bytes += len(payload)
            payload_bytes += len(payload)
            checksum = (checksum + (payload[0] if payload else 0)) & 0xffffffff
        except OSError as error:
            errors += 1
            print(f"error path={path} error={error}")
elapsed = time.monotonic() - started
mode = "tree" if scan_mode == "tree" else "full" if read_bytes <= 0 else f"prefix:{read_bytes}"
print(f"packed_smallfiles_summary files={files} expected={expected} directories={tree_dirs} expected_directories={expected_tree_dirs} leaf_directories={leaf_dirs} expected_leaf_directories={expected_leaf_dirs} levels={dir_levels} dirs_per_level={dirs_per_level} files_per_leaf={files_per_dir} file_size={file_size} read_mode={mode} logical_bytes={logical_bytes} payload_bytes={payload_bytes} errors={errors} checksum={checksum} seconds={elapsed:.6f} files_per_sec={files / elapsed if elapsed else 0:.2f}")
if files != expected or tree_dirs != expected_tree_dirs or leaf_dirs != expected_leaf_dirs or errors:
    raise SystemExit(1)
```

**capture.py**

```python
#!/usr/bin/env python3
"""Out-of-band counters; never touches the measured FUSE mount."""
import json
import pathlib
import sys
import time
import urllib.request

out = pathlib.Path(sys.argv[1])
label = sys.argv[2]
with urllib.request.urlopen('http://127.0.0.1:9567/metrics') as response:
    (out / (label + '.metrics')).write_bytes(response.read())
paths = [pathlib.Path(p) for p in (
    '/proc/sys/fs/dentry-state', '/proc/sys/fs/inode-state',
    '/proc/sys/fs/inode-nr', '/proc/loadavg', '/proc/stat',
    '/proc/pressure/cpu', '/proc/pressure/io', '/proc/pressure/memory')]
for connection in pathlib.Path('/sys/fs/fuse/connections').glob('*'):
    paths.extend(connection / name for name in ('waiting', 'max_background', 'congestion_threshold'))
values = {}
for path in paths:
    try:
        values[str(path)] = path.read_text().strip()
    except OSError as error:
        values[str(path)] = str(error)
(out / (label + '.kernel.json')).write_text(json.dumps(dict(time_ns=time.time_ns(), values=values), indent=2) + '\n')
```

**run-one.sh**

```bash
#!/usr/bin/env bash
set -euo pipefail
source <(sed '/^main "\$@"$/d' /usr/local/bin/run_perf_in_container.sh)
eval "$(declare -f run_logged_tool | sed '1s/run_logged_tool/run_logged_tool_original/')"
ttl_measure_command() {
    python3 /experiment/ttl-20261104/capture.py "$artifact_dir" measured-before
    "$@"
    # Same untimed async RELEASE allowance used by #416's attribute.py.
    sleep 0.05
    python3 /experiment/ttl-20261104/capture.py "$artifact_dir" measured-after
}
run_logged_tool() {
    local tool="$1"
    shift
    if [[ "$tool" == packed-smallfiles ]]; then
        if [[ "$TTL_ARM" == D ]]; then
            run_logged_tool_original "$tool" ttl_measure_command python3 /experiment/ttl-20261104/raw-scanner.py \
                "$mount_dir" 8192 4096 0 0 1 8192 full
        else
            run_logged_tool_original "$tool" ttl_measure_command "$@"
        fi
    else
        run_logged_tool_original "$tool" "$@"
    fi
}
main
python3 /experiment/post-scan.py "$artifact_dir"
```

**run-matrix.sh**

```bash
#!/usr/bin/env bash
set -euo pipefail
out=/mnt/brewfs-data/home/hxy/smallfile-warm
experiment="$out/ttl-20261104"
export SF_MANIFEST_KEY SF_BINARY=brewfs-main SF_RUN_ID SF_TOOLS='packed-tree packed-smallfiles'
export SF_DIRECT=1 SF_TRACE_RATE=307 SF_SOURCE_REVISION SF_SOURCE_DIRTY=0 SF_SOURCE_DIFF_SHA256=''
SF_SOURCE_REVISION=$(cat "$out/main-source.txt")
SF_MANIFEST_KEY=$(cat "$out/manifest-key.txt")
printf 'round\tarm\tttl_ms\trun_id\tstart_utc\tend_utc\n' >"$experiment/order.tsv"
for round in 1 2 3; do
    case "$round" in
        1) arms=(A B C D) ;;
        2) arms=(D C B A) ;;
        3) arms=(B A D C) ;;
    esac
    for arm in "${arms[@]}"; do
        case "$arm" in
            A) ttl=0 ;;
            B|D) ttl=1000 ;;
            C) ttl=10000 ;;
        esac
        SF_RUN_ID="ttl-20261104-$arm-r$round"
        test ! -e "$out/artifacts/$SF_RUN_ID"
        started=$(date -u +'%FT%TZ')
        printf '%s %s ttl=%s\n' "$started" "$SF_RUN_ID" "$ttl"
        bash "$out/compose.sh" run --rm --no-deps -e "BREWFS_CACHE_TTL_MS=$ttl" -e "TTL_ARM=$arm" \
            --entrypoint bash perf /experiment/ttl-20261104/run-one.sh >"$experiment/$SF_RUN_ID.log" 2>&1
        printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$round" "$arm" "$ttl" "$SF_RUN_ID" "$started" "$(date -u +'%FT%TZ')" >>"$experiment/order.tsv"
        tail -1 "$experiment/$SF_RUN_ID.log"
    done
done
touch "$experiment/matrix.done"
```

**stage-probe.py**

```python
#!/usr/bin/env python3
"""Untimed diagnostic: correlate syscall phases with exact FUSE op deltas."""
import json
import os
import pathlib
import re
import sys
import time
import urllib.request

root = pathlib.Path('/mnt/brewfs')
out = pathlib.Path(sys.argv[1])


def counters():
    with urllib.request.urlopen('http://127.0.0.1:9567/metrics') as response:
        text = response.read().decode()
    result = {}
    for line in text.splitlines():
        if line.startswith('brewfs_fuse_op_total{'):
            result[re.search(r'op="([^"]+)"', line)[1]] = int(line.split()[-1])
    return result


rows = []
def phase(mode, stage, action):
    before = counters()
    start = time.time_ns()
    value = action()
    end = time.time_ns()
    time.sleep(0.01)  # async RELEASE; this is not a latency benchmark
    after = counters()
    rows.append(dict(mode=mode, stage=stage, start_ns=start, end_ns=end,
                     op_delta={k:after[k]-v for k,v in before.items() if after[k]!=v}))
    return value


# Same tree warmup; chosen files lie beyond the initial READDIRPLUS batch.
walks = list(os.walk(root))
assert len([n for _, _, names in walks for n in names if not n.startswith('.')]) == 8192
for mode, filename in [('compose', 'f004000'), ('no-explicit-stat', 'f004001'), ('raw', 'f004002')]:
    path = root / filename
    if mode == 'compose':
        assert phase(mode, 'path.stat', path.stat).st_size == 4096
    if mode == 'raw':
        fd = phase(mode, 'os.open', lambda: os.open(path, os.O_RDONLY | os.O_CLOEXEC))
        payload = phase(mode, 'os.read-data', lambda: os.read(fd, 4096))
        assert len(payload) == 4096
        assert phase(mode, 'os.read-eof', lambda: os.read(fd, 4096)) == b''
        phase(mode, 'os.close', lambda: os.close(fd))
    else:
        handle = phase(mode, 'path.open-buffered', lambda: path.open('rb'))
        payload = phase(mode, 'handle.read-all', handle.read)
        assert len(payload) == 4096
        phase(mode, 'handle.close', handle.close)

# Trace sample rate=1, and fewer than 256 requests after the prewarm.
(out/'probe-after.stats').write_bytes((root/'.stats').read_bytes())
(out/'stages.json').write_text(json.dumps(rows, indent=2)+'\n')
print(json.dumps(rows, indent=2))
```

**run-probe.sh**

```bash
#!/usr/bin/env bash
set -euo pipefail
source <(sed '/^main "\$@"$/d' /usr/local/bin/run_perf_in_container.sh)
mkdir -p "$artifact_dir"
log_file="$artifact_dir/brewfs.log"
export BREWFS_LOG_FILE="$log_file"
trap on_exit EXIT INT TERM
raise_nofile_limit
write_config
prepare_artifacts
install_mount_helper
mount_brewfs
if [[ "${TTL_STRACE:-0}" == 1 ]]; then
    strace -qq -f -ttt -T -s 0 -e trace=%file,fstat,read,close,ioctl,lseek -e raw=ioctl \
        -o "$artifact_dir/scanner.strace" python3 /experiment/ttl-20261104/stage-probe.py "$artifact_dir"
else
    python3 /experiment/ttl-20261104/stage-probe.py "$artifact_dir"
fi
```

**run-probes.sh**

```bash
#!/usr/bin/env bash
set -euo pipefail
out=/mnt/brewfs-data/home/hxy/smallfile-warm
export SF_MANIFEST_KEY SF_BINARY=brewfs-main SF_RUN_ID SF_TOOLS=packed-smallfiles
export SF_DIRECT=1 SF_TRACE_RATE=1 SF_SOURCE_REVISION SF_SOURCE_DIRTY=0 SF_SOURCE_DIFF_SHA256=''
SF_SOURCE_REVISION=$(cat "$out/main-source.txt")
SF_MANIFEST_KEY=$(cat "$out/manifest-key.txt")
for ttl in 0 1000 10000; do
    SF_RUN_ID="ttl-20261104-stage-$ttl"
    test ! -e "$out/artifacts/$SF_RUN_ID"
    bash "$out/compose.sh" run --rm --no-deps -e "BREWFS_CACHE_TTL_MS=$ttl" \
        --entrypoint bash perf /experiment/ttl-20261104/run-probe.sh \
        >"$out/ttl-20261104/$SF_RUN_ID.log" 2>&1
done
SF_RUN_ID=ttl-20261104-syscalls-raw-0
test ! -e "$out/artifacts/$SF_RUN_ID"
bash "$out/compose.sh" run --rm --no-deps -e BREWFS_CACHE_TTL_MS=0 -e TTL_STRACE=1 \
    --entrypoint bash perf /experiment/ttl-20261104/run-probe.sh \
    >"$out/ttl-20261104/$SF_RUN_ID.log" 2>&1
```

**summarize.py**

```python
#!/usr/bin/env python3
"""Validate twelve fresh-mount runs and report between-run uncertainty."""
import csv
import hashlib
import json
import math
import pathlib
import re
import statistics

root = pathlib.Path('/mnt/brewfs-data/home/hxy/smallfile-warm')
out = root / 'ttl-20261104'
files = 8192
t95 = 4.302652729911275
expected_sha = 'e2a308519ad1e0f5d2295e10cf3fb9eb64ce57009eb64b01c87489b51128f34f'
ttls = dict(A=0, B=1000, C=10000, D=1000)
contract = dict(
    BREWFS_VOLUME_FORMAT='packed-snapshot',
    BREWFS_PACKED_MANIFEST_KEY=(root / 'manifest-key.txt').read_text().strip(),
    BREWFS_PACKED_METADATA_CACHE_BYTES='268435456',
    BREWFS_READ_MEMORY_BYTES='0', BREWFS_READ_SSD_BYTES='0',
    BREWFS_PREFETCH_ENABLED='false', BREWFS_RANGE_BACKGROUND_PREFETCH='false',
    BREWFS_FUSE_READ_DIRECT_IO='1', BREWFS_FUSE_KEEP_CACHE='0',
    BREWFS_COMPRESSION='none', BREWFS_REQUEST_TRACE='true',
    BREWFS_REQUEST_TRACE_SAMPLE_RATE='307')


def metrics(path):
    result = {}
    for line in path.read_text().splitlines():
        if line.startswith('brewfs_'):
            key, value = line.rsplit(' ', 1)
            result[key] = float(value)
    return result


def summary(values):
    mean = statistics.mean(values)
    margin = t95 * statistics.stdev(values) / math.sqrt(len(values))
    return dict(values=values, median=statistics.median(values), mean=mean,
                mean_ci95=[mean - margin, mean + margin],
                min=min(values), max=max(values), n=len(values))


rows = []
content_hashes = set()
for order in csv.DictReader((out / 'order.tsv').open(), delimiter='\t'):
    arm = order['arm']
    path = root / 'artifacts' / order['run_id']
    assert (path / 'perf.complete').exists(), path
    assert not list(path.glob('*.invalid')), path
    statuses = list(csv.DictReader((path / 'perf-summary.tsv').open(), delimiter='\t'))
    assert {r['tool'] for r in statuses} == {'packed-tree', 'packed-smallfiles'}
    assert all(r['status'] == 'pass' for r in statuses)
    manifest = json.loads((path / 'manifest.json').read_text())
    assert manifest['source']['binary_sha256'] == expected_sha
    assert manifest['source']['revision'] == (root / 'main-source.txt').read_text().strip()
    assert manifest['source']['dirty'] is False
    mounts = list((path / 'diagnostics').glob('mount-*.json'))
    assert len(mounts) == 1
    mount = json.loads(mounts[0].read_text())
    assert mount['binary_sha256'] == expected_sha
    environment = mount['environment']
    assert {k: environment.get(k) for k in contract} == contract
    assert environment['BREWFS_CACHE_TTL_MS'] == str(ttls[arm])
    stats = metrics(path / 'diagnostics/stats-packed-smallfiles-after.txt')
    assert stats['brewfs_fuse_attr_valid_ns'] == ttls[arm] * 1000000
    assert stats['brewfs_fuse_entry_valid_ns'] == ttls[arm] * 1000000
    line = next(l for l in (path / 'tools/packed-smallfiles.log').read_text().splitlines()
                if l.startswith('packed_smallfiles_summary '))
    fields = dict(re.findall(r'(\w+)=([^ ]+)', line))
    assert int(fields['files']) == files and int(fields['errors']) == 0
    assert int(fields['payload_bytes']) == 33554432
    assert int(fields['checksum']) == 1044480 and fields['read_mode'] == 'full'
    content = json.loads((path / 'sha256.json').read_text())
    assert len(content) == files
    content_hashes.add(hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest())
    before = metrics(path / 'measured-before.metrics')
    after = metrics(path / 'measured-after.metrics')
    delta = {k: after[k] - v for k, v in before.items() if k in after}
    ops = {re.search(r'op="([^"]+)"', k)[1]: int(v) for k, v in delta.items()
           if k.startswith('brewfs_fuse_op_total') and v}
    assert ops['release'] >= files
    seconds = float(fields['seconds'])
    object_gets = sum(v for k, v in delta.items() if k.startswith('brewfs_object_get_total{'))
    object_bytes = sum(v for k, v in delta.items() if k.startswith('brewfs_object_get_bytes_total{'))
    handler_seconds = sum(v for k, v in delta.items() if k.startswith('brewfs_fuse_op_latency_seconds_sum{'))
    row = dict(order, round=int(order['round']), ttl_ms=ttls[arm], files=files,
               seconds=seconds, ms_per_file=seconds * 1000 / files,
               op_counts=ops, ops_per_file={k: v / files for k, v in ops.items()},
               total_requests_per_file=sum(ops.values()) / files,
               handler_ms_per_file=handler_seconds * 1000 / files,
               object_gets=object_gets, object_get_bytes=object_bytes,
               metadata_rpc_count=sum(v for k,v in delta.items() if k.startswith('brewfs_metadata_op_total{')),
               stats_samples=len(list((path / 'stats-timeseries/packed-smallfiles').glob('*.txt'))),
               attr_valid_ns=stats['brewfs_fuse_attr_valid_ns'], entry_valid_ns=stats['brewfs_fuse_entry_valid_ns'])
    rows.append(row)
assert len(rows) == 12 and len(content_hashes) == 1
groups = {}
for arm in 'ABCD':
    selected = sorted((r for r in rows if r['arm'] == arm), key=lambda r: r['round'])
    assert [r['round'] for r in selected] == [1, 2, 3]
    groups[arm] = dict(latency_ms_per_file=summary([r['ms_per_file'] for r in selected]),
                       ops_per_file={k: summary([r['ops_per_file'].get(k, 0) for r in selected])
                                     for k in sorted(set().union(*(r['ops_per_file'] for r in selected)))},
                       handler_ms_per_file=summary([r['handler_ms_per_file'] for r in selected]),
                       total_requests_per_file=summary([r['total_requests_per_file'] for r in selected]))
comparisons = {}
for before_arm, after_arm in [('A', 'B'), ('A', 'C'), ('B', 'C'), ('B', 'D'), ('A', 'D')]:
    before = groups[before_arm]['latency_ms_per_file']['values']
    after = groups[after_arm]['latency_ms_per_file']['values']
    comparisons[before_arm + '-' + after_arm] = dict(
        savings_ms_per_file=summary([a-b for a,b in zip(before, after)]),
        savings_percent=summary([(a-b)/a*100 for a,b in zip(before, after)]))
result = dict(binary_sha256=expected_sha, source_revision=(root/'main-source.txt').read_text().strip(),
              contract=contract, rows=rows, groups=groups, paired_comparisons=comparisons,
              ci_method='Student t CI for run means; n=3 independent mounts, df=2. Median is descriptive, not the center of this CI.',
              content_manifest_sha256=next(iter(content_hashes)),
              native_reference_ms=0.27,
              note='HTTP deltas include original Compose monitoring .stats reads; exact per-file structure is established by separate sampler-free diagnostics.')
(out/'summary.json').write_text(json.dumps(result, indent=2)+'\n')
for arm, data in groups.items():
    print(arm, json.dumps(data['latency_ms_per_file']))
    print('ops/file', {k: v['median'] for k,v in data['ops_per_file'].items()})
for label, data in comparisons.items():
    print(label, json.dumps(data))
```

**collect.py**

```python
#!/usr/bin/env python3
"""Create documentation evidence, without copying executable repo changes."""
import collections
import csv
import decimal
import hashlib
import io
import json
import pathlib
import platform
import re
import shutil

root = pathlib.Path('/mnt/brewfs-data/home/hxy/smallfile-warm')
exp = root / 'ttl-20261104'
repo = pathlib.Path('/home/hxy/brewfs-main')
dest = repo / 'doc/performance/evidence/smallfile-ttl-20261104'
dest.mkdir(parents=True, exist_ok=True)
summary = json.loads((exp/'summary.json').read_text())


def dump(name, value):
    (dest/name).write_text(json.dumps(value, indent=2)+'\n')


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def selected_metrics(path):
    prefixes = ('brewfs_fuse_op_total{', 'brewfs_fuse_op_latency_seconds_sum{',
                'brewfs_object_get_total{', 'brewfs_object_get_bytes_total{',
                'brewfs_object_get_failures_total', 'brewfs_metadata_op_total{')
    return {k: float(v) for line in path.read_text().splitlines()
            if line.startswith(prefixes) for k,v in [line.rsplit(' ',1)]}


inputs = [root/name for name in ['attribute.py', 'compose.sh', 'compose.yml',
          'post-scan.py', 'perf_manifest.py', 'main-source.txt', 'main-binaries.sha256',
          'build-main.sh', 'manifest-key.txt', 'manifest-key.source.json']]
inputs += [repo/'docker/compose-xfstests'/name for name in
           ['run_perf_in_container.sh', 'docker-compose.redis-perf.yml']]
inputs += [p for p in exp.iterdir() if p.suffix in ('.py', '.sh')]
provenance = dict(source_revision=summary['source_revision'],
                  binary_sha256=summary['binary_sha256'],
                  binary_size=(root/'brewfs-main').stat().st_size,
                  uname=list(platform.uname()),
                  inputs={str(p):digest(p) for p in sorted(inputs)},
                  original_artifacts=str(root/'artifacts'),
                  run_order=(exp/'order.tsv').read_text(),
                  artifacts={})
counter_snapshots = {}
kernel_snapshots = {}
columns = ['round','arm','ttl_ms','ms_per_file','seconds','object_gets','object_get_bytes',
           'metadata_rpc_count','stats_samples','lookup','getattr','open','read','release','flush','ioctl',
           'opendir','readdir','readdirplus','releasedir','run_id','start_utc','end_utc']
with (dest/'measurements.tsv').open('w') as stream:
    writer = csv.DictWriter(stream, fieldnames=columns, delimiter='\t', lineterminator='\n')
    writer.writeheader()
    for row in summary['rows']:
        values = {k: row.get(k, row['op_counts'].get(k,0)) for k in columns}
        writer.writerow(values)
        path = root/'artifacts'/row['run_id']
        filenames = ['manifest.json', 'measured-before.metrics', 'measured-after.metrics',
                     'measured-before.kernel.json', 'measured-after.kernel.json',
                     'tools/packed-smallfiles.log','tools/packed-tree.log','sha256.json',
                     'diagnostics/stats-packed-smallfiles-before.txt',
                     'diagnostics/stats-packed-smallfiles-after.txt','perf-summary.tsv','perf.complete']
        filenames += [str(p.relative_to(path)) for p in (path/'diagnostics').glob('mount-*.json')]
        provenance['artifacts'][row['run_id']] = {name:digest(path/name) for name in filenames}
        counter_snapshots[row['run_id']] = {side:selected_metrics(path/f'measured-{side}.metrics')
                                           for side in ['before','after']}
        kernel_snapshots[row['run_id']] = {side:json.loads((path/f'measured-{side}.kernel.json').read_text())
                                          for side in ['before','after']}

diagnostics = {}
for suffix in ['stage-0','stage-1000','stage-10000','syscalls-raw-0']:
    path = root/'artifacts'/('ttl-20261104-'+suffix)
    rows = json.loads((path/'stages.json').read_text())
    trace = [json.loads(line.split(' ',2)[2]) for line in (path/'probe-after.stats').read_text().splitlines()
             if line.startswith('# brewfs_request_trace_v2 ')]
    for row in rows:
        row['getattr_trace'] = [{k: req[k] for k in ['unique','opcode','file_id','received_at_ns','total_ns']}
                                for req in trace if req['opcode']==3 and
                                row['start_ns'] <= req['received_at_ns'] <= row['end_ns']]
        assert len(row['getattr_trace']) == row['op_delta'].get('getattr',0)
    totals = {}
    for mode in ['compose','no-explicit-stat','raw']:
        counts = collections.Counter()
        for row in rows:
            if row['mode'] == mode:
                counts.update(row['op_delta'])
        totals[mode] = dict(counts)
    expected = [14,11,8] if suffix in ('stage-0','syscalls-raw-0') else [7,7,6]
    assert [sum(totals[mode].values()) for mode in ['compose','no-explicit-stat','raw']] == expected
    diagnostics[suffix] = dict(stages=rows, total_per_file=totals)
    provenance['artifacts'][path.name] = {name:digest(path/name) for name in ['stages.json','probe-after.stats']}
    if suffix == 'syscalls-raw-0':
        full_trace = (path/'scanner.strace').read_text().splitlines()
        excerpt = []
        correlations = []
        for row in rows:
            excerpt.append(f"# {row['mode']}: {row['stage']}")
            for line in full_trace:
                match = re.match(r'\s*\d+\s+(\d+\.\d+)\s+(.*) <([0-9.]+)>$',line)
                if not match:
                    continue
                began = int(decimal.Decimal(match[1])*1000000000)
                ended = began + int(decimal.Decimal(match[3])*1000000000)
                if row['start_ns'] <= began <= row['end_ns']:
                    excerpt.append(line)
                    correlations.append(dict(mode=row['mode'], stage=row['stage'], syscall=match[2],
                                             start_ns=began, end_ns=ended,
                                             getattr_nodes=[r['file_id'] for r in row['getattr_trace']
                                                            if began<=r['received_at_ns']<=ended]))
        (dest/'scanner-syscalls.txt').write_text('\n'.join(excerpt)+'\n')
        diagnostics[suffix]['syscall_correlations'] = correlations
        provenance['artifacts'][path.name]['scanner.strace'] = digest(path/'scanner.strace')
        for plain, traced in zip(diagnostics['stage-0']['stages'], rows):
            assert plain['op_delta']==traced['op_delta']

diagnostics['rejected_tracing'] = dict(
    paths=[str(root/'artifacts'/('ttl-20261104-'+suffix)) for suffix in
           ['probe-0','probe-1000','probe-10000','syscalls-0']],
    reason='strace -yy/-P, and then decoded ioctl, caused extra pathname resolution/getattr. '
           'Do not use these counter deltas or timings. Plain probes and strace with raw=ioctl agree exactly.')
dump('diagnostics.json',diagnostics)
dump('counter-snapshots.json',counter_snapshots)
dump('kernel-snapshots.json',dict(
    note='Global dentry/inode occupancy, not hit/miss counts or per-mount hit rates. FUSE waiting is queue state.',
    runs=kernel_snapshots))
dump('provenance.json',provenance)
shutil.copyfile(exp/'summary.json',dest/'summary.json')

scripts = ['prepare-raw.py','raw-scanner.py','capture.py','run-one.sh','run-matrix.sh',
           'stage-probe.py','run-probe.sh','run-probes.sh','summarize.py','collect.py']
text = ['# Measurement script archive\n',
        'Executable files used only outside the repository, under '
        '`/mnt/brewfs-data/home/hxy/smallfile-warm/ttl-20261104/`. '
        'They are recorded as documentation here; no product, test, or runner source is changed.\n',
        'Restore each fenced block using its filename into that directory on a fresh '
        'reproduction host with the preserved parent harness and main executable. '
        'Original artifacts are never overwritten: both drivers refuse existing run directories. '
        'Use unique run IDs when repeating on the evidence host.\n',
        'Run `prepare-raw.py`, then `run-matrix.sh`, `summarize.py`, `run-probes.sh`, '
        'and `collect.py` in that order. Start Redis/RustFS with the original compose.sh first; '
        'finish with `compose.sh down -v --remove-orphans`. '
        'No compilation, drop_caches, payload cache, or keep-cache changes are involved.\n']
for name in scripts:
    language = 'bash' if name.endswith('.sh') else 'python'
    text.extend([f'\n**{name}**\n\n',f'```{language}\n',(exp/name).read_text(),'```\n'])
(dest/'reproduce.md').write_text(''.join(text))
print('validated and collected',dest)
```
