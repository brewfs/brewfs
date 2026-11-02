#!/usr/bin/env python3
"""Generate an isolated binding for the repository's complete perf runner."""
import pathlib
import hashlib
import json
import subprocess
import shutil
import yaml

root = pathlib.Path('/home/hxy/perf-seqwrite-20261102')
repo = pathlib.Path('/home/hxy/brewfs-main')
previous = root / 'harness/guard-compose-template.yml'
config = yaml.safe_load(previous.read_text())
config['name'] = 'brewfs-seqwrite-acceptance'
service = config['services']['perf']
service['entrypoint'] = ['/usr/bin/tini', '--', 'bash', '/usr/local/bin/seqwrite-entrypoint.sh']
env = service['environment']
env.update({
    'BREWFS_META_URL': 'redis://127.0.0.1:26381/0',
    'BREWFS_S3_ENDPOINT': 'http://127.0.0.1:29020',
    'BREWFS_S3_BUCKET': 'brewfs-seqwrite-20261102',
    'BREWFS_WRITE_COALESCE_BYTES': '65536',
    'BREWFS_ARTIFACT_DIR': '/artifacts/${SEQWRITE_ARM:?required}',
    'BREWFS_LOG_FILE': '/artifacts/${SEQWRITE_ARM}/brewfs.log',
    'BREWFS_SOURCE_REVISION': subprocess.check_output(
        ['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip(),
    'BREWFS_SOURCE_DIRTY': '${SEQWRITE_SOURCE_DIRTY:-true}',
    'PERF_TOOLS': '${SEQWRITE_TOOLS:-fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw fio-bigread fio-bigwrite metaperf dirstress dirperf}',
    'PERF_FIO_DIRECT': '${SEQWRITE_DIRECT:-0}',
    'PERF_FIO_RANDREAD_DIRECT': '1',
    # Same random buffer generation as the historical instrumented protocol.
    'PERF_FIO_SEQWRITE_ARGS': '--refill_buffers=1 --scramble_buffers=1',
    'PERF_FIO_RANDWRITE_ARGS': '--refill_buffers=1 --scramble_buffers=1',
    'PERF_FIO_RANDRW_ARGS': '--refill_buffers=1 --scramble_buffers=1',
    'PERF_FIO_PREP_BS': '1m',
    'PERF_FIO_PREP_IODEPTH': '8',
    # Full per-block SHA-256 and cold-restart verification are separate gates.
    # sha256sum's small reads repeatedly decode full compressed blocks when
    # the read cache is disabled, making this optional preparation very costly.
    'PERF_FIO_PREP_VERIFY_SHA256': 'false',
})
# The stock runner exposes compression but not its level. Keep its workload,
# drain, error and teardown code intact; inject only the fixed baseline level.
runner_path = repo / 'docker/compose-xfstests/run_perf_in_container.sh'
runner = runner_path.read_text()
needle = '            echo "  compression: ${comp}"\n'
assert runner.count(needle) == 1
configured_runner = runner.replace(needle, needle + '            echo "  zstd_level: 1"\n')
duplicate_job = 'args+=("--name=$name" "--directory=$work_dir" "--rw=$rw" "--size=$size"'
assert configured_runner.count(duplicate_job) == 1
# A second --name starts another fio job; the controlled extra args never
# override the name, so keep exactly the one original job declaration.
configured_runner = configured_runner.replace(
    duplicate_job, 'args+=("--directory=$work_dir" "--rw=$rw" "--size=$size"')
(root / 'harness/runner-zstd1.sh').write_text(configured_runner)
(root / 'harness/runner-provenance.json').write_text(json.dumps({
    'upstream': str(runner_path), 'upstream_sha256': hashlib.sha256(runner.encode()).hexdigest(),
    'configured_sha256': hashlib.sha256(configured_runner.encode()).hexdigest(),
    'changes': ['echo cache.zstd_level: 1 after compression in generated mount YAML',
                'omit duplicate --name when restoring controlled extra fio arguments; retain one intended job'],
}, indent=2) + '\n')
helper_provenance = []
for relative in ('tools/perf/perf_lines_observe.py', 'tools/perf/perf_manifest.py',
                 'docker/compose-xfstests/perf_metadata_fallback.py'):
    source = repo / relative
    target = root / 'harness' / source.name
    shutil.copyfile(source, target)
    target.chmod(0o755)
    helper_provenance.append({'source': str(source), 'copy': str(target),
                              'sha256': hashlib.sha256(target.read_bytes()).hexdigest(),
                              'only_change': 'executable mode on external harness copy'})
(root / 'harness/helper-provenance.json').write_text(
    json.dumps(helper_provenance, indent=2) + '\n')
service['volumes'] = [
    f'{root}/harness/entrypoint.sh:/usr/local/bin/seqwrite-entrypoint.sh:ro',
    '${SEQWRITE_BINARY:?required}:/usr/local/bin/brewfs:ro',
    f'{root}/harness/runner-zstd1.sh:/usr/local/bin/run_perf_in_container.sh:ro',
    f'{root}/harness/perf_lines_observe.py:/usr/local/bin/perf_lines_observe.py:ro',
    f'{root}/harness/perf_manifest.py:/usr/local/bin/perf_manifest.py:ro',
    f'{root}/harness/perf_metadata_fallback.py:/usr/local/bin/perf_metadata_fallback.py:ro',
    f'{root}/compose:/artifacts',
]
(root / 'harness/guard-compose.yml').write_text(yaml.safe_dump(config, sort_keys=False))
