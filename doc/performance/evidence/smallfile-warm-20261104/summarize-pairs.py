#!/usr/bin/env python3
"""Paired three-round summaries of the unchanged Compose workload."""
import csv
import hashlib
import json
import math
import pathlib
import re
import statistics

root = pathlib.Path('/mnt/brewfs-data/home/hxy/smallfile-warm')
t_critical = 4.3026527299
source_revision = (root / 'main-source.txt').read_text().strip()
candidate_patch_hash = hashlib.sha256((root / 'candidate-source.patch').read_bytes()).hexdigest()
binary_hashes = {}
for arm in ('main', 'candidate'):
    for line in (root / f'{arm}-binaries.sha256').read_text().splitlines():
        digest, name = line.split(maxsplit=1)
        if pathlib.Path(name).name == f'brewfs-{arm}':
            binary_hashes[arm] = digest
assert len(set(binary_hashes.values())) == 2, 'main and candidate must be distinct preserved binaries'


def metrics(path):
    result = {}
    for line in path.read_text().splitlines():
        if line.startswith('brewfs_'):
            key, value = line.rsplit(' ', 1)
            result[key] = float(value)
    return result


def ci(values):
    mean = statistics.mean(values)
    error = statistics.stdev(values) / math.sqrt(len(values))
    return [mean - t_critical * error, mean + t_critical * error]


def compare(before, after):
    delta = [b - a for a, b in zip(before, after)]
    percent = [(b / a - 1) * 100 for a, b in zip(before, after)]
    mean = statistics.mean(delta)
    error = statistics.stdev(delta) / math.sqrt(3)
    t = mean / error if error else None
    p = 1 - abs(t) / math.sqrt(t * t + 2) if t is not None else (0 if mean else 1)
    return dict(main=before, candidate=after, differences=delta,
                mean_difference=mean, difference_ci95=ci(delta),
                candidate_mean=statistics.mean(after), candidate_mean_ci95=ci(after),
                paired_percent=percent, mean_paired_percent=statistics.mean(percent),
                percent_ci95=ci(percent), t=t, df=2, two_sided_p=p)


rows = []
hashes = set()
contracts = {}
expected_environment = dict(
    BREWFS_VOLUME_FORMAT='packed-metadata-v3',
    BREWFS_PACKED_MANIFEST_KEY=(root / 'manifest-key.txt').read_text().strip(),
    BREWFS_PACKED_METADATA_CACHE_BYTES='268435456',
    BREWFS_READ_MEMORY_BYTES='0', BREWFS_READ_SSD_BYTES='0',
    BREWFS_PREFETCH_ENABLED='false', BREWFS_RANGE_BACKGROUND_PREFETCH='false',
    BREWFS_FUSE_READ_DIRECT_IO='1', BREWFS_FUSE_KEEP_CACHE='0',
    BREWFS_CACHE_TTL_MS='0', BREWFS_COMPRESSION='none',
    BREWFS_REQUEST_TRACE='true', BREWFS_REQUEST_TRACE_SAMPLE_RATE='307')
for phase in ('warm', 'cold'):
    for round_number in range(1, 4):
        for arm in ('main', 'candidate'):
            run_id = f'{phase}-{arm}-r{round_number}'
            path = root / 'artifacts' / run_id
            assert (path / 'perf.complete').exists(), run_id
            assert not list(path.glob('*.invalid')), run_id
            with (path / 'perf-summary.tsv').open() as stream:
                tool_statuses = list(csv.DictReader(stream, delimiter='\t'))
            expected_tools = {'packed-tree', 'packed-smallfiles'} if phase == 'warm' else {'packed-smallfiles'}
            assert {row['tool'] for row in tool_statuses} == expected_tools, run_id
            assert all(row['status'] == 'pass' for row in tool_statuses), run_id
            manifest = json.loads((path / 'manifest.json').read_text())
            assert manifest['source']['binary_sha256'] == binary_hashes[arm], run_id
            assert manifest['source']['revision'] == source_revision, run_id
            assert manifest['source']['dirty'] == (arm == 'candidate'), run_id
            assert manifest['source']['dirty_diff_sha256'] == (candidate_patch_hash if arm == 'candidate' else None), run_id
            if phase in contracts:
                assert manifest['comparability'] == contracts[phase], run_id
            else:
                contracts[phase] = manifest['comparability']
            mounts = list((path / 'diagnostics').glob('mount-*.json'))
            assert len(mounts) == 1, run_id
            mount = json.loads(mounts[0].read_text())
            assert mount['binary_sha256'] == binary_hashes[arm], run_id
            assert {key: mount['environment'].get(key) for key in expected_environment} == expected_environment, run_id
            text = (path / 'tools/packed-smallfiles.log').read_text()
            line = next(line for line in text.splitlines() if line.startswith('packed_smallfiles_summary '))
            fields = dict(re.findall(r'(\w+)=([^ ]+)', line))
            assert int(fields['files']) == 8192 and int(fields['errors']) == 0
            assert int(fields['payload_bytes']) == 33554432
            assert fields['read_mode'] == 'full'
            before = metrics(path / 'diagnostics/stats-packed-smallfiles-before.txt')
            after = metrics(path / 'diagnostics/stats-packed-smallfiles-after.txt')
            get_key = 'brewfs_packed_v3_runtime_backend_range_gets_total'
            gets = after[get_key] - before[get_key]
            get_bytes_key = 'brewfs_packed_v3_runtime_backend_received_bytes_total'
            received_bytes = after[get_bytes_key] - before[get_bytes_key]
            readdir = json.loads((path / 'readdir.json').read_text())['milliseconds']
            mount_ms = json.loads((path / 'mount-time.json').read_text())['milliseconds']
            seconds = float(fields['seconds'])
            content = json.loads((path / 'sha256.json').read_text())
            assert len(content) == 8192
            hashes.add(tuple(sorted(content.items())))
            rows.append(dict(phase=phase, round=round_number, arm=arm, run_id=run_id,
                             ms_per_file=seconds * 1000 / 8192,
                             scan_seconds=seconds, gets=gets, gets_per_file=gets / 8192,
                             received_bytes=received_bytes, readdir_ms=readdir,
                             readdir_median_ms=statistics.median(readdir),
                             mount_ms=mount_ms, mount_plus_scan_ms_per_file=(mount_ms + seconds * 1000) / 8192))
assert len(hashes) == 1, 'content differs between arms or rounds'

comparisons = {}
for phase in ('warm', 'cold'):
    comparisons[phase] = {}
    for metric in ('ms_per_file', 'gets_per_file', 'readdir_median_ms', 'mount_plus_scan_ms_per_file'):
        values = {arm: [r[metric] for r in rows if r['phase'] == phase and r['arm'] == arm]
                  for arm in ('main', 'candidate')}
        if all(values['main']):
            comparisons[phase][metric] = compare(values['main'], values['candidate'])
        else:
            comparisons[phase][metric] = values

result = dict(status='rejected: every candidate round exceeds 0.40 ms/file; validation is reported separately',
              source_main=(root / 'main-source.txt').read_text().strip(),
              binary_sha256=binary_hashes,
              candidate_patch_sha256=candidate_patch_hash,
              runtime_contract=expected_environment,
              t_critical_df2=t_critical, rows=rows, paired=comparisons,
              cold_definition='Fresh client/mount, no packed-tree prewarm; server caches are not flushed.',
              content_validation='All 12 arms have identical SHA-256 for all 8192 deterministic files.')
(root / 'paired-summary.json').write_text(json.dumps(result, indent=2) + '\n')
for phase, comparisons_by_metric in comparisons.items():
    print(phase)
    for metric, comparison in comparisons_by_metric.items():
        print(metric, json.dumps(comparison))
