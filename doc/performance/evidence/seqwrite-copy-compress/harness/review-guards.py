#!/usr/bin/env python3
"""Review every completed compose arm, strict debt, amplification and teardown."""
import csv
import hashlib
import json
import os
import pathlib
import re

import summarize as s

root = s.ROOT
debt = ['brewfs_writeback_dirty_bytes', 'brewfs_buffer_dirty_bytes',
        'brewfs_writeback_live_dirty_bytes', 'brewfs_writeback_recent_pending_upload_bytes',
        'brewfs_writeback_remote_upload_inflight_bytes', 'brewfs_writeback_stage_inflight_bytes',
        'brewfs_writeback_metadata_inflight']
output = {'compose': {}, 'direct': {}, 'correctness_files': {}}


def system_window(directory, tool):
    samples = [json.loads(path.read_text()) for path in
               sorted((directory / 'system-timeseries' / tool).glob('*.json'))]
    if len(samples) < 2:
        return {'samples': len(samples)}
    elapsed = (samples[-1]['monotonic_ns'] - samples[0]['monotonic_ns']) / 1e9
    def ticks(sample):
        data = sample['raw'][f"/proc/{sample['pid']}/stat"].rsplit(')', 1)[1].split()
        return int(data[11]) + int(data[12])
    row = {'samples': len(samples), 'sample_window_seconds': elapsed,
           'mount_cpu_seconds': (ticks(samples[-1]) - ticks(samples[0])) / os.sysconf('SC_CLK_TCK')}
    row['mount_mean_cpu_cores'] = row['mount_cpu_seconds'] / elapsed
    row['mount_peak_rss_mib'] = max(int(re.search(
        r'VmRSS:\s+(\d+)', sample['raw'][f"/proc/{sample['pid']}/status"]).group(1))
        for sample in samples) / 1024
    for kind in ['cpu', 'io', 'memory']:
        for scope in ['some', 'full']:
            totals = []
            for sample in [samples[0], samples[-1]]:
                raw = sample['raw'].get('/proc/pressure/' + kind, '')
                match = re.search(r'^' + scope + r' .*total=(\d+)', raw, re.M)
                totals.append(int(match.group(1)) if match else None)
            if all(value is not None for value in totals):
                row[f'host_{kind}_pressure_{scope}_pct'] = (totals[1] - totals[0]) / (elapsed * 1e4)
    return row

for directory in sorted((root / 'compose').glob('*')):
    if not directory.is_dir() or not (directory / 'status.json').exists():
        continue
    status = json.loads((directory / 'status.json').read_text())
    assert status['status'] == 0, directory
    assert not list(directory.glob('*.invalid')), directory
    with (directory / 'perf-summary.tsv').open() as stream:
        tools = list(csv.DictReader(stream, delimiter='\t'))
    assert all(tool['status'] == 'pass' for tool in tools), directory
    unmounts = (directory / 'unmount-commands.log').read_text().splitlines()
    assert unmounts and all('status=0 ' in line for line in unmounts), directory
    processes = (directory / 'processes-after-runner.txt').read_text()
    assert not any('brewfs mount ' in line for line in processes.splitlines()), directory
    rows = {}
    for tool in tools:
        name = tool['tool']
        before_path = directory / 'diagnostics' / f'stats-{name}-before.txt'
        after_path = directory / 'diagnostics' / f'stats-{name}-post-write-drained-after.txt'
        if not after_path.exists():
            after_path = directory / 'diagnostics' / f'stats-{name}-after.txt'
        before = s.reader.parse_stats_file(before_path)
        after = s.reader.parse_stats_file(after_path)
        delta = {key: value - before.get(key, 0) for key, value in after.items()}
        final_debt = {key: after[key] for key in debt}
        assert all(value == 0 for value in final_debt.values()), (directory, name, final_debt)
        put = delta.get('brewfs_s3_put_ops_total', 0)
        put_bytes = delta.get('brewfs_s3_put_bytes_total', 0)
        written = delta.get('brewfs_fuse_write_bytes_total', 0)
        row = {'final_debt': final_debt, 'put_ops': put, 'put_bytes': put_bytes,
               'logical_fuse_write_bytes': written,
               'get_ops': delta.get('brewfs_s3_get_ops_total', 0),
               'get_bytes': delta.get('brewfs_s3_get_bytes_total', 0),
               'average_put_bytes': put_bytes / put if put else None,
               'put_per_gib_written': put / (written / (1 << 30)) if written else None,
               'upload_bytes_per_written_byte': put_bytes / written if written else None}
        row['system_window'] = system_window(directory, name)
        row['metadata_cache_delta'] = {key: value for key, value in delta.items()
                                       if key.startswith('brewfs_meta_') and '_cache_' in key}
        redis_path = directory / 'diagnostics' / f'redis-{name}-after.txt'
        if redis_path.exists():
            row['redis_commandstats'] = {}
            for line in redis_path.read_text().splitlines():
                if line.startswith('cmdstat_'):
                    command, values = line.split(':', 1)
                    row['redis_commandstats'][command.removeprefix('cmdstat_')] = {
                        key: float(value) for key, value in
                        (item.split('=', 1) for item in values.split(','))}
        for key in ['brewfs_writeback_upload_batch_ops_total',
                    'brewfs_writeback_upload_batch_bytes_total',
                    'brewfs_writeback_upload_batch_blocks_total',
                    'brewfs_writeback_upload_partial_tail_ops_total',
                    'brewfs_writeback_slice_create_ops_total']:
            row[key.removeprefix('brewfs_')] = delta.get(key)
        rows[name] = row
    output['compose'][directory.name] = {'tools': tools, 'unmounts': unmounts,
                                         'workloads': rows}
for path in sorted((root / 'rounds').glob('*-direct/guard-*/complete.json')):
    complete = json.loads(path.read_text())
    summary = complete['summary']
    assert summary['status'] == 0 and summary['drain']['clear'], path
    assert all(all(value == 0 for value in item['debt'].values())
               for item in summary['drain']['samples'][-3:]), path
    fio = summary['fio']
    output['direct'][complete['workload'] + '-' + complete['variant']] = {
        'active_mib_s': fio['bw_mib_s'],
        'wall_plus_drain_mib_s': fio['wall_plus_drain_bw_mib_s'],
        'active_plus_drain_mib_s': fio['active_plus_drain_bw_mib_s'],
        'objects': summary['objects_including_drain'],
        'drain_seconds': summary['drain']['seconds'],
        'p99_ms': {op: fio[op + '_latency'].get('percentile', {}).get('99.000000', 0) / 1e6
                   for op in ['read', 'write']},
    }
for path in sorted((root / 'correctness').glob('*/*blocks-sha256.json')):
    values = json.loads(path.read_text())
    output['correctness_files'][str(path.relative_to(root))] = {
        'sha256': hashlib.file_digest(path.open('rb'), 'sha256').hexdigest(),
        'blocks': len(values), 'bytes': path.stat().st_size,
    }
(root / 'guard-review.json').write_text(json.dumps(output, indent=2) + '\n')
print(json.dumps({'compose_arms': len(output['compose']), 'direct_arms': len(output['direct']),
                  'correctness_hash_lists': len(output['correctness_files'])}, indent=2))
