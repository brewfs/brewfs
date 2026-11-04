#!/usr/bin/env python3
import collections
import json
import pathlib
import statistics
import sys

root = pathlib.Path('/mnt/brewfs-data/home/hxy/smallfile-warm')
arm = sys.argv[1] if len(sys.argv) > 1 else 'main'
direct_modes = tuple(map(int, sys.argv[2:])) if len(sys.argv) > 2 else (1, 0)
result = {}
for direct in direct_modes:
    path = root / 'artifacts' / f'attribution-{arm}-d{direct}'
    rows = [json.loads((path / f'compose-r{i}.json').read_text()) for i in range(1, 4)]
    stages = {stage: statistics.mean(r['stages_ns'][stage] / 8192 / 1e6 for r in rows)
              for stage in rows[0]['stages_ns']}
    modes = {mode: [json.loads((path / f'{mode}-r{i}.json').read_text())['ms_per_file']
                    for i in range(1, 4)]
             for mode in ('compose', 'tree', 'openclose', 'openread')}

    def metrics(file):
        output = {}
        for line in file.read_text().splitlines():
            if line.startswith('brewfs_'):
                key, value = line.rsplit(' ', 1)
                output[key] = float(value)
        return output

    b = metrics(path / 'compose-r1-before.metrics')
    a = metrics(path / 'compose-r1-after.metrics')
    prefixes = ('brewfs_fuse_op_total', 'brewfs_fuse_op_latency_seconds_sum',
                'brewfs_metadata_op_total', 'brewfs_cache_hits_total',
                'brewfs_cache_misses_total', 'brewfs_object_get_total',
                'brewfs_object_get_bytes_total')
    delta = {k: a[k] - b.get(k, 0) for k in a if k.startswith(prefixes)}

    def traces(file):
        return [json.loads(line.split(' ', 2)[2]) for line in file.read_text().splitlines()
                if line.startswith('# brewfs_request_trace_v2 ')]

    trace_by_op = collections.defaultdict(list)
    for i in range(1, 4):
        before = traces(path / f'compose-r{i}-before.stats')
        lower = max((row['unique'] for row in before), default=0)
        for row in traces(path / f'compose-r{i}-after.stats'):
            if row['unique'] > lower:
                trace_by_op[row['opcode']].append(row)
    trace_summary = {op: dict(samples=len(rs), mean_us=statistics.mean(r['total_ns'] / 1000 for r in rs),
                             reply_us=statistics.mean(r['reply_ns'] / 1000 for r in rs),
                             queue_us=statistics.mean(r['queue_wait_ns'] / 1000 for r in rs),
                             exclusive_ns=[statistics.mean(r['exclusive_ns'][j] for r in rs)
                                           for j in range(12)])
                     for op, rs in trace_by_op.items()}
    stat_before = metrics(path / 'compose-r1-before.stats')
    stat_after = metrics(path / 'compose-r1-after.stats')
    packed_deltas = {k: stat_after[k] - stat_before.get(k, 0) for k in stat_after
                     if k.startswith(('brewfs_packed_v3_runtime_backend_', 'brewfs_fuse_'))}
    result[str(direct)] = dict(stages_ms_per_file=stages, modes_ms_per_file=modes,
                              first_scan_metric_deltas=delta, packed_deltas=packed_deltas,
                              trace=trace_summary)

output_name = 'attribution.json' if arm == 'main' else f'attribution-{arm}.json'
(root / output_name).write_text(json.dumps(result, indent=2) + '\n')
for direct, row in result.items():
    print('DIRECT', direct)
    print('stages ms/file:', row['stages_ms_per_file'])
    print('modes ms/file:', row['modes_ms_per_file'])
    print('nonzero counters:', {k: v for k, v in row['first_scan_metric_deltas'].items() if v})
    print('trace:', row['trace'])
