#!/usr/bin/env python3
"""Recover complete sampled traces from each original warm acceptance scan."""
import collections
import json
import pathlib
import statistics

root = pathlib.Path('/mnt/brewfs-data/home/hxy/smallfile-warm')
weights = {1: 2, 3: 6, 14: 1, 15: 2, 18: 1, 25: 1, 39: 1}
segments = ['queue_wait', 'file_lock', 'chunk_lock', 'dirty_merge', 'flush',
            'metadata_rpc', 'metadata_commit', 'object_put_queue', 'object_put',
            'reply', 'handler_residual', 'handler_poll']


def traces(path):
    return [json.loads(line.split(' ', 2)[2]) for line in path.read_text().splitlines()
            if line.startswith('# brewfs_request_trace_v2 ')]


result = {}
for arm in ('main', 'candidate'):
    rows = []
    for number in (1, 2, 3):
        path = root / 'artifacts' / f'warm-{arm}-r{number}'
        lower = max((r['unique'] for r in traces(path / 'diagnostics/stats-packed-smallfiles-before.txt')), default=0)
        after = path / 'diagnostics/stats-packed-smallfiles-after.txt'
        upper = max(r['unique'] for r in traces(after))
        records = {}
        for snapshot in sorted((path / 'stats-timeseries/packed-smallfiles').glob('*.txt')) + [after]:
            records.update((r['unique'], r) for r in traces(snapshot) if lower < r['unique'] <= upper)
        grouped = collections.defaultdict(list)
        for record in records.values():
            grouped[record['opcode']].append(record)
        rows.append(dict(round=number, samples=len(records),
                         samples_by_opcode={op: len(grouped[op]) for op in weights},
                         lifecycle_us_per_file=sum(count * statistics.mean(r['total_ns'] / 1000 for r in grouped[op]) for op, count in weights.items()),
                         exclusive_us_per_file={name: sum(count * statistics.mean(r['exclusive_ns'][j] / 1000 for r in grouped[op])
                                                          for op, count in weights.items())
                                                for j, name in enumerate(segments)}))
    result[arm] = dict(rounds=rows,
                       mean_exclusive_us_per_file={name: statistics.mean(r['exclusive_us_per_file'][name] for r in rows) for name in segments},
                       mean_lifecycle_us_per_file=statistics.mean(r['lifecycle_us_per_file'] for r in rows))
result['note'] = 'Request lifecycle estimates and within-request exclusive segments can overlap across requests; they are not a wall-clock partition.'
(root / 'paired-traces.json').write_text(json.dumps(result, indent=2) + '\n')
for arm in ('main', 'candidate'):
    print(arm, 'samples', [r['samples'] for r in result[arm]['rounds']],
          'lifecycle us/file', result[arm]['mean_lifecycle_us_per_file'])
    print(result[arm]['mean_exclusive_us_per_file'])
