#!/usr/bin/env python3
import collections
import json
import pathlib
import statistics

root = pathlib.Path('/mnt/brewfs-data/home/hxy/smallfile-warm')
path = root / 'historical-candidate-r3'


def traces(file):
    return [json.loads(line.split(' ', 2)[2]) for line in file.read_text().splitlines()
            if line.startswith('# brewfs_request_trace_v2 ')]


lower = max((r['unique'] for r in traces(path / 'diagnostics/stats-packed-smallfiles-before.txt')), default=0)
records = {}
for file in sorted((path / 'stats-timeseries/packed-smallfiles').glob('*.txt')) + [path / 'diagnostics/stats-packed-smallfiles-after.txt']:
    records.update((r['unique'], r) for r in traces(file) if r['unique'] > lower)
grouped = collections.defaultdict(list)
for row in records.values():
    grouped[row['opcode']].append(row)
count_per_file = {1: 2, 3: 6, 14: 1, 15: 2, 18: 1, 25: 1, 39: 1}
names = ['queue_wait', 'file_lock', 'chunk_lock', 'dirty_merge', 'flush',
         'metadata_rpc', 'metadata_commit', 'object_put_queue', 'object_put',
         'reply', 'handler_residual', 'handler_poll']
result = dict(samples=sum(len(grouped[op]) for op in count_per_file),
              exclusive_us_per_file={name: sum(count * statistics.mean(r['exclusive_ns'][j] / 1000 for r in grouped[op])
                                               for op, count in count_per_file.items())
                                     for j, name in enumerate(names)},
              op_samples={op: len(grouped[op]) for op in count_per_file},
              note='Exclusive within each request; requests can overlap. Not an exclusive wall-time partition.')
(root / 'historical-exclusive.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))
