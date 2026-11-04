#!/usr/bin/env python3
"""Summarize diagnostic mixed pairs without accepting the failed original guard."""
import csv
import hashlib
import json
import math
import pathlib
import statistics

root = pathlib.Path('/mnt/brewfs-data/home/hxy/smallfile-warm')
assert (root / 'diagnostic-mixed.done').exists()
comparisons = json.loads((root / 'guard-summary.json').read_text())
assert next(row for row in comparisons if row['label'] == 'mixed-r1')['status'] == 'incomplete_or_failed'


def metrics(path):
    result = {}
    for line in path.read_text().splitlines():
        if line.startswith('brewfs_'):
            key, value = line.rsplit(' ', 1)
            result[key] = float(value)
    return result


def compare(before, after, with_percent=True):
    delta = [b-a for a, b in zip(before, after)]
    percent = [(b/a-1)*100 for a, b in zip(before, after)]
    def ci(values):
        mean = statistics.mean(values)
        half = 4.3026527299 * statistics.stdev(values) / math.sqrt(3)
        return [mean-half, mean+half]
    error = statistics.stdev(delta)/math.sqrt(3)
    t = statistics.mean(delta)/error if error else None
    result = dict(main=before, candidate=after,
                main_mean=statistics.mean(before), candidate_mean=statistics.mean(after),
                difference_mean=statistics.mean(delta), difference_ci95=ci(delta),
                t=t, df=2, two_sided_p=(1-abs(t)/math.sqrt(t*t+2)) if t is not None else None)
    if with_percent:
        result.update(paired_percent_mean=statistics.mean(percent), paired_percent_ci95=ci(percent))
    return result


rows = []
for round_number in (1, 2, 3):
    label = f'diagnostic-mixed-r{round_number}'
    comparison = next(row for row in comparisons if row['label'] == label)
    assert 'status' not in comparison, label
    with (root / 'guard-comparisons' / f'{label}-d0.tsv').open() as stream:
        table = {(row['kind'], row['metric']): row for row in csv.DictReader(stream, delimiter='\t')
                 if row['item'] == 'fio-randrw'}
    for arm, column in [('main', 'baseline'), ('candidate', 'candidate')]:
        path = root / 'artifacts' / f'guard-{label}-{arm}-d0'
        assert json.loads((path / 'diagnostic-contract.json').read_text())['diagnostic_only']
        values = dict(
            active_total_bw_mib_s=sum(float(table['fio', metric][column])
                                      for metric in ('read_bw_mib_s', 'write_bw_mib_s')),
            effective_wall_bw_mib_s=float(table['runtime', 'mixed_effective_wall_bw_mib_s'][column]),
            effective_active_plus_drain_bw_mib_s=float(table['runtime', 'mixed_effective_active_plus_drain_bw_mib_s'][column]),
            read_p99_ms=float(table['fio', 'read_total_p99_ms'][column]),
            write_p99_ms=float(table['fio', 'write_total_p99_ms'][column]))
        before = metrics(path / 'diagnostics/stats-fio-randrw-before.txt')
        drained = metrics(path / 'diagnostics/stats-fio-randrw-post-write-drained-after.txt')
        def delta(name):
            result = drained[name] - before[name]
            assert result >= 0, (arm, round_number, name)
            return result
        write_bytes = delta('brewfs_fuse_write_bytes_total')
        put_ops = delta('brewfs_s3_put_ops_total')
        put_bytes = delta('brewfs_s3_put_bytes_total')
        batch_ops = delta('brewfs_writeback_upload_batch_ops_total')
        final_debt = {name: drained[name] for name in (
            'brewfs_writeback_dirty_bytes', 'brewfs_writeback_live_dirty_bytes',
            'brewfs_writeback_recent_pending_upload_bytes', 'brewfs_writeback_remote_upload_inflight_bytes')}
        assert not any(final_debt.values()), (arm, round_number, final_debt)
        fio = json.loads((path / 'results/fio-randrw.json').read_text())
        assert write_bytes == sum(job['write']['io_bytes'] for job in fio['jobs'])
        with (path / 'post-write-drain.tsv').open() as stream:
            drain = next(csv.DictReader(stream, delimiter='\t'))
        values['drain_seconds'] = float(drain['extended_strict_drain_s'])
        waits = [json.loads(line)['exit_wait_ms'] for line in (path / 'exit-waits.jsonl').read_text().splitlines()]
        assert len(waits) == 2, path
        values['prefill_exit_wait_ms'], values['final_exit_wait_ms'] = waits
        rows.append(dict(round=round_number, arm=arm, metrics=values, final_debt=final_debt,
                         drained_amplification=dict(
                             put_ops_per_gib_written=put_ops/(write_bytes/2**30),
                             average_put_mib=put_bytes/put_ops/2**20,
                             upload_byte_amplification=put_bytes/write_bytes,
                             average_batch_mib=delta('brewfs_writeback_upload_batch_bytes_total')/batch_ops/2**20,
                             partial_tail_ops=delta('brewfs_writeback_upload_partial_tail_ops_total'))))
paired = {key: compare(
    [row['metrics'][key] for row in rows if row['arm']=='main'],
    [row['metrics'][key] for row in rows if row['arm']=='candidate'],
    with_percent=not key.endswith('exit_wait_ms'))
    for key in rows[0]['metrics']}
failure = root / 'artifacts/guard-mixed-r1-candidate-d0/brewfs.log'
result = dict(status='diagnostic only; original guard failed and absolute small-file target was missed',
              original_guard_failure=dict(run_id='guard-mixed-r1-candidate-d0',
                  stage='Remount after zero-debt prefill, before timed fio',
                  error='Address already in use (os error 98)',
                  log_sha256=hashlib.sha256(failure.read_bytes()).hexdigest(),
                  cause_status='Runner does not await process exit. Idle probes did not reproduce the failure; candidate teardown timing has not been exonerated.'),
              changed_protocol='Wait for the previous mount process to exit after original cleanup, before remount.',
              notes=['Wall bandwidth uses the canonical runner wall-time accounting.',
                     'Exit waits begin after cleanup and observer startup; they are additional waits, not full teardown duration.',
                     'Amplification uses the final strict-drain snapshot, not the still-uploading end-of-fio snapshot.',
                     'Remaining core fio, metadata stress and direct-write guards were not completed.'],
              rows=rows, paired=paired)
(root / 'diagnostic-mixed-summary.json').write_text(json.dumps(result, indent=2)+'\n')
for key, value in paired.items():
    print(key, json.dumps(value))
