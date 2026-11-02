#!/usr/bin/env python3
"""Paired estimates from the repository compose artifact reader, without filtering rounds."""
import argparse
import csv
import importlib.util
import json
import math
import pathlib
import statistics
import sys

ROOT = pathlib.Path('/home/hxy/perf-seqwrite-20261102')
spec = importlib.util.spec_from_file_location(
    'artifact_compare', '/home/hxy/brewfs-main/tools/perf/compare_artifacts.py')
reader = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = reader
spec.loader.exec_module(reader)


def paired(before, after, higher_is_better=True):
    assert len(before) == len(after) == 3
    ratios = [math.log(b / a) for a, b in zip(before, after)]
    center = statistics.mean(ratios)
    radius = 4.302652729911275 * statistics.stdev(ratios) / math.sqrt(3)
    return {
        'baseline': before, 'candidate': after,
        'baseline_median': statistics.median(before),
        'candidate_median': statistics.median(after),
        'paired_geomean_change_pct': 100 * math.expm1(center),
        'paired_95_ci_pct': [100 * math.expm1(center - radius),
                             100 * math.expm1(center + radius)],
        'higher_is_better': higher_is_better,
        'non_regression_3pct_established': (
            math.expm1(center - radius) >= -0.03 if higher_is_better
            else math.expm1(center + radius) <= 0.03),
    }


def read_arm(directory, scenes=None):
    status = json.loads((directory / 'status.json').read_text())
    assert status['status'] == 0, directory
    metrics = reader.load_artifact(directory)
    boundaries = {}
    with (directory / 'phase-boundaries.tsv').open() as stream:
        for item, event, timestamp in csv.reader(stream, delimiter='\t'):
            assert (item, event) not in boundaries, (directory, item, event)
            boundaries[item, event] = int(timestamp)

    def get(kind, item, name, default=None):
        value = metrics.get((kind, item, name))
        return default if value is None else value.value

    result = {}
    for scene in scenes or ['seqwrite', 'randwrite', 'randread', 'seqread', 'randrw']:
        item = 'fio-' + scene
        fio_json = json.loads((directory / 'results' / (item + '.json')).read_text())
        expected_jobs = (1 if scene in ['seqwrite', 'seqread'] else
                         8 if scene in ['bigread', 'bigwrite'] else 4)
        assert all(job['error'] == 0 for job in fio_json['jobs']), (directory, item)
        assert len(fio_json['jobs']) == 1, (directory, item, 'unexpected fio groups')
        assert int(fio_json['jobs'][0]['job options']['numjobs']) == expected_jobs
        assert get('summary', item, 'status') == 'pass', (directory, item)
        read = get('fio', item, 'read_bw_mib_s', 0)
        write = get('fio', item, 'write_bw_mib_s', 0)
        read_mib = get('fio', item, 'read_io_mib', 0)
        write_mib = get('fio', item, 'write_io_mib', 0)
        wall = get('summary', item, 'tool_wall_s')
        active = get('fio', item, 'active_io_runtime_s')
        drain = get('drain', item, 'extended_strict_drain_s', 0)
        assert active >= 19.5 and wall > 0, (directory, scene, active, wall)
        assert get('drain', item, 'post_write_drain_timeout', 0) == 0
        if write:
            assert get('drain', item, 'completion_evidence') == 'extended'
            for debt in ['drain_pending_mib', 'drain_dirty_mib', 'drain_buffer_dirty_mib']:
                assert get('drain', item, debt) == 0, (directory, scene, debt)
        row = {
            'active_mib_s': read + write,
            'effective_wall_mib_s': (read_mib + write_mib) / wall,
            'effective_active_plus_drain_mib_s': (read_mib + write_mib) / (active + drain),
            'wall_plus_drain_mib_s': (read_mib + write_mib) / (wall + drain),
            'wall_seconds': wall, 'active_seconds': active, 'strict_drain_seconds': drain,
        }
        start = boundaries[item, 'tool_start']
        returned = boundaries[item, 'tool_return']
        completed = boundaries[item, 'post-write_extended_drained'] if write else returned
        precise_wall = (returned - start) / 1e9
        precise_envelope = (completed - start) / 1e9
        assert precise_wall >= active and precise_envelope >= precise_wall
        row.update({
            'precise_tool_wall_seconds': precise_wall,
            'precise_wall_plus_drain_seconds': precise_envelope,
            'precise_wall_plus_drain_mib_s': (read_mib + write_mib) / precise_envelope,
        })
        if write:
            drain_start = boundaries[item, 'post-write_drain_start']
            row['post_tool_observation_gap_seconds'] = (drain_start - returned) / 1e9
            row['precise_strict_drain_seconds'] = (completed - drain_start) / 1e9
        for op in ['read', 'write']:
            if get('fio', item, op + '_bw_mib_s', 0):
                row[op + '_p99_ms'] = get('fio', item, op + '_total_p99_ms')
                row[op + '_clat_p99_ms'] = get('fio', item, op + '_clat_p99_ms')
                row[op + '_total_ios'] = get('fio', item, op + '_total_ios')
                row[op + '_p99_sample_sufficient'] = bool(get('fio', item, op + '_p99_sample_sufficient'))
        result[scene] = row
    return {'source': str(directory), 'status': status, 'workloads': result}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--stage', default='ab')
    parser.add_argument('--scenes', nargs='+')
    args = parser.parse_args()
    arms = {
        variant: [read_arm(ROOT / 'compose' / f'{args.stage}-{pair:02d}-{variant}', args.scenes)
                  for pair in range(1, 4)]
        for variant in ['baseline', 'candidate']
    }
    workloads = {}
    for scene in arms['baseline'][0]['workloads']:
        rows = {variant: [arm['workloads'][scene] for arm in variants]
                for variant, variants in arms.items()}
        names = [key for key in rows['baseline'][0]
                 if key.endswith('mib_s') or key.endswith('p99_ms')]
        workloads[scene] = {
            name: paired([row[name] for row in rows['baseline']],
                         [row[name] for row in rows['candidate']],
                         higher_is_better=name.endswith('mib_s'))
            for name in names
        }
    output = {'method': 'Paired log(B/A) Student t, n=3, df=2, two-sided 95%; all rounds retained',
              'arms': arms, 'workloads': workloads}
    destination = ROOT / f'{args.stage}-summary.json'
    destination.write_text(json.dumps(output, indent=2) + '\n')
    lines = ['| Scene | Baseline active | Candidate active | Change (95% CI) | Baseline wall+drain | Candidate wall+drain |',
             '|---|---:|---:|---|---:|---:|']
    for scene, data in workloads.items():
        active = data['active_mib_s']
        wall = data['wall_plus_drain_mib_s']
        lo, hi = active['paired_95_ci_pct']
        lines.append(f"| {scene} | {active['baseline_median']:.3f} | {active['candidate_median']:.3f} | "
                     f"{active['paired_geomean_change_pct']:+.2f}% [{lo:+.2f}, {hi:+.2f}]% | "
                     f"{wall['baseline_median']:.3f} | {wall['candidate_median']:.3f} |")
    (ROOT / f'{args.stage}-table.md').write_text('\n'.join(lines) + '\n')
    print('\n'.join(lines))


if __name__ == '__main__':
    main()
