#!/usr/bin/env python3
"""Summarize all five fixed A/B pairs; never select or retry by performance."""
import argparse
import importlib.util
import json
import math
from pathlib import Path
import statistics
import subprocess
import sys


def describe(values):
    return {
        'n': len(values), 'values': values, 'median': statistics.median(values),
        'mean': statistics.mean(values), 'min': min(values), 'max': max(values),
        'range': max(values) - min(values), 'sample_stddev': statistics.stdev(values),
    }


def summarize(directory):
    manifest = json.loads((directory / 'manifest.json').read_text())
    runs = json.loads((directory / 'runs.json').read_text())
    if manifest['rounds'] != 5 or len(runs) != 10:
        raise ValueError('this analysis requires the complete predeclared five-pair matrix')
    expected = [(i, mode) for i in range(1, 6) for mode in ['baseline', 'default']]
    if [(row['round'], row['mode']) for row in runs] != expected:
        raise ValueError('runs are not in the fixed A1 B1 ... A5 B5 order')
    repo = Path(__file__).resolve().parents[2]
    spec = importlib.util.spec_from_file_location('comparison', repo / 'tools/perf/compare_artifacts.py')
    comparison = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = comparison
    spec.loader.exec_module(comparison)
    raw = []
    for run in runs:
        assert run['status'] == 0 and len(run['artifacts']) == 1, run
        artifact = Path(run['artifacts'][0])
        assert (artifact / 'binary-sha256.txt').read_text().strip() == manifest['binaries'][run['mode']]['sha256']
        assert json.loads((artifact / 'unmount-check.json').read_text())['remaining_fuse_mounts'] == []
        assert json.loads((artifact / 'unmount-check.json').read_text())['remaining_mount_processes'] == []
        assert json.loads((artifact / 'compose-cleanup.json').read_text()) == {'containers': '', 'volumes': ''}
        metrics = comparison.load_artifact(artifact)
        for scene in manifest['tools'].split():
            if not scene.startswith('fio-'):
                continue
            result = json.loads((artifact / 'results' / f'{scene}.json').read_text())
            assert len(result['jobs']) == 1
            job = result['jobs'][0]
            assert job['error'] == 0
            options = job['job options']
            assert str(options['randseed']) == '20261031', options
            assert str(options['randrepeat']) == '1', options
            assert max(job['read']['runtime'], job['write']['runtime']) >= 29700
            row = dict(round=run['round'], arm=run['mode'], scene=scene, artifact=str(artifact))
            row['mib_s'] = sum(job[d]['bw_bytes'] for d in ['read', 'write']) / 1048576
            for direction in ['read', 'write']:
                percentiles = job[direction].get('clat_ns', {}).get('percentile', {})
                row[f'{direction}_p99_ms'] = percentiles.get('99.000000', 0) / 1e6 if job[direction]['io_bytes'] else None
            for suffix in ['effective_wall_bw_mib_s', 'effective_active_plus_drain_bw_mib_s']:
                values = [metrics.get(('runtime', scene, d + '_' + suffix)) for d in ['read', 'write']]
                present = [value.value for value in values if value is not None]
                row[suffix] = sum(present) if present else None
            row['fio_job_options'] = options
            raw.append(row)
    scenes = {}
    for scene in manifest['tools'].split():
        if not scene.startswith('fio-'):
            continue
        arms = {arm: [row for row in raw if row['scene'] == scene and row['arm'] == arm]
                for arm in ['baseline', 'default']}
        summary = {}
        for arm, rows in arms.items():
            summary[arm] = {metric: describe([row[metric] for row in rows])
                            for metric in ['mib_s', 'read_p99_ms', 'write_p99_ms',
                                           'effective_wall_bw_mib_s', 'effective_active_plus_drain_bw_mib_s']
                            if all(row[metric] is not None for row in rows)}
        a = [row['mib_s'] for row in arms['baseline']]
        b = [row['mib_s'] for row in arms['default']]
        log_ratios = [math.log(y / x) for x, y in zip(a, b)]
        mean = statistics.mean(log_ratios)
        half = 2.7764451051977987 * statistics.stdev(log_ratios) / math.sqrt(5)
        summary['median_delta_pct'] = (statistics.median(b) / statistics.median(a) - 1) * 100
        summary['paired_delta_pct'] = describe([(y / x - 1) * 100 for x, y in zip(a, b)])
        summary['paired_geometric_mean_delta_pct'] = math.expm1(mean) * 100
        summary['paired_geometric_mean_95pct_ci_pct'] = [math.expm1(mean - half) * 100, math.expm1(mean + half) * 100]
        summary['paired_delta_changes_sign'] = min(log_ratios) < 0 < max(log_ratios)
        scenes[scene] = summary
    for round_index in range(1, 6):
        group = {r['mode']: r['artifacts'][0] for r in runs if r['round'] == round_index}
        subprocess.run([sys.executable, str(repo / 'tools/perf/compare_artifacts.py'),
                        group['baseline'], group['default'], '-o',
                        str(directory / f'comparison-round-{round_index}.md')], check=True,
                       stdout=subprocess.DEVNULL)
    result = {'manifest': manifest, 'runs': runs, 'raw': raw, 'scenes': scenes,
              'ci_method': 'Paired mean log(B/A), Student-t df=4, 95%, back-transformed; not median-ratio CI',
              'stddev_method': 'Sample standard deviation, denominator n-1'}
    (directory / 'noise-analysis.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({scene: {key: value for key, value in stats.items()
                              if key not in ['baseline', 'default']}
                      for scene, stats in scenes.items()}, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    summarize(parser.parse_args().directory.resolve())
