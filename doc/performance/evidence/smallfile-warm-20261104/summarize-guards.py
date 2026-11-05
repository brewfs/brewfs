#!/usr/bin/env python3
"""Preserve canonical artifact comparisons and extract guard review rows."""
import csv
import hashlib
import json
import pathlib
import re
import subprocess
import sys

root = pathlib.Path('/mnt/brewfs-data/home/hxy/smallfile-warm')
compare = pathlib.Path('/home/hxy/brewfs-main/tools/perf/compare_artifacts.py')
out = root / 'guard-comparisons'
out.mkdir(exist_ok=True)
source_revision = (root / 'main-source.txt').read_text().strip()
candidate_patch_hash = hashlib.sha256((root / 'candidate-source.patch').read_bytes()).hexdigest()
binary_hashes = {}
for arm in ('main', 'candidate'):
    for line in (root / f'{arm}-binaries.sha256').read_text().splitlines():
        digest, name = line.split(maxsplit=1)
        if pathlib.Path(name).name == f'brewfs-{arm}':
            binary_hashes[arm] = digest
groups = {}
for path in sorted((root / 'artifacts').glob('guard-*')):
    match = re.fullmatch(r'guard-(.+)-(main|candidate)-d([01])', path.name)
    if match:
        label, arm, direct = match.groups()
        groups.setdefault((label, direct), {})[arm] = path

results = []
for (label, direct), arms in groups.items():
    if set(arms) != {'main', 'candidate'}:
        results.append(dict(label=label, direct=int(direct), status='incomplete'))
        continue
    checks = {}
    for arm, path in arms.items():
        with (path / 'perf-summary.tsv').open() as stream:
            statuses = list(csv.DictReader(stream, delimiter='\t'))
        with (path / 'post-write-drain.tsv').open() as stream:
            drains = list(csv.DictReader(stream, delimiter='\t'))
        checks[arm] = dict(
            complete=(path / 'perf.complete').exists(),
            invalid_markers=[p.name for p in path.glob('*.invalid')],
            statuses=statuses, drains=drains,
            manifest=json.loads((path / 'manifest.json').read_text()))
        source = checks[arm]['manifest']['source']
        assert source['binary_sha256'] == binary_hashes[arm], (label, arm)
        assert source['revision'] == source_revision, (label, arm)
        assert source['dirty'] == (arm == 'candidate'), (label, arm)
        assert source['dirty_diff_sha256'] == (candidate_patch_hash if arm == 'candidate' else None), (label, arm)
    assert checks['main']['manifest']['comparability'] == checks['candidate']['manifest']['comparability'], label
    if any(not item['complete'] or item['invalid_markers'] or not item['statuses']
           or any(status['status'] != 'pass' for status in item['statuses'])
           for item in checks.values()):
        results.append(dict(label=label, direct=int(direct), status='incomplete_or_failed', checks=checks))
        continue
    target = out / f'{label}-d{direct}.tsv'
    subprocess.run([sys.executable, str(compare), str(arms['main']), str(arms['candidate']),
                    '--format', 'tsv', '--output', str(target)], check=True)
    with target.open() as stream:
        rows = list(csv.DictReader(stream, delimiter='\t'))
    selected = [r for r in rows if
                r['kind'] in ('summary', 'drain', 'gap', 'amplification') or
                r['metric'].endswith(('effective_wall_bw_mib_s',
                                       'effective_active_plus_drain_bw_mib_s',
                                       'total_p99_ms', 'clat_p99_ms')) or
                r['metric'] in ('read_bw_mib_s', 'write_bw_mib_s')]
    metaperf = {}
    if label == 'metaperf':
        for arm, path in arms.items():
            metaperf[arm] = (path / 'tools/metaperf.log').read_text()
    results.append(dict(label=label, direct=int(direct), checks=checks,
                        metrics=selected, metaperf=metaperf))

(root / 'guard-summary.json').write_text(json.dumps(results, indent=2) + '\n')
for row in results:
    print(row['label'], 'direct=' + str(row['direct']))
    if 'status' in row:
        print(row['status'])
    else:
        for metric in row['metrics']:
            if metric['kind'] != 'gap':
                print(metric['item'], metric['metric'], metric['baseline'],
                      metric['candidate'], metric['delta_pct'])
