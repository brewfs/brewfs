#!/usr/bin/env python3
"""Audit source identity, completed gates, teardown, and hard numerical targets."""
import datetime
import hashlib
import json
import pathlib

import summarize

root = pathlib.Path('/home/hxy/perf-seqwrite-20261102')
repo = pathlib.Path('/home/hxy/brewfs-main')
gates = {}
for status in sorted((root / 'validation').glob('*.status')):
    name = status.stem
    gates[name] = {
        'status': int(status.read_text()),
        'log': str(status.with_suffix('.log')),
        'command': status.with_suffix('.command').read_text().strip()
        if status.with_suffix('.command').exists() else None,
    }
required = [
    'fmt', 'syntax-run_perf_in_container', 'syntax-run_redis_perf',
    'syntax-run_juicefs_perf_in_container', 'syntax-run_juicefs_perf',
    'perf-report-delta', 'juicefs-direct-matrix', 'juicefs-perf-report',
    'test-workspace', 'overlay-full', 'check-workspace', 'build-workspace',
    'check-tokio', 'check-io-uring', 'clippy-workspace', 'clippy-all-targets',
    'clippy-overlay', 'diff-check', 'ci',
]
manifest_path = root / 'build/candidate.sources.after.json'
source_mismatches = []
if manifest_path.exists():
    manifest = json.loads(manifest_path.read_text())
    for name, expected in manifest['files'].items():
        if name.startswith(('src/', 'vendor/')) or name in ['Cargo.toml', 'Cargo.lock', 'build.rs']:
            path = repo / name
            if not path.exists() or hashlib.file_digest(path.open('rb'), 'sha256').hexdigest() != expected:
                source_mismatches.append(name)
else:
    source_mismatches.append('candidate manifest missing')

checks = {}
profile_path = root / 'rounds/seqwrite/profile-01-candidate/results/candidate/seqwrite/measure-1/copy-cost.json'
if profile_path.exists():
    profile = json.loads(profile_path.read_text())
    checks['memmove_self'] = {'value': profile['memmove_only_self_pct'], 'limit': 10,
                              'passed': profile['memmove_only_self_pct'] <= 10}
    checks['compression_inclusive'] = {
        'value': profile['compression_and_checksum_union_pct'], 'limit': 15,
        'passed': profile['compression_and_checksum_union_pct'] <= 15,
        'note': 'Uses policy/codec/framing plus checksum union; also retain raw codec and full module bounds.',
    }
    checks['profile_valid'] = {'passed': profile['lost_samples'] == 0 and profile['unknown_self_pct'] < 5,
                               'lost_samples': profile['lost_samples'],
                               'unknown_self_pct': profile['unknown_self_pct']}
ab_path = root / 'ab-summary.json'
if ab_path.exists():
    ab = json.loads(ab_path.read_text())
    for metric, minimum in [('active_mib_s', 150), ('wall_plus_drain_mib_s', 130),
                            ('precise_wall_plus_drain_mib_s', 130)]:
        value = ab['workloads']['seqwrite'][metric]['candidate_median']
        checks['seqwrite_' + metric] = {'value': value, 'minimum': minimum, 'passed': value >= minimum}
    for scene in ['randwrite', 'randread', 'seqread', 'randrw']:
        value = ab['workloads'][scene]['active_mib_s']
        checks[scene + '_nonregression'] = {
            'passed': value['non_regression_3pct_established'],
            'geomean_change_pct': value['paired_geomean_change_pct'],
            '95_ci_pct': value['paired_95_ci_pct'],
        }
    for op in ['read', 'write']:
        value = ab['workloads']['randrw'].get(op + '_p99_ms')
        if value:
            checks['randrw_' + op + '_p99_nonregression'] = {
                'passed': value['non_regression_3pct_established'],
                'geomean_change_pct': value['paired_geomean_change_pct'],
                '95_ci_pct': value['paired_95_ci_pct'],
            }

verification = root / 'correctness/candidate-summary.json'
checks['content_and_crash'] = {'passed': False}
if verification.exists():
    value = json.loads(verification.read_text())
    checks['content_and_crash'] = {
        'passed': value['all_sha256_match'] and value['all_restarts_passed'],
        'bytes': value['verified_bytes'], 'cases': len(value['checks']),
    }
checks['local_ci'] = {
    'passed': all(name in gates and gates[name]['status'] == 0 for name in required),
    'missing_or_failed': [name for name in required if name not in gates or gates[name]['status'] != 0],
}
checks['source_identity'] = {'passed': not source_mismatches, 'mismatches': source_mismatches}
for stage in ['broad', 'compressible']:
    expected = 2 if stage == 'broad' else 6
    paths = sorted((root / 'compose').glob(stage + '-*/status.json'))
    checks[stage + '_complete'] = {'passed': len(paths) == expected and all(
        json.loads(path.read_text())['status'] == 0 for path in paths), 'arms': len(paths)}
if checks['broad_complete']['passed']:
    scenes = ['seqread', 'seqwrite', 'randread', 'randwrite', 'randrw', 'bigread', 'bigwrite']
    broad = {variant: summarize.read_arm(root / 'compose' / ('broad-01-' + variant), scenes)
             for variant in ['baseline', 'candidate']}
    regressions = {}
    for scene in scenes:
        before = broad['baseline']['workloads'][scene]['active_mib_s']
        after = broad['candidate']['workloads'][scene]['active_mib_s']
        change = (after / before - 1) * 100
        if change < -3:
            regressions[scene] = {'baseline_mib_s': before, 'candidate_mib_s': after,
                                  'change_pct': change}
    checks['broad_observed_nonregression'] = {
        'passed': not regressions, 'unresolved_regressions': regressions,
        'note': 'One pair only; tool exit success does not establish performance acceptance.',
    }
paths = sorted((root / 'compose').glob('direct-*/status.json'))
checks['compose_direct_guard_complete'] = {'passed': len(paths) == 2 and all(
    json.loads(path.read_text())['status'] == 0 for path in paths), 'arms': len(paths)}
guards = sorted((root / 'rounds').glob('*-direct/guard-*/complete.json'))
checks['direct_guard_complete'] = {'passed': len(guards) == 6, 'arms': len(guards)}
teardowns = []
for path in sorted((root / 'correctness').glob('*/teardown.json')):
    value = json.loads(path.read_text())
    teardowns.append({'path': str(path), **value})
checks['crash_teardowns'] = {'passed': len(teardowns) == 8 and all(
    not value['still_mounted'] and value['unmount_status'] == 0 and
    value['returncode'] == (-9 if value['signal'] else 0) for value in teardowns),
    'count': len(teardowns)}
output = {
    'generated_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'checks': checks, 'gates': gates, 'correctness_teardowns': teardowns,
    'automatic_merge_allowed': len(checks) >= 17 and all(value['passed'] for value in checks.values()),
    'note': 'Even all checks passing requires manual review of broad, compressible, object amplification and drain evidence.',
}
(root / 'final-audit.json').write_text(json.dumps(output, indent=2) + '\n')
print(json.dumps({'checks': checks, 'automatic_merge_allowed': output['automatic_merge_allowed']}, indent=2))
