#!/usr/bin/env python3
"""Record local gate results and immutable experiment inputs for the report."""
import datetime
import hashlib
import json
import pathlib

root = pathlib.Path('/mnt/brewfs-data/home/hxy/smallfile-warm')
gates = root / 'gates'


def identity(path):
    data = path.read_bytes()
    return dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())


results = []
for line in (gates / 'status.tsv').read_text().splitlines():
    label, status = line.split('\t')
    log = gates / f'{label}.log'
    results.append(dict(
        gate=label, exit_code=int(status),
        started=(gates / f'{label}.started').read_text().strip(),
        log=identity(log),
        test_results=[row for row in log.read_text().splitlines()
                      if row.startswith('test result:')]))

inputs = [
    'main-source.txt', 'main-binaries.sha256', 'candidate-binaries.sha256',
    'candidate-source.patch', 'candidate-reproduction.patch',
    'candidate-tokio.rs', 'candidate-tokio-tests.rs',
    'build-main.sh', 'build-candidate.sh', 'rustc-serial.sh',
    'compose.yml', 'compose-guard.yml', 'compose.sh', 'compose-guard.sh',
    'make-fixture.sh', 'manifest-key.source.json', 'run-pairs.sh',
    'run-acceptance.sh', 'post-scan.py', 'run-main-attribution.sh',
    'run-attribute.sh', 'attribute.py', 'candidate-attribution-after-pairs.sh',
    'run-gates.sh', 'run-guard-pairs.sh', 'native-xfstests.sha256',
    'summarize-guards.py', 'run-guard-diagnostic.sh', 'run-diagnostic-mixed-pairs.sh',
    'capture-listener.py', 'run-remount-probe.sh', 'run-remount-probes.sh',
    'summarize-diagnostic-mixed.py',
]
result = dict(
    recorded_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    disposition='rejected: absolute warm target missed; no performance acceptance',
    gate_pipeline_complete=(gates / 'done').exists(), gates=results,
    experiment_input_identity={name: identity(root / name) for name in inputs},
    validation_scope='Preserved candidate source and binaries; final PR contains documentation only.',
    ignored_tests='Ignored repository tests remain ignored; an exit-zero gate is not a claim that they ran.',
    compiler_serialization='One Cargo invocation at a time; flock serializes compiler processes; no compile/perf overlap.',
    github_actions_used=False)
(root / 'validation-summary.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({key: result[key] for key in ('recorded_at', 'gate_pipeline_complete')}, indent=2))
for item in results:
    print(item['gate'], item['exit_code'], item['test_results'])
