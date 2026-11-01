"""Read-only reproduction of the failed campaign's archive and report audit.

Exit zero means the evidence matches the published failed-campaign record.
It does not accept full observability or uninterrupted six-cell performance.
No archive is extracted; no workload, observer or cloud API is invoked.
"""
import argparse
import concurrent.futures
import csv
import hashlib
import json
from pathlib import Path
import tarfile

import verify_cloud_results as frozen

HERE = Path(__file__).resolve().parent
REPORT = HERE.parents[2] / 'doc/testing/entity-packed-million-r2'


def read(path):
    return json.loads(path.read_text())


def verify_sources():
    for item in read(REPORT / 'source-digests.json')['files']:
        actual = hashlib.sha256((HERE / item['path']).read_bytes()).hexdigest()
        assert actual == item['published_sha256'] == item['measured_sha256'], item['path']
    manifest = read(HERE / 'frozen/frozen-files.json')
    assert len(manifest) == 26
    for item in manifest:
        assert hashlib.sha256((HERE / 'frozen' / item['path']).read_bytes()).hexdigest() == item['sha256']


def verify_archive(role, ref, directory):
    path = directory / ref['filename']
    assert path.stat().st_size == ref['bytes'], path
    with path.open('rb') as source:
        assert hashlib.file_digest(source, 'sha256').hexdigest() == ref['sha256'], path
    parsed = frozen.scan(path)
    assert parsed['saved']['deployment-public.json']['role'] == role
    assert parsed['members'] == ref['members']
    if role == 'client':
        wanted = {x['path'] for x in read(HERE / 'frozen/frozen-files.json')}
        found = set()
        with tarfile.open(path, 'r:gz') as archive:
            for member in archive:
                if member.name == 'frozen-files.json':
                    assert archive.extractfile(member).read() == (HERE / 'frozen/frozen-files.json').read_bytes()
                elif member.name in wanted:
                    assert archive.extractfile(member).read() == (HERE / 'frozen' / member.name).read_bytes()
                    found.add(member.name)
        assert wanted-found == {'harness/backend-image-lock.json', 'runtime-source-provenance.json'}
    return role, parsed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archives', type=Path, help='Directory containing the three private *-final.tar.gz files')
    parser.add_argument('--sources-only', action='store_true', help='Check published/measured source hashes and data tables only')
    args = parser.parse_args()
    assert args.sources_only or args.archives, 'Supply --archives or --sources-only'
    if not __debug__:
        parser.error('Run Python without -O; assertions are verification gates')
    verify_sources()
    report = read(REPORT / 'results.json')
    validation = report['validation']
    assert validation['strict_acceptance_passed'] is False
    assert validation['full_observability_acceptance'] is False
    assert validation['uninterrupted_six_cell_performance_acceptance'] is False
    cells = report['cells']
    assert [(c['client'], c['repetition']) for c in cells] == [
        ('brewfs', 1), ('juicefs', 1), ('juicefs', 2), ('brewfs', 2), ('brewfs', 3), ('juicefs', 3)]
    with (REPORT / 'cells.csv').open(newline='') as source:
        csv_cells = list(csv.DictReader(source))
    with (REPORT / 'workspaces.csv').open(newline='') as source:
        csv_workspaces = list(csv.DictReader(source))
    assert len(csv_cells) == 6 and len(csv_workspaces) == 24
    for cell, tabular in zip(cells, csv_cells):
        assert cell['client'] == tabular['client'] and cell['repetition'] == int(tabular['repetition'])
        for key in ['lifecycle_s', 'correctness_verification_s', 'lifecycle_plus_correctness_s']:
            assert cell[key] == float(tabular[key])
        assert cell['lifecycle_plus_correctness_s'] == cell['lifecycle_s'] + cell['correctness_verification_s']
        for item in cell['details']:
            tab = csv_workspaces.pop(0)
            assert (tab['client'], int(tab['round']), int(tab['workspace'])) == (
                cell['client'], cell['repetition'], item['workspace'])
            for field, value in [('bootstrap_s', item['bootstrap_s']), ('clone_s', item['clone_s']),
                                 ('prewarm_s', item['read']['prewarm_s']), ('read_s', item['read']['active_s']),
                                 ('read_p95_ms', item['read']['p95_ms']), ('cow_s', item['cow']['active_s'])]:
                assert float(tab[field]) == value
            assert tab['trace_sha256'] == validation['trace_sha256'] == item['read']['trace_sha256']
    if args.sources_only:
        print(json.dumps(dict(source_hashes_and_tables_match=True, strict_acceptance_passed=False)))
        return
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(verify_archive, role, ref, args.archives)
                   for role, ref in validation['archive_references'].items()]
        nodes = dict(f.result() for f in futures)
    client = nodes['client']; saved = client['saved']
    assert saved['experiment-contract.json'] == report['contract']
    assert client['samples'] == validation['raw_sample_counts'] == dict(read=98304, prewarm=12288, cow=2400)
    assert client['sample_files'] == dict(read=24, prewarm=24, cow=24)
    assert client['traces'] == [validation['trace_sha256']]
    actual_rows = saved['artifacts/rows.json']
    assert len(actual_rows) == 6
    for actual, public in zip(actual_rows, cells):
        for key in ['client', 'repetition', 'files', 'workspaces', 'lifecycle_s', 'observed_phase_s',
                    'correctness_verification_s', 'remote_ready', 'source_names_verified', 'remote_logical_verification']:
            assert actual[key] == public[key], (key, actual['client'], actual['repetition'])
        recovered = actual.get('recovered_interrupted_cell', False)
        assert recovered == public['recovered_interrupted_cell']
        if recovered:
            assert actual['uninterrupted_performance_sample_valid'] is False
            assert actual['object_counters_before'] is None and public['physical_http_delta'] is None
        for private, published in zip(actual['details'], public['details']):
            assert private['read'] == published['read']
            assert {k:v for k,v in private['cow'].items() if k != 'modified_indices'} == published['cow']
        tails = actual['details'] if actual['client'] == 'brewfs' else [actual['shared_teardown']]
        for tail in tails:
            assert tail['remote_ready'] and not tail['assisted_fuse_cleanup']
            assert all(a['returncode'] == 0 for a in tail['ordinary_unmount_attempts'])
            state = tail['real_worker_exit']
            assert state['complete'] and state['returncode'] == 0 and not state['live']
            assert state['exits'] and all(e['returncode'] == 0 for e in state['exits'])
    assert sum(c['recovered_interrupted_cell'] for c in cells) == 1
    start = saved['artifacts/campaign-started.json']['utc_ns']
    end = saved['artifacts/completed.json']['ended']['utc_ns']
    assert (end-start)/1e9 == validation['campaign_operation_span_s']
    assert saved['artifacts/runner-result.json']['full_observability_acceptance'] is False
    for role, node in nodes.items():
        host = [(t,b) for t,b in node['hosts'] if start <= t <= end]
        assert len({b for _,b in host}) == 1
        assert frozen.gap([t for t,_ in host], start, end) == validation['coverage'][role]['maximum_host_gap_s']
        assert len(host) == validation['coverage'][role]['host_samples']
    assert validation['coverage']['objects']['maximum_host_gap_s'] > 30
    assert sorted(saved['artifacts/metrics-catalog.json']) == ['juicefs-mount']
    for name in ['brewfs-mount', 'juicefs-sync', 'juicefs-mount']:
        assert len(client['prom'][name]['success']) == validation['coverage']['client']['exporter_success_counts'][name]
    assert nodes['objects']['http'] == validation['physical_http_all_phases']
    print(json.dumps(dict(evidence_matches_published_record=True, archive_members=sum(n['members'] for n in nodes.values()),
                          raw_samples=client['samples'], strict_acceptance_passed=False,
                          full_observability_acceptance=False, clean_operation_pairs=2)))


if __name__ == '__main__':
    main()
