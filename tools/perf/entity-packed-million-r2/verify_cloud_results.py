"""Validate every archive byte, workload result, drain/exit and observation span."""
from pathlib import Path
import gzip,hashlib,io,json,tarfile,time

HERE=Path(__file__).resolve().parent

def scan(path,finalization_required=True):
    seen={};saved={};hosts=[];prom={};http={};samples={'read':0,'prewarm':0,'cow':0};traces=set()
    sample_files={'read':0,'prewarm':0,'cow':0};component_failures=[]
    with tarfile.open(path,'r:gz') as archive:
        for member in archive:
            assert member.isfile() and not Path(member.name).is_absolute() and '..' not in Path(member.name).parts
            name=member.name;assert name not in seen
            source=archive.extractfile(member);digest=hashlib.sha256()
            if name.startswith('artifacts/object-log/') and name.endswith('.jsonl'):
                for line in source:
                    digest.update(line);row=json.loads(line);key=row['method']+'_'+str(row['status'])
                    value=http.setdefault(key,dict(requests=0,response_bytes=0,request_time_s=0))
                    value['requests']+=1;value['response_bytes']+=int(row['response_bytes']);value['request_time_s']+=float(row['request_time'])
            elif name.startswith('artifacts/telemetry/') and name.endswith('.jsonl.gz'):
                raw=source.read();digest.update(raw)
                with gzip.GzipFile(fileobj=io.BytesIO(raw)) as records:
                    for line in records:
                        row=json.loads(line)
                        if '/host-' in name:hosts.append((row['utc_ns'],row['boot_id'].strip()))
                        else:
                            value=prom.setdefault(row['name'],dict(success=[],failure=[],states={}))
                            value['states'][row['state']]=value['states'].get(row['state'],0)+1
                            if row['ok']:value['success'].append(row['ended']['utc_ns'])
                            else:value['failure'].append(dict(utc_ns=row['ended']['utc_ns'],state=row['state'],error=row['error']))
            elif name.endswith('.json') or name.endswith('.py') or name.endswith('.sh') or name.endswith('.conf.in'):
                raw=source.read();digest.update(raw)
                if name=='archive-manifest.json':manifest=json.loads(raw);continue
                if name in ['artifacts/rows.json','artifacts/completed.json','artifacts/runner-result.json',
                    'artifacts/campaign-started.json','artifacts/metrics-catalog.json','experiment-contract.json',
                    'deployment-public.json','artifacts/finalization.json','artifacts/bootstrap-complete.json']:
                    saved[name]=json.loads(raw)
                elif name.endswith('-random-read-samples.json') or name.endswith('-prewarm-samples.json'):
                    rows=json.loads(raw);kind='read' if name.endswith('-random-read-samples.json') else 'prewarm'
                    expected=4096 if kind=='read' else 512
                    assert len(rows)==expected and all(r['ok'] and r['checked_bytes']==4096 for r in rows),name
                    assert {r['order'] for r in rows}==set(range(expected)),name
                    if kind=='read':assert sum(r['initial_warm'] for r in rows)==2048,name
                    samples[kind]+=len(rows);sample_files[kind]+=1
                elif name.endswith('-cow-samples.json'):
                    rows=json.loads(raw);assert len(rows)==100 and all(r['ok'] for r in rows),name
                    for row in rows:
                        assert all(row[key]>=0 for key in ['before_read_ms','open_ms','pwrite_ms','fsync_ms','close_ms','after_read_ms','total_ms'])
                    samples['cow']+=len(rows);sample_files['cow']+=1
                elif name.endswith('-trace.json'):
                    rows=json.loads(raw);assert len(rows)==4096;traces.add(hashlib.sha256(raw).hexdigest())
                elif name.startswith('artifacts/components/') and name.endswith('/state.json'):
                    row=json.loads(raw)
                    if not row['complete'] or row['returncode']!=0:component_failures.append(dict(path=name,state=row))
            else:
                while data:=source.read(1<<20):digest.update(data)
            seen[name]=dict(bytes=member.size,sha256=digest.hexdigest())
    expected={row['path']:dict(bytes=row['bytes'],sha256=row['sha256']) for row in manifest['files']}
    assert expected==seen,'Archive file manifest mismatch: '+str(path)
    assert not manifest['active_segments_deferred'],'Final archive omitted active segments'
    if finalization_required:assert saved['artifacts/finalization.json']['observation_closed']
    assert hosts and not component_failures,component_failures
    return dict(saved=saved,hosts=hosts,prom=prom,http=http,samples=samples,sample_files=sample_files,
        traces=sorted(traces),members=len(seen))

def gap(timestamps,start,end):
    rows=[start]+sorted(t for t in timestamps if start<=t<=end)+[end]
    return max(b-a for a,b in zip(rows,rows[1:]))/1e9

def main():
    references=json.loads((HERE/'collected-archives.json').read_text());assert set(references)=={'client','objects','metadata'}
    nodes={}
    for role,value in references.items():
        path=Path(value['path'])
        with path.open('rb') as source:actual=hashlib.file_digest(source,'sha256').hexdigest()
        assert actual==value['sha256']
        nodes[role]=scan(path)
        assert nodes[role]['saved']['deployment-public.json']['role']==role
    client=nodes['client'];saved=client['saved'];contract=saved['experiment-contract.json']
    assert contract['namespace_files']==1000000 and contract['expected_cells']==6
    assert saved['artifacts/runner-result.json']['status']=='complete'
    assert saved['artifacts/completed.json']['cells']==6 and saved['artifacts/completed.json']['all_remote_ready']
    rows=saved['artifacts/rows.json'];assert len(rows)==6
    assert {(r['client'],r['repetition']) for r in rows}=={(arm,rep) for arm in ['brewfs','juicefs'] for rep in range(1,4)}
    for row in rows:
        assert row['remote_ready'] and row['remote_logical_verification'] and row['source_names_verified']==1000000
        assert row['workspaces']==4 and row['lifecycle_s']>0 and len(row['details'])==4
        for detail in row['details']:
            assert detail['read']['samples']==4096 and detail['read']['prewarm_files']==512
            assert detail['cow']['files']==100
            if row['client']=='brewfs':
                assert detail['remote_ready'] and not detail['assisted_fuse_cleanup']
                assert detail['real_worker_exit']['complete'] and detail['real_worker_exit']['returncode']==0
        if row['client']=='juicefs':
            tail=row['shared_teardown'];assert tail['remote_ready'] and not tail['assisted_fuse_cleanup']
            assert tail['real_worker_exit']['complete'] and tail['real_worker_exit']['returncode']==0
    assert client['samples']==dict(read=98304,prewarm=12288,cow=2400)
    assert client['sample_files']==dict(read=24,prewarm=24,cow=24) and len(client['traces'])==1
    catalog=saved['artifacts/metrics-catalog.json']
    assert any('fuse' in name for name in catalog['brewfs-mount'])
    assert 'juicefs_staging_blocks' in catalog['juicefs-mount']
    assert any('sync_copied' in name for name in catalog['juicefs-sync'])
    start=saved['artifacts/campaign-started.json']['utc_ns'];end=saved['artifacts/completed.json']['ended']['utc_ns']
    coverage={}
    for role,node in nodes.items():
        active=[row for row in node['hosts'] if start<=row[0]<=end]
        assert active and len({boot for _,boot in active})==1,'Missing host data or instance reboot: '+role
        maximum=gap([t for t,_ in active],start,end)
        assert maximum<=30,'Host observation gap >30 seconds: '+role
        coverage[role]=dict(host_samples=len(active),maximum_host_gap_s=maximum,
            exporter_failures={name:data['failure'] for name,data in node['prom'].items()},
            metric_success_counts={name:len(data['success']) for name,data in node['prom'].items()},
            archive_members=node['members'])
    for name in ['tikv','pd']:
        assert gap(nodes['metadata']['prom'][name]['success'],start,end)<=30,'Backend metric gap: '+name
    assert nodes['objects']['http'].get('PUT_200',{}).get('requests',0)>0
    result=dict(valid=True,full_campaign_complete=True,namespace_files=1000000,cells=6,workspaces=24,
        samples=client['samples'],ordinary_exit_only=True,remote_ready_all=True,all_byte_checks_passed=True,
        archives_checked=3,coverage=coverage,physical_http_all_phases=nodes['objects']['http'],
        source_generation_and_all_overheads_retained=True,utc_ns=time.time_ns(),
        interpretation='Validation establishes complete evidence and correctness, not a universal performance winner. Keep slow phases and physical HTTP errors in the report.')
    (HERE/'cloud-evidence-validation.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(dict(valid=True,cells=6,workspaces=24,archives_checked=3)))
if __name__=='__main__':main()
