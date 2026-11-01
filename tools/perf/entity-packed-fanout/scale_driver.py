"""Large immutable namespaces and isolated COW workspaces on the owned cloud VM."""
from pathlib import Path
import json
import os
import subprocess
import shutil
import time
import traceback
import uuid

import campaign as c
import scale_workloads as sw

def open_cell(label,client,revision,meta=None,writable=False):
    return c.mount_cell(label,client,'tikv','packed' if client=='brewfs' else 'none',
        revision,'upload_before_commit',8,meta,writable_existing=writable)

def verify_names(root,count):
    found=sum(len(files) for _,_,files in os.walk(root/'small'))
    assert found==count,(found,count)
    return found

def prepare(count,cap):
    source=c.WORK/f'scale-source-{count}'
    generated=time.perf_counter()
    descriptor=sw.create_source(source,count)
    # Source generation is common setup. Finish its dirty writeout before
    # either product's separately measured onboarding interval begins.
    os.sync()
    generated=time.perf_counter()-generated
    out=c.WORK/f'scale-producer-{count}'
    art=c.ART/f'scale-onboarding-{count}';art.mkdir()
    manifest=art/'manifest.txt'
    command=[str(c.ROOT/'bin/packed_v3_snapshot_fixture'),'--source-directory',str(source),
        '--output-dir',str(out),'--source-consistency','best-effort-detected',
        '--source-hardlink-policy','visible-links','--bucket',c.CFG['bucket'],
        '--endpoint',c.CFG['endpoint'],'--region','us-east-1','--force-path-style','true',
        '--prefix','scale-'+str(count)+'-'+uuid.uuid4().hex,'--manifest-output',str(manifest),
        '--metadata-codec','raw','--data-codec','raw','--inline-data','off',
        '--chunk-size','4194304','--block-size','4194304','--frame-policy','size-only',
        '--access-profile','random-small-file','--max-random-frame-raw-bytes',str(cap)]
    c.dump(art/'producer-command.json',command)
    c.dump(art/'disk-before.json',dict(zip(['total','used','free'],shutil.disk_usage(c.WORK))))
    begin=time.perf_counter()
    with open(art/'producer.log','w') as log:
        subprocess.run(command,check=True,stdout=log,stderr=subprocess.STDOUT,timeout=3600)
    packed_s=time.perf_counter()-begin
    begin=time.perf_counter()
    cell=open_cell(f'scale-import-{count}','juicefs','stock')
    try:
        env=dict(cell.env,scalebase=cell.meta)
        command=[c.jfs.BINARY,'sync','--threads=8','--dirs','--perms','--inplace',
            '--metrics=127.0.0.1:0',str(source)+'/', 'jfs://scalebase/base/']
        c.dump(art/'juicefs-native-sync-command.json',command)
        with open(art/'juicefs-native-sync.log','w') as log:
            subprocess.run(command,check=True,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=3600)
        cell.stop()
        imported_s=cell.ended-begin
        cell.finish_observer()
    except BaseException:
        cell.cleanup_failure();raise
    state=dict(files=count,frame_cap=cap,manifest=manifest.read_text().strip(),
        juicefs_meta=cell.meta,source_generation_common_s=generated,
        packed_onboarding_s=packed_s,juicefs_onboarding_s=imported_s,
        onboarding_repetitions=1,descriptor=descriptor)
    c.dump(art/'onboarding.json',state)
    return state

def finish(cell):
    cell.stop()
    return dict(ended=cell.ended,remote_ready=True,
        drain_tail_s=cell.ended-cell.debt_probe_started,
        ordinary_unmount_attempts=cell.unmount_attempts,assisted_fuse_cleanup=cell.assisted)

def read_case(label,client,revision,profile,state):
    c.CFG['manifest']=state['manifest']
    begin=time.perf_counter()
    cell=open_cell(label,client,revision,state['juicefs_meta'] if client=='juicefs' else None)
    try:
        root=cell.mount/'base' if client=='juicefs' else cell.mount
        c.backend_metrics(cell.art/'tikv-before.prom')
        offset=c.object_offset()
        c.dump(cell.art/'host-before.json',c.perf_payload.host_snapshot(cell.proc.pid))
        started=time.perf_counter()
        result=sw.full_scan(root,state['files']) if profile=='full-scan' else sw.read_trace(root,state['files'])
        active_end=time.perf_counter()
        c.dump(cell.art/'host-after-work.json',c.perf_payload.host_snapshot(cell.proc.pid))
        teardown=finish(cell)
        result.update(teardown)
        result.update(bootstrap_s=cell.bootstrap_s,work_with_prewarm_s=active_end-started,
            drain_tail_s=cell.ended-active_end,
            lifecycle_s=cell.ended-begin,active_plus_drain_s=cell.ended-started-result.get('prewarm_s',0),
            object_requests_including_prewarm_and_drain=c.object_delta(offset))
        c.backend_metrics(cell.art/'tikv-after.prom')
        assert cell.verify_upper() in (0,None)
        cell.finish_observer()
        return result
    except BaseException:
        (cell.art/'failure.txt').write_text(traceback.format_exc())
        cell.cleanup_failure();raise

def workspace_case(label,client,revision,state):
    c.CFG['manifest']=state['manifest']
    c.backend_metrics(c.ART/(label+'-tikv-before.prom'))
    offset=c.object_offset()
    begin=time.perf_counter()
    cells=[];closed=[];details=[];roots=[]
    result={}
    try:
        shared=None
        if client=='juicefs':
            shared=open_cell(label+'-shared',client,revision,state['juicefs_meta'],writable=True)
            cells.append(shared)
            parent=shared.mount/('workspaces-'+uuid.uuid4().hex);parent.mkdir()
        for workspace in range(4):
            clone_s=0
            if client=='brewfs':
                cell=open_cell(label+f'-w{workspace}',client,revision)
                cells.append(cell)
                root=cell.mount
            else:
                cell=shared
                root=parent/f'w{workspace}'
                command=[c.jfs.BINARY,'clone','-p',str(shared.mount/'base'),str(root)]
                started=time.perf_counter()
                with open(shared.art/f'clone-{workspace}.log','w') as log:
                    subprocess.run(command,check=True,env=shared.env,stdout=log,stderr=subprocess.STDOUT,timeout=1800)
                clone_s=time.perf_counter()-started
                roots.append(root.relative_to(shared.mount))
            started=time.perf_counter()
            read=sw.read_trace(root,state['files'],working_files=1024,operations=4096)
            cow=sw.cow(root,state['files'],workspace)
            operations_s=time.perf_counter()-started
            part=dict(workspace=workspace,clone_s=clone_s,operations_s=operations_s,read=read,cow=cow)
            if client=='brewfs':
                part.update(finish(cell));closed.append(cell)
            details.append(part)
        if shared:
            teardown=finish(shared);closed.append(shared)
            ended=shared.ended
        else:
            teardown={};ended=closed[-1].ended
        result=dict(lifecycle_s=ended-begin,workspaces=4,details=details,
            shared_teardown=teardown,remote_ready=True,
            object_requests_including_all_workspaces=c.object_delta(offset))
        c.backend_metrics(c.ART/(label+'-tikv-after.prom'))
        # All validation below is outside the complete workspace performance clock.
        for cell in closed:
            cell.verify_upper();cell.finish_observer()
        verify=open_cell(label+'-remote-verify',client,revision,
            state['juicefs_meta'] if client=='juicefs' else None)
        cells.append(verify)
        base=verify.mount/'base' if client=='juicefs' else verify.mount
        result['source_names_verified']=verify_names(base,state['files'])
        sw.verify_source_sample(base,state['files'])
        if client=='juicefs':
            for workspace,relative in enumerate(roots):
                root=verify.mount/relative
                assert verify_names(root,state['files'])==state['files']
                for index in sw.working_set(state['files'],100):
                    expected=sw.wl.payload(index+(workspace+1)*10000000,sw.FILE_BYTES)
                    sw.wl.direct_read(sw.path(root,index),0,sw.FILE_BYTES,expected)
        verify.stop();verify.finish_observer()
        return result
    except BaseException:
        (c.ART/(label+'-failure.txt')).write_text(traceback.format_exc())
        for cell in cells:
            cell.cleanup_failure()
        raise

def main():
    assert os.environ.get('ENTITY_CLOUD_VALIDATED')=='1'
    assert not subprocess.check_output(['swapon','--noheadings','--show']).strip()
    config=json.loads((c.HERE/'scale-plan.json').read_text())
    rows=[]
    for count in config['namespace_sizes']:
        state=prepare(count,config['frame_cap'])
        # The 1M capacity pilot is preserved and excluded from speed claims.
        for rep in ([0,1,2,3] if count==1000000 else [1,2,3]):
            variants=[('brewfs','baseline'),('brewfs','candidate'),('juicefs','stock')]
            if rep%2==0:variants.reverse()
            for profile in (['half-random'] if rep==0 else ['full-scan','half-random','workspaces']):
                for client,revision in variants:
                    label=f'scale-{count}-{profile}-{client}-{revision}-r{rep}'
                    c.dump(c.ART/'scale-phase.json',dict(label=label,completed=len(rows)))
                    result=workspace_case(label,client,revision,state) if profile=='workspaces' else read_case(label,client,revision,profile,state)
                    result.update(files=count,profile=profile,client=client,revision=revision,repetition=rep,
                        frame_cap=state['frame_cap'],packed_onboarding_s=state['packed_onboarding_s'],
                        juicefs_onboarding_s=state['juicefs_onboarding_s'],onboarding_repetitions=1)
                    rows.append(result)
                    c.dump(c.ART/(label+'-result.json'),result)
                    c.dump(c.ART/'scale-rows.json',rows)
                    print(f'{len(rows)}/{config["expected_cells"]} {label}: lifecycle={result["lifecycle_s"]:.3f}s',flush=True)
                    if len(rows)%3==0:
                        # Only transfer between fully stopped cells. The normal
                        # campaign uses four-digit IDs; scale IDs are distinct.
                        token=f'scale-{len(rows):04d}'
                        checkpoint=c.ROOT/f'checkpoint-{token}.tar.gz'
                        subprocess.run(['tar','-C',str(c.ROOT),'-czf',str(checkpoint),
                            'artifacts','campaign.log','scale.log'],check=True)
                        (c.ROOT/'checkpoint-ready.txt').write_text(token+'\n')
                        deadline=time.monotonic()+600
                        while not (c.ROOT/f'checkpoint-{token}-collected').exists():
                            if time.monotonic()>=deadline:
                                raise TimeoutError('scale checkpoint collection was not acknowledged')
                            time.sleep(1)
                        # The downloaded hash has already been acknowledged.
                        checkpoint.unlink()
        assert len(rows)>0
    assert len(rows)==config['expected_cells']
    c.dump(c.ART/'scale-completed.json',dict(cells=len(rows),all_remote_ready=True))

if __name__=='__main__':main()
