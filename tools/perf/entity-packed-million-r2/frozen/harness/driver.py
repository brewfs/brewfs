"""One million files, six paired workspace batches; no phase kill deadline."""
from pathlib import Path
import json,os,subprocess,time,uuid
from common import ROOT,ART,atomic,read,stamp
from control import stage,target,checkpoint,request
from managed import ManagedProcess,run
from progress import Progress
import cell as c
import scale_workloads as sw

CONTRACT=read(ROOT/'experiment-contract.json')

def prepare(count):
    source=c.WORK/f'source-{count}';art=ART/'onboarding';art.mkdir()
    with stage('source-generation',files=count) as generation:
        descriptor=sw.create_source(source,count);os.sync()
    manifest=art/'manifest.txt'
    command=[str(ROOT/'bin/packed_v3_snapshot_fixture'),'--source-directory',str(source),
        '--output-dir',str(c.WORK/'producer'),'--source-consistency','best-effort-detected',
        '--source-hardlink-policy','visible-links','--bucket',c.CFG['bucket'],
        '--endpoint',c.CFG['endpoint'],'--region','us-east-1','--force-path-style','true',
        '--prefix','million-r2-'+uuid.uuid4().hex,'--manifest-output',str(manifest),
        '--metadata-codec','raw','--data-codec','raw','--inline-data','off',
        '--chunk-size','4194304','--block-size','4194304','--frame-policy','size-only',
        '--access-profile','random-small-file','--max-random-frame-raw-bytes','262144']
    atomic(art/'producer-command.json',command)
    with stage('packed-onboarding',files=count) as packed:
        with open(art/'producer.log','w') as log:run(command,stdout=log,check=True)
    with stage('juicefs-onboarding',files=count) as imported:
        mount=c.Cell('import','juicefs')
        env=dict(mount.env,scalebase=mount.meta)
        command=[c.jfs.BINARY,'sync','--threads=8','--dirs','--perms','--inplace',
            '--metrics=127.0.0.1:8911',str(source)+'/', 'jfs://scalebase/base/']
        atomic(art/'juicefs-native-sync-command.json',command)
        target('juicefs-sync','http://127.0.0.1:8911/metrics','starting')
        with stage('juicefs-native-import',files=count) as native:
            with open(art/'juicefs-native-sync.log','w') as log:
                process=ManagedProcess(command,env=env,stdout=log)
                # The native sync exporter is short-lived; preserve actual
                # startup gaps, and scrape immediately as well as periodically.
                while process.poll() is None:
                    status=request(c.CFG['agents']['client']+'/scrape',{})
                    metric=status['scrapes'].get('juicefs-sync',{})
                    if metric.get('ok'):target('juicefs-sync','http://127.0.0.1:8911/metrics','active')
                    time.sleep(5)
                assert process.wait()==0,'Native import failed; components and data preserved'
            target('juicefs-sync','http://127.0.0.1:8911/metrics','stopped')
        mount.stop();mount.finish_observer()
    state=dict(files=count,manifest=manifest.read_text().strip(),juicefs_meta=mount.meta,
        source_generation_common_s=generation['elapsed_s'],packed_onboarding_s=packed['elapsed_s'],
        juicefs_onboarding_s=imported['elapsed_s'],juicefs_native_import_s=native['elapsed_s'],
        descriptor=descriptor,onboarding_repetitions=1)
    atomic(art/'result.json',state,durable=True);return state

def verify_names(root,count,label):
    progress=Progress(label,sw.DIRECTORIES,'directories');found=0
    with os.scandir(root/'small') as scan:directories=sorted(entry.name for entry in scan)
    assert directories==[f'd{n:04d}' for n in range(sw.DIRECTORIES)]
    for directory in range(sw.DIRECTORIES):
        with os.scandir(root/'small'/f'd{directory:04d}') as scan:names=sorted(entry.name for entry in scan)
        assert names==[f'f{index:07d}' for index in range(directory,count,sw.DIRECTORIES)]
        found+=len(names);progress.advance()
    assert found==count;return found

def workspace_case(label,client,state):
    c.CFG['manifest']=state['manifest'];art=ART/label;art.mkdir()
    offset=request(c.CFG['agents']['objects']+'/objects')
    cells=[];closed=[];details=[];roots=[];progress=Progress(label+'-workspaces',4,'workspaces')
    begin=time.perf_counter();shared=None
    with stage(label+'-workspace-lifecycle',files=state['files'],client=client) as whole:
        if client=='juicefs':
            shared=c.Cell(label+'-shared',client,state['juicefs_meta'],writable=True);cells.append(shared)
            parent=shared.mount/('workspaces-'+uuid.uuid4().hex);parent.mkdir()
        for workspace in range(4):
            clone_s=0
            if client=='brewfs':
                cell=c.Cell(label+f'-w{workspace}',client);cells.append(cell);root=cell.mount
            else:
                cell=shared;root=parent/f'w{workspace}'
                command=[c.jfs.BINARY,'clone','-p',str(shared.mount/'base'),str(root)]
                with stage(label+f'-w{workspace}-clone',workspace=workspace) as clone:
                    with open(art/f'clone-{workspace}.log','w') as log:run(command,env=shared.env,stdout=log,check=True)
                clone_s=clone['elapsed_s'];roots.append(root.relative_to(shared.mount))
            with stage(label+f'-w{workspace}-half-random-read',workspace=workspace):
                result_read=sw.read_trace(root,state['files'],art,label+f'-w{workspace}')
            with stage(label+f'-w{workspace}-cow',workspace=workspace):
                result_cow=sw.cow(root,state['files'],workspace,art,label+f'-w{workspace}')
            detail=dict(workspace=workspace,clone_s=clone_s,read=result_read,cow=result_cow,
                bootstrap_s=cell.bootstrap_s if client=='brewfs' else 0)
            if client=='brewfs':detail.update(cell.stop());closed.append(cell)
            details.append(detail);atomic(art/'partial-workspaces.json',details);progress.advance()
        teardown={}
        if shared:teardown=shared.stop();closed.append(shared)
        ended=time.perf_counter()
    end_objects=request(c.CFG['agents']['objects']+'/objects')
    result=dict(client=client,workspaces=4,details=details,shared_teardown=teardown,
        lifecycle_s=ended-begin,observed_phase_s=whole['elapsed_s'],remote_ready=True,
        object_counters_before=offset,object_counters_after=end_objects)
    atomic(art/'timed-result.json',result,durable=True)
    with stage(label+'-correctness-verification') as correctness:
        for cell in closed:cell.verify_upper();cell.finish_observer()
        verify=c.Cell(label+'-remote-verify',client,state['juicefs_meta'] if client=='juicefs' else None)
        base=verify.mount/'base' if client=='juicefs' else verify.mount
        result['source_names_verified']=verify_names(base,state['files'],label+'-verify-base')
        sw.verify_source_sample(base,state['files'])
        if client=='juicefs':
            for workspace,relative in enumerate(roots):
                root=verify.mount/relative
                verify_names(root,state['files'],label+f'-verify-w{workspace}')
                for index in sw.working_set(state['files'],100):
                    expected=sw.wl.payload(index+(workspace+1)*10000000,sw.FILE_BYTES)
                    sw.wl.direct_read(sw.path(root,index),0,sw.FILE_BYTES,expected)
        verify.stop();verify.finish_observer()
    result['correctness_verification_s']=correctness['elapsed_s'];result['remote_logical_verification']=True
    return result

def main():
    assert os.environ.get('ENTITY_CLOUD_VALIDATED')=='1'
    assert not subprocess.check_output(['swapon','--noheadings','--show']).strip()
    assert not (ART/'campaign-started.json').exists(),'Never silently restart a partial experiment'
    started=stamp();atomic(ART/'campaign-started.json',started,durable=True)
    with stage('campaign',files=CONTRACT['namespace_files']):
        state=prepare(CONTRACT['namespace_files']);rows=[]
        checkpoint('onboarding',c.CFG)
        for repetition in range(1,4):
            arms=['brewfs','juicefs'] if repetition%2 else ['juicefs','brewfs']
            for client in arms:
                label=f'million-{client}-r{repetition}'
                atomic(ART/'campaign-state.json',dict(completed_cells=len(rows),expected_cells=6,
                    current_label=label,repetition=repetition,client=client,**stamp()))
                result=workspace_case(label,client,state)
                result.update(repetition=repetition,files=state['files'],profile='workspace-fanout',
                    packed_onboarding_s=state['packed_onboarding_s'],juicefs_onboarding_s=state['juicefs_onboarding_s'])
                rows.append(result);atomic(ART/(label+'-result.json'),result,durable=True)
                atomic(ART/'rows.json',rows,durable=True);checkpoint(label,c.CFG)
        assert len(rows)==6
        atomic(ART/'completed.json',dict(cells=6,all_remote_ready=True,started=started,ended=stamp()),durable=True)
        atomic(ART/'campaign-state.json',dict(completed_cells=6,expected_cells=6,status='complete',**stamp()),durable=True)

if __name__=='__main__':
    from control import EVENTS
    try:main()
    finally:EVENTS.close()
