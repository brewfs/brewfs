"""Authorized continuation of six never-started cells; preparation is never called."""
from pathlib import Path
import hashlib,json,os,subprocess,sys,time,traceback
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'harness'))
from common import ART,atomic,read,stamp
from control import stage,checkpoint,request,EVENTS

def main():
    assert ROOT==Path('/opt/entity-packed-million-r2')
    authorization=read(ROOT/'continuation-authorization.json');assert authorization['answer']
    assert not (ART/'continuation-started.json').exists(),'Never replay a partial continuation'
    assert not (ART/'campaign-state.json').exists() and not (ART/'rows.json').exists()
    assert not list(ART.glob('million-*')),'An original workspace cell already exists'
    first=read(ART/'runner-result.json');assert first['status']=='failed-preserved'
    assert 'checkpoint' in first['error'] and 'RemoteDisconnected' in first['error']
    assert not subprocess.check_output(['swapon','--noheadings','--show']).strip()
    for line in Path('/proc/self/mountinfo').read_text().splitlines():
        if ' - fuse' in line:assert str(ROOT/'work') not in line
    for path in (ART/'components').glob('*/state.json'):
        value=read(path);assert value['complete'] and value['returncode']==0
    for row in read(ROOT/'frozen-files.json'):
        with (ROOT/row['path']).open('rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()==row['sha256'],row['path']
    recovery=ART/'continuation';recovery.mkdir()
    for name in ['runner-result.json','failure.json','campaign-started.json']:
        if (ART/name).exists():atomic(recovery/('original-'+name),read(ART/name),durable=True)
    source=ART/'onboarding/result.json';state=read(source);assert state['files']==1000000
    started=read(ART/'campaign-started.json');now=stamp()
    atomic(ART/'continuation-started.json',dict(authorization=authorization,original_start=started,
        previous_failure=first,started=now,onboarding_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
        preparation_replayed=False,workspace_cells_previously_started=0,
        interruption_s=(now['utc_ns']-first['utc_ns'])/1e9,
        full_campaign_accounting_includes_interruption=True),durable=True)
    os.environ['ENTITY_CLOUD_VALIDATED']='1'
    import driver
    contract=driver.CONTRACT
    assert (contract['namespace_files'],contract['repetitions'],contract['workspaces_per_batch'],contract['expected_cells'])==(1000000,3,4,6)
    from node_bootstrap import unit
    unit('brewfs-million-r2-progress',['/usr/bin/python3',str(ROOT/'harness/progress.py'),'--interval','5'],environment=True)
    atomic(ART/'runner-result.json',dict(status='running',continuation=True,**stamp()),durable=True)
    rows=[]
    with stage('campaign-continuation',files=state['files'],original_start=started):
        checkpoint('continuation-ready',driver.c.CFG)
        # Same arm order, labels, repetitions, functions and result schema as
        # the frozen driver.main suffix. Do not invoke driver.main/prepare.
        for repetition in range(1,4):
            arms=['brewfs','juicefs'] if repetition%2 else ['juicefs','brewfs']
            for client in arms:
                label=f'million-{client}-r{repetition}'
                atomic(ART/'campaign-state.json',dict(completed_cells=len(rows),expected_cells=6,
                    current_label=label,repetition=repetition,client=client,**stamp()))
                result=driver.workspace_case(label,client,state)
                result.update(repetition=repetition,files=state['files'],profile='workspace-fanout',
                    packed_onboarding_s=state['packed_onboarding_s'],juicefs_onboarding_s=state['juicefs_onboarding_s'])
                rows.append(result);atomic(ART/(label+'-result.json'),result,durable=True)
                atomic(ART/'rows.json',rows,durable=True);checkpoint(label,driver.c.CFG)
        assert len(rows)==6
        atomic(ART/'completed.json',dict(cells=6,all_remote_ready=True,started=started,ended=stamp()),durable=True)
        atomic(ART/'campaign-state.json',dict(completed_cells=6,expected_cells=6,status='complete',**stamp()),durable=True)
    atomic(ART/'runner-result.json',dict(status='complete',continuation=True,**stamp()),durable=True)

if __name__=='__main__':
    try:main()
    except BaseException:
        atomic(ART/'runner-result.json',dict(status='failed-preserved',continuation=True,
            error=traceback.format_exc(),resources_preserved=True,**stamp()),durable=True)
        raise
    finally:EVENTS.close()
