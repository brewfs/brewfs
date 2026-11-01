"""Join the existing final clone, then run only its never-started operations.

The interrupted cell is explicitly disqualified as an uninterrupted performance
sample. Original frozen workload functions and all previous evidence remain.
"""
from pathlib import Path
import gzip,hashlib,json,os,sys,time,traceback

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'harness'))
from common import ART,atomic,read,stamp
from control import EVENTS,checkpoint,request,stage

LABEL='million-juicefs-r3'
MOUNT_ID='3cbe03bddc3d4f4c84b6011730732a6b'
CLONE_ID='10c52a6045dc4bf69eaeb93420205683'

def attach(component_id):
    from managed import ManagedProcess
    directory=ART/'components'/component_id
    identity=read(directory/'identity.json')
    private=read(ROOT/'private'/(component_id+'.json'))
    state=read(directory/'state.json')
    assert identity['unit']==f'million-r2-{component_id}.service'
    assert state['cgroup']=='/system.slice/'+identity['unit']
    if not state['complete']:
        assert state['supervisor_pid']>0 and Path('/proc',str(state['supervisor_pid'])).exists()
        for item in state['live']:
            from common import proc_start
            assert proc_start(item['pid'])==item['start_ticks']
    else:
        assert state['returncode']==0 and not state['live']
    proc=ManagedProcess.__new__(ManagedProcess)
    proc.id=component_id;proc.unit=identity['unit'];proc.directory=directory
    proc.state=directory/'state.json';proc.stop_request=Path(private['stop_request'])
    proc.args=identity['command']
    assert not proc.stop_request.exists()
    return proc,private,identity

def original_lifecycle_start():
    found=[];interrupted=[]
    for path in sorted((ART/'timeline').glob('phases-*.jsonl.gz*')):
        try:
            with gzip.open(path,'rt') as f:
                for line in f:
                    value=json.loads(line)
                    if value['name']==LABEL+'-workspace-lifecycle' and value['status']=='running':
                        found.append(value['start'])
        except EOFError:
            assert path.name.endswith('.open')
            interrupted.append(path)
    assert len(found)==1
    return found[0],interrupted

def plan():
    assert ROOT==Path('/opt/entity-packed-million-r2')
    assert read(ROOT/'continuation-authorization.json')['answer']
    rows=read(ART/'rows.json');assert len(rows)==5
    assert [(v['client'],v['repetition']) for v in rows]==[
        ('brewfs',1),('juicefs',1),('juicefs',2),('brewfs',2),('brewfs',3)]
    assert all(v['remote_ready'] and v['remote_logical_verification'] for v in rows)
    failure=read(ART/'runner-result.json')
    assert failure['status']=='failed-preserved' and 'Never replay a partial continuation' in failure['error']
    assert not Path('/proc/54550').exists(),'The original controller must be stopped'
    for row in read(ROOT/'frozen-files.json'):
        with (ROOT/row['path']).open('rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()==row['sha256']
    details=read(ART/LABEL/'partial-workspaces.json')
    assert [d['workspace'] for d in details]==[0,1,2]
    assert not (ART/(LABEL+'-result.json')).exists()
    assert not (ART/LABEL/'timed-result.json').exists()
    assert not list((ART/LABEL).glob(LABEL+'-w3-*')),'Never repeat any final-workspace workload'
    assert not list((ART/'progress').glob(LABEL+'-w3-*'))
    mount,mount_private,mount_identity=attach(MOUNT_ID)
    clone,clone_private,clone_identity=attach(CLONE_ID)
    assert mount.poll() is None
    command=clone_identity['command']
    assert command[:3]==[str(ROOT/'bin/juicefs'),'clone','-p']
    mountpoint=ROOT/'work'/(LABEL+'-shared')/'mnt'
    assert command[3]==str(mountpoint/'base')
    destination=Path(command[4]);assert destination.name=='w3'
    assert destination.parent.parent==mountpoint and destination.parent.name.startswith('workspaces-')
    assert (mountpoint/'.config').exists() or (mountpoint/'.jfs.config').exists()
    start,interrupted=original_lifecycle_start()
    return rows,details,failure,mount,mount_private,clone,destination,start,interrupted

def main():
    rows,details,failure,mount,mount_private,clone,destination,start,interrupted=plan()
    recovery=ART/'last-workspace-recovery'
    recovery.mkdir(exist_ok=False)
    (recovery/'driver-source.py').write_bytes(Path(__file__).read_bytes())
    atomic(recovery/'original-runner-failure.json',failure,durable=True)
    atomic(recovery/'original-partial-workspaces.json',details,durable=True)
    atomic(recovery/'start.json',dict(started=stamp(),original_lifecycle_start=start,
        attached_mount_component=MOUNT_ID,attached_clone_component=CLONE_ID,
        never_replayed_workspaces=[0,1,2],last_workspace_operations_previously_started=False,
        uninterrupted_performance_sample_valid=False),durable=True)
    for path in interrupted:
        identity=(path.stat().st_dev,path.stat().st_ino)
        for process in Path('/proc').iterdir():
            if not process.name.isdigit():continue
            try:
                for fd in (process/'fd').iterdir():
                    try:
                        s=fd.stat();assert (s.st_dev,s.st_ino)!=identity,'Interrupted journal still has a live writer'
                    except FileNotFoundError:pass
            except FileNotFoundError:pass
        destination_log=path.with_name(path.name[:-5]+'.interrupted')
        assert not destination_log.exists();os.replace(path,destination_log)
        atomic(recovery/(path.name+'.identity.json'),dict(original=str(path),retained=str(destination_log),
            sha256=hashlib.sha256(destination_log.read_bytes()).hexdigest(),truncated_original_retained=True),durable=True)
    import driver
    c=driver.c;sw=driver.sw
    saved=read(ART/'onboarding/result.json');assert saved['files']==1000000
    c.CFG['manifest']=saved['manifest']
    shared=c.Cell.__new__(c.Cell)
    shared.label=LABEL+'-shared';shared.client='juicefs';shared.art=ART/shared.label
    shared.work=c.WORK/shared.label;shared.mount=shared.work/'mnt';shared.cache=shared.work/'cache'
    shared.logpath=shared.art/'mount.log';shared.log=shared.logpath.open('a')
    shared.proc=mount;shared.probe=None;shared.env=mount_private['env']
    shared.meta=saved['juicefs_meta'];shared.metrics_name='juicefs-mount'
    shared.metrics_url='http://127.0.0.1:8910/metrics'
    atomic(ART/'runner-result.json',dict(status='running',partial_cell_recovery=True,
        original_failure_preserved=True,uninterrupted_performance_sample_valid=False,**stamp()),durable=True)
    with stage(LABEL+'-recovery-join-existing-clone',existing_component=CLONE_ID):
        assert clone.wait()==0,'Existing clone failed; preserve the workspace and process evidence'
    atomic(recovery/'clone-completed.json',read(clone.state),durable=True)
    original_clone_start=read(clone.state)['started']
    # This duration deliberately includes controller failure/recovery waiting.
    clone_s=(stamp()['monotonic_ns']-original_clone_start['monotonic_ns'])/1e9
    with stage(LABEL+'-w3-half-random-read',workspace=3,recovered_after_controller_stop=True):
        result_read=sw.read_trace(destination,saved['files'],ART/LABEL,LABEL+'-w3')
    with stage(LABEL+'-w3-cow',workspace=3,recovered_after_controller_stop=True):
        result_cow=sw.cow(destination,saved['files'],3,ART/LABEL,LABEL+'-w3')
    details.append(dict(workspace=3,clone_s=clone_s,read=result_read,cow=result_cow,bootstrap_s=0,
        clone_duration_includes_recovery_wait=True))
    atomic(ART/LABEL/'partial-workspaces.json',details,durable=True)
    progress=read(ART/'progress'/(LABEL+'-workspaces.json'));now=stamp()
    progress.update(done=4,failed=0,updated=now,percent=100.0,
        elapsed_s=(now['monotonic_ns']-progress['started']['monotonic_ns'])/1e9,
        recovered_interrupted_cell=True)
    progress['rate_per_s']=4/progress['elapsed_s']
    atomic(ART/'progress'/(LABEL+'-workspaces.json'),progress,durable=True)
    teardown=shared.stop();shared.finish_observer();ended=stamp()
    result=dict(client='juicefs',workspaces=4,details=details,shared_teardown=teardown,
        lifecycle_s=(ended['monotonic_ns']-start['monotonic_ns'])/1e9,
        observed_phase_s=(ended['monotonic_ns']-start['monotonic_ns'])/1e9,remote_ready=True,
        object_counters_before=None,object_counters_after=request(c.CFG['agents']['objects']+'/objects'),
        recovered_interrupted_cell=True,uninterrupted_performance_sample_valid=False,
        timing_source='Original durable lifecycle stage start to actual exit; includes external stop and recovery; original private perf_counter origin unavailable',
        original_object_counter_offset_unavailable=True)
    atomic(ART/LABEL/'timed-result.json',result,durable=True)
    with stage(LABEL+'-correctness-verification',recovered_interrupted_cell=True) as correctness:
        verify=c.Cell(LABEL+'-remote-verify','juicefs',saved['juicefs_meta'])
        base=verify.mount/'base'
        result['source_names_verified']=driver.verify_names(base,saved['files'],LABEL+'-verify-base')
        sw.verify_source_sample(base,saved['files'])
        for workspace in range(4):
            root=verify.mount/destination.parent.relative_to(shared.mount)/f'w{workspace}'
            driver.verify_names(root,saved['files'],LABEL+f'-verify-w{workspace}')
            for index in sw.working_set(saved['files'],100):
                expected=sw.wl.payload(index+(workspace+1)*10000000,sw.FILE_BYTES)
                sw.wl.direct_read(sw.path(root,index),0,sw.FILE_BYTES,expected)
        verify.stop();verify.finish_observer()
    result.update(correctness_verification_s=correctness['elapsed_s'],remote_logical_verification=True,
        repetition=3,files=saved['files'],profile='workspace-fanout',
        packed_onboarding_s=saved['packed_onboarding_s'],juicefs_onboarding_s=saved['juicefs_onboarding_s'])
    rows.append(result);atomic(ART/(LABEL+'-result.json'),result,durable=True)
    atomic(ART/'rows.json',rows,durable=True);checkpoint(LABEL,c.CFG)
    atomic(ART/'completed.json',dict(cells=6,all_remote_ready=True,
        started=read(ART/'campaign-started.json'),ended=stamp(),
        recovered_interrupted_cell=LABEL,full_observability_acceptance=False,
        uninterrupted_six_cell_performance_acceptance=False),durable=True)
    atomic(ART/'campaign-state.json',dict(completed_cells=6,expected_cells=6,status='complete-with-failures',**stamp()),durable=True)
    atomic(ART/'runner-result.json',dict(status='complete',partial_cell_recovery=True,
        full_observability_acceptance=False,uninterrupted_six_cell_performance_acceptance=False,**stamp()),durable=True)

if __name__=='__main__':
    if sys.argv[1:]==['--inspect']:
        values=plan()
        print(json.dumps(dict(safe_to_attach=True,complete_cells=len(values[0]),partial_workspaces=len(values[1]),
            mount_component=MOUNT_ID,clone_component=CLONE_ID,uninterrupted_performance_sample_valid=False)))
    else:
        try:main()
        except BaseException:
            atomic(ART/'runner-result.json',dict(status='failed-preserved',partial_cell_recovery=True,
                error=traceback.format_exc(),resources_preserved=True,**stamp()),durable=True)
            raise
        finally:EVENTS.close()
