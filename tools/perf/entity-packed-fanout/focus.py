"""One design comparison: immutable-base fanout into four writable workspaces."""
import json,os,subprocess,time
import campaign as c
import scale_driver as sd

def main():
    assert os.environ.get('ENTITY_CLOUD_VALIDATED')=='1'
    assert not subprocess.check_output(['swapon','--noheadings','--show']).strip()
    contract=json.loads((c.HERE/'experiment-contract.json').read_text())
    assert contract['expected_cells_including_pilots']==14
    rows=[]
    for count in contract['namespace_sizes']:
        c.dump(c.ART/'focus-phase.json',dict(stage='onboarding',files=count,completed=len(rows)))
        state=sd.prepare(count,contract['packed_frame_cap'])
        for rep in ([0,1,2,3] if count==1000000 else [1,2,3]):
            arms=[('brewfs','baseline'),('juicefs','stock')]
            if rep%2==0:arms.reverse()
            for client,revision in arms:
                label=f'focus-{count}-{client}-{revision}-r{rep}'
                c.dump(c.ART/'focus-phase.json',dict(stage='workspaces',label=label,files=count,completed=len(rows)))
                result=sd.workspace_case(label,client,revision,state)
                result.update(files=count,profile='workspace-fanout',client=client,revision=revision,repetition=rep,
                    frame_cap=contract['packed_frame_cap'],mode=contract['upload_mode'],
                    packed_onboarding_s=state['packed_onboarding_s'],
                    juicefs_onboarding_s=state['juicefs_onboarding_s'],onboarding_repetitions=1)
                rows.append(result)
                c.dump(c.ART/(label+'-result.json'),result)
                c.dump(c.ART/'focus-rows.json',rows)
                print(f'{len(rows)}/14 {label}: complete_workspaces_s={result["lifecycle_s"]:.3f}',flush=True)
            token=f'focus-{len(rows):04d}'
            path=c.ROOT/f'checkpoint-{token}.tar.gz'
            subprocess.run(['tar','-C',str(c.ROOT),'-czf',str(path),'artifacts','focus.log'],check=True)
            with path.open('rb') as durable:os.fsync(durable.fileno())
            (c.ROOT/'checkpoint-ready.txt').write_text(token+'\n')
            deadline=time.monotonic()+600
            while not (c.ROOT/f'checkpoint-{token}-collected').exists():
                if time.monotonic()>=deadline:raise TimeoutError('Focused checkpoint was not collected')
                time.sleep(1)
            path.unlink()
    assert len(rows)==14
    c.dump(c.ART/'focus-completed.json',dict(cells=14,all_remote_ready=True))

if __name__=='__main__':main()
