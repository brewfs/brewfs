"""Named-file disk characterization, separate from filesystem comparison."""
from pathlib import Path
import json,os,subprocess,time

ROOT=Path(__file__).resolve().parent
ART=ROOT/'artifacts/preflight'
def main():
    assert os.path.ismount('/data') and os.stat('/data').st_dev!=os.stat('/').st_dev
    cfg=json.loads((ROOT/'private/role.json').read_text());owner=json.loads(Path('/data/.brewfs-million-r2-owner.json').read_text())
    assert owner['disk_id']==cfg['data_disk_id'] and owner['run_token']==cfg['run_token']
    directory=Path('/data')/cfg['run_token']/'preflight';directory.mkdir(parents=True,exist_ok=False)
    ART.mkdir(parents=True,exist_ok=True);filename=directory/'fio.dat'
    assert filename.parent.resolve()==directory.resolve() and not filename.exists()
    cases=[('fill','write','1m',8,'libaio',False),
        ('fsync-4k','randwrite','4k',1,'sync',True),
        ('randread-4k','randread','4k',32,'libaio',True),
        ('randwrite-4k','randwrite','4k',32,'libaio',True),
        ('seqread-1m','read','1m',8,'libaio',True)]
    for name,rw,bs,depth,engine,timed in cases:
        command=['fio','--name='+name,'--filename='+str(filename),'--size=4g','--rw='+rw,
            '--bs='+bs,'--iodepth='+str(depth),'--ioengine='+engine,'--direct=1','--numjobs=1',
            '--group_reporting=1','--output-format=json','--output='+str(ART/(name+'.json')),
            '--randseed=9173']
        if name=='fsync-4k':command+=['--fsync=1']
        if timed:command+=['--time_based=1','--runtime=30']
        start=time.time_ns();(ART/(name+'-command.json')).write_text(json.dumps(command)+'\n')
        with open(ART/(name+'.log'),'w') as log:subprocess.run(command,stdout=log,stderr=subprocess.STDOUT,check=True)
        result=json.loads((ART/(name+'.json')).read_text());assert all(job['error']==0 for job in result['jobs'])
        (ART/(name+'-timeline.json')).write_text(json.dumps(dict(start_utc_ns=start,end_utc_ns=time.time_ns()))+'\n')
    # Remove only the explicitly owned scratch file; retain every measurement.
    assert filename.resolve().parent==directory.resolve();filename.unlink()
    os.sync();(ART/'complete.json').write_text(json.dumps(dict(cases=5,comparison_data=False,utc_ns=time.time_ns()))+'\n')
if __name__=='__main__':main()
