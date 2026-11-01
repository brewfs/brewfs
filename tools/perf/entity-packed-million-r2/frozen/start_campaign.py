"""Start the once-only frozen campaign after all three dedicated nodes are ready."""
from pathlib import Path
import hashlib,json,os,sys,time,traceback,urllib.request

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'harness'))
from common import atomic,read,stamp
from control import request,EVENTS

def main():
    assert ROOT==Path('/opt/entity-packed-million-r2')
    cfg=read(ROOT/'private/role.json');assert cfg['role']=='client'
    contract=read(ROOT/'experiment-contract.json')
    assert contract['namespace_files']==1000000 and contract['expected_cells']==6
    assert contract['workspaces_per_batch']==4 and contract['upload_mode']=='upload_before_commit'
    assert os.path.ismount(ROOT/'work') and os.stat(ROOT/'work').st_dev==os.stat('/data').st_dev
    assert not (ROOT/'artifacts/campaign-started.json').exists(),'Never rerun a partial campaign'
    for row in read(ROOT/'frozen-files.json'):
        with (ROOT/row['path']).open('rb') as source:actual=hashlib.file_digest(source,'sha256').hexdigest()
        assert actual==row['sha256'],'Frozen source changed: '+row['path']
    settings=read(ROOT/'harness/settings.json')
    for role,url in settings['agents'].items():
        status=request(url+'/status')
        assert time.time_ns()-status['updated']['utc_ns']<20e9,'Stale agent: '+role
        assert status['role']==role
        if role=='metadata':assert all(status['scrapes'][name]['ok'] for name in ['tikv','pd'])
    import cell
    cell.S3.create_bucket(Bucket=settings['bucket'])
    assert cell.S3.list_objects_v2(Bucket=settings['bucket'],MaxKeys=1)['KeyCount']==0
    from node_bootstrap import unit
    unit('brewfs-million-r2-progress',['/usr/bin/python3',str(ROOT/'harness/progress.py'),'--interval','5'],environment=True)
    os.environ['ENTITY_CLOUD_VALIDATED']='1'
    import driver
    driver.main()
    atomic(ROOT/'artifacts/runner-result.json',dict(status='complete',**stamp()),durable=True)

if __name__=='__main__':
    try:main()
    except BaseException:
        atomic(ROOT/'artifacts/runner-result.json',dict(status='failed-preserved',
            error=traceback.format_exc(),resources_preserved=True,**stamp()),durable=True)
        raise
    finally:EVENTS.close()
