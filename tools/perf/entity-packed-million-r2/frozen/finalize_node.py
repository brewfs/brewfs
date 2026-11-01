"""Close observation only after completed work, then archive every final segment."""
from pathlib import Path
import json,os,subprocess,sys,time

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'harness'))
from common import atomic,read,stamp
from control import request,EVENTS
import archive

def main():
    assert ROOT==Path('/opt/entity-packed-million-r2') and os.getuid()==0
    cfg=read(ROOT/'private/role.json');role=cfg['role']
    if role=='client':
        assert read(ROOT/'artifacts/runner-result.json')['status']=='complete'
        assert read(ROOT/'artifacts/completed.json')['cells']==6
        rows=read(ROOT/'artifacts/rows.json');assert len(rows)==6 and all(r['remote_ready'] for r in rows)
        for path in (ROOT/'artifacts/components').glob('*/state.json'):assert read(path)['complete'],str(path)
        for line in Path('/proc/self/mountinfo').read_text().splitlines():
            if ' - fuse' in line:assert str(ROOT/'work') not in line,'A workload mount is still active'
        subprocess.run(['systemctl','stop','brewfs-million-r2-progress'],check=True)
    else:
        args=['docker','compose','-p','million-r2-'+role,'-f',str(ROOT/'private/compose.json'),'logs','--no-color']
        result=subprocess.run(args,check=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        # Credentials are synthetic but are still excluded from exported logs.
        raw=result.stdout
        for name in ['AWS_ACCESS_KEY_ID','AWS_SECRET_ACCESS_KEY']:
            secret=read(ROOT/'private/environment.json').get(name,'')
            if secret:raw=raw.replace(secret.encode(),b'[REDACTED TEST CREDENTIAL]')
        (ROOT/'artifacts/service-final.log').write_bytes(raw)
    request('http://'+cfg['private_ip']+':9910/scrape',{})
    subprocess.run(['systemctl','stop','brewfs-million-r2-agent'],check=True)
    EVENTS.close()
    assert not list((ROOT/'artifacts').rglob('*.open')),'Unclosed journal; preserve and diagnose'
    atomic(ROOT/'artifacts/finalization.json',dict(role=role,observation_closed=True,**stamp()),durable=True)
    result=archive.create('final')
    validation=archive.verify(result['path']);assert not validation['deferred']
    atomic(ROOT/'final-archive.json',dict(archive=result,validation=validation),durable=True)
    print(json.dumps(read(ROOT/'final-archive.json')))

if __name__=='__main__':main()
