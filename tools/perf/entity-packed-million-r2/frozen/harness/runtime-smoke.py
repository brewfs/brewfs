"""Actual local service/import/clone/FUSE correctness and observation gate."""
from pathlib import Path
import json,os,signal,subprocess,sys,time,urllib.request
from common import ROOT,ART,atomic,read
from managed import ManagedProcess
from control import request,checkpoint,EVENTS

cfg=read(ROOT/'private/smoke.json');os.environ.update(read(ROOT/'private/environment.json'))
components=[]
def start(command,name):
    log=open(ART/(name+'.log'),'w');process=ManagedProcess(command,stdout=log)
    components.append((name,process,log));return process
def wait_url(url,process=None):
    while True:
        if process is not None and process.poll() is not None:
            raise RuntimeError('Required bootstrap process exited before '+url)
        try:
            with urllib.request.urlopen(url,timeout=3) as response:
                if response.status==200:return response.read()
        except OSError:pass
        time.sleep(.25)

pd=f'http://127.0.0.1:{cfg["pd"]}'
wait_url(pd+'/pd/api/v1/config')
while True:
    try:
        stores=request(pd+'/pd/api/v1/stores').get('stores',[])
        if len(stores)==1 and stores[0]['store']['state_name']=='Up':
            request(pd+'/pd/api/v1/config',{'max-replicas':1});break
    except OSError as e:atomic(ART/'bootstrap-retry.json',dict(error=repr(e)))
    time.sleep(.25)
atomic(ART/'pd-config.json',request(pd+'/pd/api/v1/config'))
atomic(ART/'tikv-stores.json',request(pd+'/pd/api/v1/stores'))
wait_url(f'http://127.0.0.1:{cfg["objects"]}/health')
objects=Path(cfg['roles']['objects'])
nginx_process=start([cfg['nginx'],'-p',str(objects/'work'),'-c',str(objects/'work/nginx.conf'),'-g','daemon off;'],'nginx')
for role,base in cfg['roles'].items():
    base=Path(base)
    if role=='metadata':
        atomic(base/'metrics-targets.json',[
            dict(name='pd',state='active',url=pd+'/metrics'),
            dict(name='tikv',state='active',url=f'http://127.0.0.1:{cfg["tikv_status"]}/metrics')])
    agent_process=start(['/usr/bin/python3',str(base/'harness/node_agent.py'),'--role',role,
        '--bind','127.0.0.1','--port',str(cfg['agents'][role]),'--interval','0.2'],'agent-'+role)
    wait_url(f'http://127.0.0.1:{cfg["agents"][role]}/health',agent_process)
import cell as c
import driver
wait_url(c.CFG['endpoint']+'/health',nginx_process)
c.S3.create_bucket(Bucket=c.CFG['bucket'])
start(['/usr/bin/python3',str(ROOT/'harness/progress.py'),'--interval','1'],'progress')
atomic(ART/'campaign-state.json',dict(status='local-correctness-only',expected_cells=2,completed_cells=0))
state=driver.prepare(4096)
rows=[]
for client in ['brewfs','juicefs']:
    rows.append(driver.workspace_case('smoke-'+client,client,state))
    atomic(ART/'smoke-rows.json',rows)
    atomic(ART/'campaign-state.json',dict(status='local-correctness-only',expected_cells=2,completed_cells=len(rows)))
    checkpoint(client,c.CFG)
# Check real coverage rather than accepting an HTTP exporter that has no data.
client_catalog=read(ART/'metrics-catalog.json')
metadata_catalog=read(Path(cfg['roles']['metadata'])/'artifacts/metrics-catalog.json')
assert client_catalog['juicefs-sync'] and any('sync_copied' in n for n in client_catalog['juicefs-sync'])
assert any('fuse' in n for n in client_catalog['brewfs-mount'])
assert any('staging' in n for n in client_catalog['juicefs-mount'])
assert any('tikv_' in n for n in metadata_catalog['tikv'])
assert any('pd_' in n for n in metadata_catalog['pd'])
assert read(ART/'live-status.json')['agents']['objects']['reachable']
assert request(c.CFG['agents']['objects']+'/objects')['counts']['PUT_200']['requests']>0
for row in rows:
    assert row['remote_ready'] and row['remote_logical_verification']
    assert all(not d.get('assisted_fuse_cleanup') for d in row['details'])
atomic(ART/'smoke-passed.json',dict(local_correctness_only=True,rows=len(rows),
    native_import=True,clone=True,half_prewarmed_read=True,cow=True,strong_drain=True,
    real_worker_exit=True,complete_observation=True),durable=True)
EVENTS.close()
# Normal successful smoke shutdown; failures above never enter this block.
for name,process,log in reversed(components):
    process.send_signal(signal.SIGTERM);process.wait();log.close()
with open(ART/'service-logs.txt','w') as log:subprocess.run(cfg['compose']+['logs','--no-color'],stdout=log,stderr=subprocess.STDOUT,check=True)
subprocess.run(cfg['compose']+['down','--remove-orphans'],check=True)
atomic(ART/'smoke-cleanup.json',dict(services_stopped=True,bind_data_preserved=True),durable=True)
