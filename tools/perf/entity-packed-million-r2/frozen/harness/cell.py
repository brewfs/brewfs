"""Dedicated entity/JuiceFS mounts, preserving failures and real worker exits."""
from pathlib import Path
import concurrent.futures,hashlib,json,os,signal,subprocess,time,uuid
import boto3
from botocore.config import Config
import juicefs_tools as jfs
import perf_payload
from common import ROOT,ART,atomic,read,stamp
from control import stage,target,metric_ready,request
from managed import ManagedProcess,run

CFG=read(ROOT/'harness/settings.json')
WORK=ROOT/'work'
jfs.BINARY=str(ROOT/'bin/juicefs')
S3=boto3.client('s3',endpoint_url=CFG['endpoint'],region_name='us-east-1',
    config=Config(max_pool_connections=16,retries={'max_attempts':0},s3={'addressing_style':'path'}))

def mounted(path):return subprocess.run(['mountpoint','-q',str(path)],stdout=subprocess.DEVNULL).returncode==0
def records(path,name):
    return [json.loads(line.split('=',1)[1]) for line in path.read_text().splitlines() if line.startswith(name+'=')]

class Probe:
    def __init__(self,volume,namespace,art):
        self.log=open(art/'probe.log','w');self.namespace=namespace
        self.proc=subprocess.Popen([str(ROOT/'bin/entity_metadata_probe'),'--backend','tikv',
            '--endpoint',CFG['pd_endpoint'],'--volume',volume],stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,stderr=self.log,text=True,bufsize=1)
        assert self.receive()=={'ready':True}
    def receive(self):
        line=self.proc.stdout.readline();assert line,'Metadata observer exited; inspect probe.log'
        return json.loads(line)
    def query(self,op):
        self.proc.stdin.write(json.dumps(dict(op=op,namespace=self.namespace))+'\n');self.proc.stdin.flush()
        return self.receive()
    def close(self):
        self.proc.stdin.close();assert self.proc.wait()==0;self.log.close()

class Cell:
    def __init__(self,label,client,meta=None,writable=False):
        self.label=label;self.client=client;self.art=ART/label;self.art.mkdir()
        self.work=WORK/label;self.work.mkdir();self.mount=self.work/'mnt';self.mount.mkdir()
        self.cache=self.work/'cache';self.cache.mkdir();self.logpath=self.art/'mount.log'
        self.volume=str(uuid.uuid4());self.namespace=str(uuid.uuid4());self.probe=None
        self.env=dict(os.environ,TOKIO_WORKER_THREADS='8',GOMAXPROCS='8',
            ACCESS_KEY=os.environ['AWS_ACCESS_KEY_ID'],SECRET_KEY=os.environ['AWS_SECRET_ACCESS_KEY'])
        self.meta=meta or f'tikv://{CFG["pd_endpoint"]}/jfs-{uuid.uuid4().hex}'
        self.metrics_name=client+'-mount';self.metrics_url='http://127.0.0.1:8910/metrics'
        with stage(label+'-bootstrap') as timing:
            if client=='juicefs':
                if meta is None:
                    command=[jfs.BINARY,'format','--storage=s3','--bucket='+CFG['endpoint']+'/'+CFG['bucket'],
                        '--block-size=4096','--compress=none','--trash-days=0',self.meta,'jfs-'+uuid.uuid4().hex]
                    with open(self.art/'format.log','w') as log:run(command,env=self.env,stdout=log,check=True)
                command=jfs.mount_command(self.meta,self.mount,self.cache,'upload_before_commit',8,
                    readonly=meta is not None and not writable)
                command=[arg if arg!='--metrics=127.0.0.1:0' else '--metrics=127.0.0.1:8910' for arg in command]
            else:
                yaml=f'''data:
  backend: s3
  s3:
    bucket: {CFG['bucket']}
    endpoint: {CFG['endpoint']}
    region: us-east-1
    force_path_style: true
    max_concurrency: 8
meta:
  backend: tikv
  tikv: {{pd_endpoints: ["{CFG['pd_endpoint']}"]}}
layout: {{chunk_size: 4194304, block_size: 4194304}}
fuse: {{workers: 16, max_background: 256, privileged: false}}
cache:
  root: {self.cache}
  read_memory_bytes: 0
  read_ssd_bytes: 0
  write_ssd_bytes: 1073741824
  writeback_mode: upload_before_commit
'''
                (self.art/'mount.yaml').write_text(yaml)
                self.probe=Probe(self.volume,self.namespace,self.art)
                command=[str(ROOT/'bin/brewfs'),'--metrics','--metrics-addr','127.0.0.1:8910',
                    'mount',str(self.mount),'--config',str(self.art/'mount.yaml'),
                    '--volume-format','workspace-entity-v1','--experimental-entity-mount',
                    '--entity-volume-id',self.volume,'--workspace',self.namespace,
                    '--workspace-namespace','perf-'+self.namespace,'--packed-manifest-key',CFG['manifest']]
            atomic(self.art/'command.json',command)
            atomic(self.art/'identity.json',dict(client=client,volume=self.volume,namespace=self.namespace,
                juicefs_meta=self.meta if client=='juicefs' else None))
            self.log=open(self.logpath,'w');target(self.metrics_name,self.metrics_url,'starting')
            self.proc=ManagedProcess(command,stdout=self.log,env=self.env)
            while not mounted(self.mount):
                if self.proc.poll() is not None:raise RuntimeError('Mount exited: '+str(self.logpath))
                time.sleep(.025)
            # Select a live, component-owned process actually holding /dev/fuse.
            while True:
                state=read(self.proc.state);worker=None
                for item in state['live']:
                    try:
                        if any(os.readlink(fd)=='/dev/fuse' for fd in Path(f'/proc/{item["pid"]}/fd').iterdir()):
                            worker=item['pid'];break
                    except FileNotFoundError:pass
                if worker:break
                if self.proc.poll() is not None:raise RuntimeError('No live FUSE worker')
                time.sleep(.025)
            self.connections=perf_payload.fuse_connections(worker,self.mount)
            atomic(self.art/'fuse-worker.json',dict(pid=worker,connections=self.connections,state=state))
            for connection in self.connections:
                control=Path('/sys/fs/fuse/connections')/connection/'max_background'
                control.write_text('256\n');assert control.read_text().strip()=='256'
            metric_ready(self.metrics_name,self.metrics_url,
                ['juicefs_staging_blocks'] if client=='juicefs' else ['brewfs_'],self.proc)
            if client=='juicefs':
                cfgpath=next(self.mount/n for n in ['.config','.jfs.config'] if (self.mount/n).exists())
                actual=json.loads(cfgpath.read_text().replace('\0',''))
                atomic(self.art/'effective-config.json',{k:actual[k] for k in
                    ['Chunk','FuseOpts','AttrTimeout','EntryTimeout','DirEntryTimeout','Version']})
                assert actual['Chunk']['CacheSize']<=1 and not actual['Chunk']['Writeback']
        self.bootstrap_s=timing['elapsed_s']

    def pending(self):return jfs.pending(self.mount) if self.client=='juicefs' else self.probe.query('status')['debt']
    def stop(self):
        with stage(self.label+'-drain-and-exit') as timing:
            self.debt_probe_started=time.perf_counter();self.pending_at_end=self.pending()
            atomic(self.art/'pending-at-end.json',self.pending_at_end)
            request(CFG['agents']['client']+'/scrape',{})
            target(self.metrics_name,self.metrics_url,'stopping')
            self.unmount_attempts=[];self.assisted=[]
            if self.client=='juicefs':
                stable=0
                while stable<2:
                    debt=self.pending();leftovers=[p for d in self.cache.rglob('rawstaging') for p in d.rglob('*') if p.is_file()]
                    stable=stable+1 if not any(debt.values()) and not leftovers else 0
                    atomic(self.art/'drain-progress.json',dict(debt=debt,stable=stable,**stamp()))
                    if stable<2:time.sleep(.01)
                self.remote_ready_at=time.perf_counter();atomic(self.art/'metrics-drained.json',jfs.metrics(self.mount))
                request(CFG['agents']['client']+'/scrape',{})
                while mounted(self.mount):
                    started=time.perf_counter();logpath=self.art/f'unmount-{len(self.unmount_attempts)}.log'
                    with open(logpath,'w') as output:
                        proc=ManagedProcess([jfs.BINARY,'umount',str(self.mount)],env=self.env,stdout=output)
                        code=proc.wait()
                    self.unmount_attempts.append(dict(returncode=code,elapsed_s=time.perf_counter()-started))
                    atomic(self.art/'unmount-attempts.json',self.unmount_attempts)
                    if code:
                        if 'Device or resource busy' not in logpath.read_text():raise RuntimeError('Ordinary unmount failed')
                        time.sleep(.05)
                assert self.proc.wait()==0,'Real JuiceFS worker exit failed'
            else:
                self.proc.send_signal(signal.SIGTERM);self.remote_ready_at=None
                while self.proc.poll() is None:
                    stopped=records(self.logpath,'entity_stopped')
                    if stopped and stopped[0].get('remote_ready') and self.remote_ready_at is None:
                        self.remote_ready_at=time.perf_counter()
                    time.sleep(.01)
                assert self.proc.wait()==0,'Real BrewFS worker exit failed'
                status=records(self.logpath,'entity_writeback');stopped=records(self.logpath,'entity_stopped')
                assert len(status)==len(stopped)==1 and stopped[0]['remote_ready']
                assert status[0]['pending_objects']==status[0]['pending_bytes']==0
                self.remote_ready_at=self.remote_ready_at or time.perf_counter()
                atomic(self.art/'writeback.json',status[0])
            assert not mounted(self.mount)
            self.ended=time.perf_counter();self.log.close();target(self.metrics_name,self.metrics_url,'stopped')
        self.teardown=dict(remote_ready=True,drain_tail_s=self.ended-self.debt_probe_started,
            ordinary_unmount_attempts=self.unmount_attempts,assisted_fuse_cleanup=[],
            real_worker_exit=read(self.proc.state),observed_drain_phase_s=timing['elapsed_s'])
        return self.teardown

    def verify_upper(self):
        if self.client=='juicefs':return None
        descriptors=[json.loads(bytes.fromhex(v)) for v in self.probe.query('objects').values()]
        def verify(d):
            key=f'workspace-entity-v1/{self.volume}/{d["namespace_id"]}/{d["slice_id"]}'
            reply=S3.get_object(Bucket=CFG['bucket'],Key=key)
            data=reply['Body'].read();reply['Body'].close()
            assert len(data)==d['size'] and list(hashlib.sha256(data).digest())==d['sha256']
            return dict(key=key,size=d['size'])
        with concurrent.futures.ThreadPoolExecutor(8) as pool:objects=list(pool.map(verify,descriptors))
        atomic(self.art/'verified-upper-objects.json',objects);return len(objects)

    def finish_observer(self):
        if self.probe:self.probe.close();self.probe=None
