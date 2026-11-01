#!/usr/bin/env python3
"""Cloud-only entity/TiKV comparison. Every measured cell includes final exit."""
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import time
import traceback
import uuid
import urllib.request

import boto3
from botocore.config import Config
import juicefs_tools as jfs
import perf_payload
import workloads as wl

HERE=Path(__file__).resolve().parent
ROOT=HERE.parent
jfs.BINARY=str(ROOT/'bin/juicefs')
ART=ROOT/'artifacts'
WORK=ROOT/'work'
CFG=json.loads((HERE/'settings.json').read_text())
S3=boto3.client('s3',endpoint_url=CFG['endpoint'],region_name='us-east-1',config=Config(
    max_pool_connections=16,retries={'max_attempts':0},s3={'addressing_style':'path'}))


def dump(path,value): path.write_text(json.dumps(value,indent=2)+'\n')
def mounted(path): return subprocess.run(['mountpoint','-q',str(path)]).returncode==0
def records(path,name):
    return [json.loads(line.split('=',1)[1]) for line in path.read_text().splitlines() if line.startswith(name+'=')]


class Probe:
    def __init__(self,backend,volume,namespace,art):
        endpoint=f'127.0.0.1:{CFG["pd_port"]}' if backend=='tikv' else f'redis://127.0.0.1:{CFG["redis_port"]}/0'
        self.log=open(art/'probe.log','w')
        self.proc=subprocess.Popen([str(ROOT/'bin/entity_metadata_probe'),'--backend',backend,'--endpoint',endpoint,
                                    '--volume',volume],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=self.log,text=True,bufsize=1)
        self.namespace=namespace
        assert self.receive()=={'ready':True}
    def receive(self):
        ready,_,_=select.select([self.proc.stdout],[],[],60)
        assert ready,'metadata observer timed out'
        line=self.proc.stdout.readline()
        assert line,'metadata observer exited; see probe.log'
        return json.loads(line)
    def query(self,op):
        self.proc.stdin.write(json.dumps(dict(op=op,namespace=self.namespace))+'\n');self.proc.stdin.flush()
        return self.receive()
    def close(self):
        self.proc.stdin.close()
        assert self.proc.wait(timeout=30)==0
        self.log.close()


def backend_metrics(path):
    try:
        data=urllib.request.urlopen(f'http://127.0.0.1:{CFG["tikv_status_port"]}/metrics',timeout=10).read()
        path.write_bytes(data)
        pd=urllib.request.urlopen(f'http://127.0.0.1:{CFG["pd_port"]}/metrics',timeout=10).read()
        path.with_name(path.stem.replace('tikv','pd')+'.prom').write_bytes(pd)
    except OSError as e: raise RuntimeError('TiKV metrics required') from e


def object_offset(): return (WORK/'object-requests.log').stat().st_size
def object_delta(offset,end=None):
    with open(WORK/'object-requests.log','rb') as f:
        f.seek(offset);lines=f.read(-1 if end is None else end-offset).decode().splitlines()
    result={}
    for line in lines:
        method,status,size,*_=line.split()
        k=f'{method}_{status}'
        item=result.setdefault(k,{'requests':0,'response_bytes':0})
        item['requests']+=1;item['response_bytes']+=int(size)
    return result


class Cell:
    def __init__(self,label,client,backend,lower,revision,mode,threads,juice_meta=None,writable_existing=False):
        self.art=ART/label;self.art.mkdir()
        self.work=WORK/label;self.work.mkdir()
        self.mount=self.work/'mnt';self.mount.mkdir()
        self.cache=self.work/'cache';self.cache.mkdir()
        self.logpath=self.art/'mount.log'
        self.proc=None;self.probe=None
        self.client=client;self.backend=backend;self.mode=mode
        self.volume=str(uuid.uuid4());self.namespace=str(uuid.uuid4())
        self.connections=[]
        self.env=dict(os.environ,TOKIO_WORKER_THREADS=str(threads),GOMAXPROCS=str(threads))
        self.env.update(ACCESS_KEY=os.environ['AWS_ACCESS_KEY_ID'],SECRET_KEY=os.environ['AWS_SECRET_ACCESS_KEY'])
        self.meta=juice_meta or f'tikv://127.0.0.1:{CFG["pd_port"]}/jfs-{uuid.uuid4().hex}'
        begin=time.perf_counter()
        if client=='juicefs':
            if juice_meta is None:
                name='jfs-'+uuid.uuid4().hex
                command=[jfs.BINARY,'format','--storage=s3','--bucket='+CFG['endpoint']+'/'+CFG['bucket'],
                         '--block-size=4096','--compress=none','--trash-days=0',self.meta,name]
                dump(self.art/'format-command.json',command)
                with open(self.art/'format.log','w') as output:
                    subprocess.run(command,env=self.env,stdout=output,stderr=subprocess.STDOUT,check=True,timeout=180)
            command=jfs.mount_command(self.meta,self.mount,self.cache,mode,8,readonly=juice_meta is not None and not writable_existing)
        else:
            m=f'  backend: {backend}\n'
            m+=f'  tikv: {{pd_endpoints: ["127.0.0.1:{CFG["pd_port"]}"]}}\n' if backend=='tikv' else f'  redis: {{url: "redis://127.0.0.1:{CFG["redis_port"]}/0"}}\n'
            yaml=f'''data:
  backend: s3
  s3:
    bucket: {CFG['bucket']}
    endpoint: {CFG['endpoint']}
    region: us-east-1
    force_path_style: true
    max_concurrency: 8
meta:
{m}layout: {{chunk_size: 4194304, block_size: 4194304}}
fuse: {{workers: 16, max_background: 256, privileged: false}}
cache:
  root: {self.cache}
  read_memory_bytes: 0
  read_ssd_bytes: 0
  write_ssd_bytes: 1073741824
  writeback_mode: {mode}
'''
            (self.art/'mount.yaml').write_text(yaml)
            command=[str(ROOT/f'bin/brewfs-{revision}'),'mount',str(self.mount),'--config',str(self.art/'mount.yaml'),
                     '--volume-format','workspace-entity-v1','--experimental-entity-mount','--entity-volume-id',self.volume,
                     '--workspace',self.namespace,'--workspace-namespace','perf-'+self.namespace]
            command+=['--packed-manifest-key',CFG['manifest']] if lower=='packed' else ['--entity-native-source',str(WORK/'source')]
            self.probe=Probe(backend,self.volume,self.namespace,self.art)
        dump(self.art/'command.json',command)
        dump(self.art/'identity.json',dict(client=client,backend=backend,lower=lower,revision=revision,mode=mode,threads=threads,
                                          volume=self.volume,namespace=self.namespace,juicefs_meta=self.meta if client=='juicefs' else None))
        self.log=open(self.logpath,'w')
        self.proc=subprocess.Popen(command,stdout=self.log,stderr=subprocess.STDOUT,env=self.env)
        jfs.wait_mount(self.proc,self.mount,self.logpath)
        self.connections=perf_payload.fuse_connections(self.proc.pid,self.mount)
        for connection in self.connections:
            control=Path('/sys/fs/fuse/connections')/connection/'max_background'
            control.write_text('256\n');assert control.read_text().strip()=='256'
        if client=='juicefs':
            cfgpath=next(self.mount/n for n in ['.config','.jfs.config'] if (self.mount/n).exists())
            actual=json.loads(cfgpath.read_text().replace('\0',''))
            dump(self.art/'effective-config.json',{k:actual[k] for k in ['Chunk','FuseOpts','AttrTimeout','EntryTimeout','DirEntryTimeout','Version']})
            assert actual['Chunk']['CacheSize']<=1
            assert actual['Chunk']['Writeback']==(mode=='commit_before_upload')
        self.bootstrap_s=time.perf_counter()-begin

    def pending(self):
        return jfs.pending(self.mount) if self.client=='juicefs' else self.probe.query('status')['debt']

    def stop(self):
        self.unmount_attempts=[]
        self.debt_probe_started=time.perf_counter()
        self.pending_at_end=self.pending()
        self.debt_observed_at=time.perf_counter()
        if self.client=='juicefs':
            jfs.wait_drain(self.mount,self.cache,timeout=240)
            self.remote_ready_at=time.perf_counter()
            dump(self.art/'metrics-drained.json',jfs.metrics(self.mount))
            unmount_log=self.art/'unmount-attempts.json'
            self.assisted=jfs.stop(self.proc,self.mount,drained=True,unmount_log=unmount_log)
            self.unmount_attempts=json.loads(unmount_log.read_text())
        else:
            self.proc.send_signal(signal.SIGTERM)
            deadline=time.monotonic()+240
            ready_at=None;self.assisted=[]
            while self.proc.poll() is None:
                now=time.perf_counter()
                stopped=records(self.logpath,'entity_stopped')
                if stopped and stopped[0].get('remote_ready'):
                    if ready_at is None: ready_at=now
                    if now-ready_at>=.1 and not self.assisted:
                        status=self.probe.query('status')
                        assert status['debt']==dict(objects=0,bytes=0) and status['descriptor']['state']=='Quiescent'
                        assert not mounted(self.mount)
                        for connection in self.connections:
                            control=Path('/sys/fs/fuse/connections')/connection
                            try:
                                assert int((control/'waiting').read_text())==0
                                (control/'abort').write_text('1\n')
                                self.assisted.append(dict(connection=connection,after_remote_ready_s=now-ready_at))
                            except FileNotFoundError: pass
                if time.monotonic()>deadline: raise TimeoutError('BrewFS complete drain/process-exit timeout')
                time.sleep(.002)
            assert self.proc.wait()==0,self.logpath.read_text()[-3000:]
            status=records(self.logpath,'entity_writeback'); stopped=records(self.logpath,'entity_stopped')
            assert len(status)==len(stopped)==1 and stopped[0]['remote_ready']
            assert status[0]['pending_objects']==status[0]['pending_bytes']==0
            self.remote_ready_at=ready_at or time.perf_counter()
            dump(self.art/'writeback.json',status[0])
        assert not mounted(self.mount)
        self.ended=time.perf_counter()
        self.log.close()

    def verify_upper(self):
        if self.client=='juicefs': return None # A fresh cache-free mount verifies the logical files below.
        rows=self.probe.query('objects')
        descriptors=[json.loads(bytes.fromhex(v)) for v in rows.values()]
        def verify(d):
            key=f'workspace-entity-v1/{self.volume}/{d["namespace_id"]}/{d["slice_id"]}'
            reply=S3.get_object(Bucket=CFG['bucket'],Key=key)
            data=reply['Body'].read();reply['Body'].close()
            assert len(data)==d['size'] and list(hashlib.sha256(data).digest())==d['sha256']
            return dict(key=key,size=d['size'])
        with concurrent.futures.ThreadPoolExecutor(8) as pool: objects=list(pool.map(verify,descriptors))
        dump(self.art/'verified-upper-objects.json',objects)
        return len(objects)

    def finish_observer(self):
        if self.probe: self.probe.close();self.probe=None

    def cleanup_failure(self):
        if getattr(self,'proc',None) and self.proc.poll() is None:
            self.proc.terminate()
            try:self.proc.wait(timeout=60)
            except subprocess.TimeoutExpired:self.proc.kill();self.proc.wait()
        if getattr(self,'mount',None) and mounted(self.mount):subprocess.run(['fusermount3','-u',str(self.mount)],check=True,timeout=30)
        if getattr(self,'probe',None):self.probe.close()


def mount_cell(*args,**kwargs):
    cell=Cell.__new__(Cell)
    try:
        Cell.__init__(cell,*args,**kwargs)
        return cell
    except BaseException:
        if getattr(cell,'art',None) and cell.art.exists():(cell.art/'startup-failure.txt').write_text(traceback.format_exc())
        cell.cleanup_failure()
        raise


def prepare_juicefs():
    cell=mount_cell('juicefs-corpus-bootstrap','juicefs','tikv','none','stock','upload_before_commit',8)
    begin=time.perf_counter()
    for path in sorted((WORK/'source').rglob('*')):
        if path.is_dir():(cell.mount/path.relative_to(WORK/'source')).mkdir()
    paths=sorted(p for p in (WORK/'source').rglob('*') if p.is_file())
    def write(path):
        target=cell.mount/path.relative_to(WORK/'source')
        with open(target,'wb',buffering=0) as f:f.write(path.read_bytes());os.fsync(f.fileno())
    try:
        with concurrent.futures.ThreadPoolExecutor(8) as pool:list(pool.map(write,paths))
        active_end=time.perf_counter();cell.stop()
    except BaseException:
        cell.cleanup_failure();raise
    dump(cell.art/'result.json',dict(copy_active_s=active_end-begin,complete_copy_s=cell.ended-begin,meta=cell.meta))
    return cell.meta


def run_one(spec,number,juice_meta):
    client,backend,lower,revision,mode,profile,rep,threads,workers=spec[:9]
    layout=spec[9] if len(spec)>9 else '256k'
    CFG['manifest']=CFG['manifests'][layout]
    label=f'{number:03d}-{profile}-{backend}-{lower}-{revision}-{mode}-r{rep}'
    cell=mount_cell(label,client,backend,lower,revision,mode,threads,juice_meta if client=='juicefs' and profile.endswith('read') else None)
    verify=None
    try:
        fio=None
        setup_begin=time.perf_counter()
        if profile in ['small-write','bulk-write']:wl.prepare_writes(cell.mount,profile)
        if profile=='cow' and client=='juicefs':
            (cell.mount/'small').mkdir()
            for d in range(wl.SMALL_DIRS):(cell.mount/'small'/f'd{d:03d}').mkdir()
            def copy_small(i):
                with open(wl.small_path(cell.mount/'small',i),'wb',buffering=0) as f:
                    f.write(wl.payload(i,wl.SMALL_BYTES));os.fsync(f.fileno())
            with concurrent.futures.ThreadPoolExecutor(8) as pool:list(pool.map(copy_small,range(wl.SMALL_FILES)))
            jfs.wait_drain(cell.mount,cell.cache,timeout=240)
        if profile=='randrw':
            corpus=cell.mount/'mixed';corpus.mkdir()
            fprofile=dict(name='randrw',kind='fio',file_bytes=8<<20,direct=1,runtime_s=10,files=workers)
            perf_payload.seed(corpus,fprofile,workers,cell.art)
            deadline=time.monotonic()+240
            while any(cell.pending().values()):
                assert time.monotonic()<deadline
                time.sleep(.01)
        backend_metrics(cell.art/'tikv-before.prom')
        dump(cell.art/'host-before.json',perf_payload.host_snapshot(cell.proc.pid))
        setup_s=time.perf_counter()-setup_begin
        offset=object_offset();started=time.perf_counter()
        read_counter=[]
        if profile.endswith('read'):
            half=profile.startswith('half-');kind='large' if 'large' in profile else 'small'
            result=wl.run_reads(cell.mount,kind,half,workers,cell.art,lambda:read_counter.append(object_offset()))
        elif profile=='cow':result=wl.cow_files(cell.mount,workers)
        elif profile=='randrw':
            times,fio=perf_payload.run(None,corpus,fprofile,workers,cell.art)
            wall=time.perf_counter()-started
            result=dict(active_s=wall,user_bytes=fio['read']['io_bytes']+fio['write']['io_bytes'],
                        p50_ms=times[0],p95_ms=times[1],fio_sync=fio.get('sync'),
                        fio={d:{k:fio[d][k] for k in ['io_bytes','bw_bytes','iops','clat_ns']} for d in ['read','write']})
        else:result=wl.write_files(cell.mount,profile,workers)
        active_end=time.perf_counter()
        active_object_end=object_offset()
        dump(cell.art/'host-after-work.json',perf_payload.host_snapshot(cell.proc.pid))
        cell.stop()
        active_objects=object_delta(read_counter[0] if read_counter else offset,active_object_end)
        prewarm_s=result.get('prewarm_s',0)
        measured_active_s=active_end-started-prewarm_s
        result.update(profile=profile,client=client,metadata=backend,lower=lower,revision=revision,mode=mode,repetition=rep,
                      layout=layout,frame_cap_bytes=CFG['frame_caps'][layout] if client=='brewfs' else None,
                      workers=workers,runtime_threads=threads,bootstrap_s=cell.bootstrap_s,workload_setup_s=setup_s,
                      active_s=measured_active_s,
                      work_with_prewarm_s=active_end-started,drain_tail_s=cell.ended-active_end,
                      total_s=cell.ended-started,active_plus_drain_s=cell.ended-started-prewarm_s,
                      prewarm_s=prewarm_s,lifecycle_s=cell.bootstrap_s+setup_s+cell.ended-started,
                      total_mib_s=result['user_bytes']/(cell.ended-started-prewarm_s)/(1<<20),
                      active_mib_s=result['user_bytes']/measured_active_s/(1<<20),
                      debt_probe_s=cell.debt_observed_at-cell.debt_probe_started,
                      harness_before_drain_s=cell.debt_probe_started-active_end,pending_at_end=cell.pending_at_end,
                      remote_ready_after_active_s=cell.remote_ready_at-active_end,remote_ready=True,
                      assisted_fuse_cleanup=cell.assisted,ordinary_unmount_attempts=cell.unmount_attempts,
                      object_requests_active=active_objects,
                      object_requests_includes_prewarm_and_drain=object_delta(offset))
        backend_metrics(cell.art/'tikv-after.prom')
        result['upper_objects_verified']=cell.verify_upper()
        if profile.endswith('read') and client=='brewfs':assert result['upper_objects_verified']==0,'read must stay in immutable lower'
        if client=='juicefs' and not profile.endswith('read'):
            verify=mount_cell(label+'-remote-verify','juicefs','tikv','none','stock','upload_before_commit',threads,cell.meta)
            if profile=='randrw':perf_payload.verify_files(None,verify.mount/'mixed',fprofile,workers,verify.art)
            elif profile=='cow':
                for i in range(100):
                    expected=wl.payload(i,4096)+wl.payload(i+20000,4096)+wl.payload(i,8192,8192)
                    assert wl.small_path(verify.mount/'small',i).read_bytes()==expected
            else:
                count,size=(wl.SMALL_FILES,wl.SMALL_BYTES) if profile=='small-write' else (wl.LARGE_FILES,wl.LARGE_BYTES)
                for i in range(count):
                    path=wl.small_path(verify.mount/'created',i) if profile=='small-write' else verify.mount/'created'/f'f{i:05d}'
                    assert path.read_bytes()==wl.payload(i,size)
            verify.stop()
            result['remote_logical_verification']=True
        cell.finish_observer()
        dump(cell.art/'result.json',result)
        return result
    except BaseException:
        (cell.art/'failure.txt').write_text(traceback.format_exc())
        try:
            backend_metrics(cell.art/'tikv-failure.prom')
        except Exception:
            (cell.art/'failure-metrics-error.txt').write_text(traceback.format_exc())
        if verify:verify.cleanup_failure()
        cell.cleanup_failure();raise


def main():
    assert os.environ.get('ENTITY_CLOUD_VALIDATED')=='1'
    assert not subprocess.check_output(['swapon','--noheadings','--show']).strip()
    plan=json.loads((HERE/'plan.json').read_text())
    juice_meta=prepare_juicefs()
    rows=[]
    for n,spec in enumerate(plan['cells'],1):
        (ART/'phase.txt').write_text(json.dumps(dict(index=n,spec=spec)))
        row=run_one(spec,n,juice_meta);rows.append(row);dump(ART/'rows.json',rows)
        print(f'{n}/{len(plan["cells"])} {row["profile"]} {row["metadata"]} {row["revision"]}: active={row["active_s"]:.3f}s tail={row["drain_tail_s"]:.3f}s total={row["total_s"]:.3f}s',flush=True)
        if n%10==0:
            subprocess.run(['tar','-C',str(ROOT),'-czf',str(ROOT/f'checkpoint-{n:04d}.tar.gz'),'artifacts','campaign.log'],check=True)
            (ROOT/'checkpoint-ready.txt').write_text(f'{n:04d}\n')
            # Collection must not consume CPU/network during the next cell.
            # The controller acknowledges only after checking the downloaded hash.
            ack=ROOT/f'checkpoint-{n:04d}-collected'
            deadline=time.monotonic()+600
            while not ack.exists():
                if time.monotonic()>=deadline: raise TimeoutError('checkpoint collection was not acknowledged')
                time.sleep(1)
            time.sleep(1)
    dump(ART/'completed.json',dict(cells=len(rows),all_remote_ready=True))

if __name__=='__main__':main()
