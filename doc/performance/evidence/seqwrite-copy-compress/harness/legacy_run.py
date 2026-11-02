#!/usr/bin/env python3
"""Repeatable measurement harness; run as root. No repository modifications."""
import argparse, collections, contextlib, errno, hashlib, json, mmap, os, pathlib, re, shutil
import signal, subprocess, threading, time, traceback, urllib.request
import yaml

ROOT=pathlib.Path('/home/hxy/perf-full-20261101')
STATE=pathlib.Path('/home/hxy/perf-full-state-20261101')
BIN=ROOT/'bin/brewfs-observability'
FIXTURE=ROOT/'bin/packed_v3_snapshot_fixture'
ENDPOINT='http://127.0.0.1:29000'
METRICS='http://127.0.0.1:9567/metrics'
BUCKET='brewfs-perf-full-20261101'
ENV=dict(os.environ)
for key in list(ENV):
    if key.startswith(('BREWFS_','PERF_','AWS_')) or key.lower() in ['http_proxy','https_proxy','all_proxy','no_proxy']:
        ENV.pop(key,None)
ENV.update(AWS_ACCESS_KEY_ID='rustfsadmin',AWS_SECRET_ACCESS_KEY='rustfsadmin',AWS_DEFAULT_REGION='us-east-1',
           AWS_EC2_METADATA_DISABLED='true',NO_PROXY='localhost,127.0.0.1',RUST_LOG='warn,brewfs=info',
           TOKIO_WORKER_THREADS='1',
           BREWFS_REQUEST_TRACE='false',BREWFS_FUSE_WRITEBACK='false',
           BREWFS_PACKED_METADATA_CACHE_BYTES=str(256<<20))
DEBT=['brewfs_writeback_dirty_bytes','brewfs_buffer_dirty_bytes','brewfs_writeback_live_dirty_bytes',
      'brewfs_writeback_live_slices','brewfs_writeback_recent_pending_upload_bytes',
      'brewfs_writeback_recent_pending_upload_slices','brewfs_writeback_stage_inflight_bytes',
      'brewfs_writeback_remote_upload_inflight_bytes','brewfs_writeback_metadata_inflight']

def log(message):
    print(time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),message,flush=True)

def save(path,obj):
    path=pathlib.Path(path); path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(obj,indent=2)+'\n')

def command(argv,path,timeout=600,env=ENV):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('w') as output:
        p=subprocess.run([str(x) for x in argv],stdout=output,stderr=subprocess.STDOUT,env=env,cwd=STATE,timeout=timeout)
    if p.returncode: raise RuntimeError(f'command status {p.returncode}; {path}')

def metrics():
    with urllib.request.urlopen(METRICS,timeout=5) as response: return response.read().decode()

def parse(text):
    values={}
    for line in text.splitlines():
        if line and not line.startswith('#'):
            try: k,v=line.rsplit(' ',1); values[k]=float(v)
            except ValueError: pass
    return values

def total(values,name): return sum(v for k,v in values.items() if k==name or k.startswith(name+'{'))

def object_delta(before,after):
    out={}
    for op in ['get','put']:
        count=total(after,f'brewfs_object_{op}_total')-total(before,f'brewfs_object_{op}_total')
        size=total(after,f'brewfs_object_{op}_bytes_total')-total(before,f'brewfs_object_{op}_bytes_total')
        out[op]={'count':count,'bytes':size,'average_bytes':size/count if count else None}
    return out

def histograms(before,after):
    groups=collections.defaultdict(list)
    for key,value in after.items():
        m=re.fullmatch(r'brewfs_(fuse_op|object_op|metadata_op)_latency_seconds_bucket\{(.+)\}',key)
        if not m: continue
        labels=dict(re.findall(r'(\w+)="([^"]*)"',m[2])); bound=float(labels.pop('le'))
        group=m[1]+':'+','.join(f'{k}={v}' for k,v in sorted(labels.items()))
        groups[group].append((bound,value-before.get(key,0)))
    result={}
    for group,buckets in groups.items():
        buckets.sort(); count=buckets[-1][1]
        row={'count':count,'buckets':[['+Inf' if b==float('inf') else b,n] for b,n in buckets]}
        for percentile in [50,95,99]:
            target=count*percentile/100; last_bound=last_count=0; q=None; overflow=False
            if count:
                for bound,n in buckets:
                    if n>=target:
                        if bound==float('inf'): q=last_bound; overflow=True
                        else: q=last_bound+(bound-last_bound)*(target-last_count)/(n-last_count) if n>last_count else bound
                        break
                    last_bound,last_count=bound,n
            row[f'p{percentile}_ms']=None if q is None else q*1000
            row[f'p{percentile}_overflow']=overflow
        result[group]=row
    return result

def snapshot(mount,directory,prefix):
    http=metrics(); (directory/f'{prefix}.prom').write_text(http)
    legacy=(mount/'.stats').read_text(); (directory/f'{prefix}.stats').write_text(legacy)
    return parse(http),parse(legacy)

def drain(mount,directory):
    directory.mkdir(parents=True,exist_ok=True)
    start=time.monotonic(); stable=0; previous=None; samples=[]; first_clear=None
    while True:
        http,legacy=snapshot(mount,directory,f'sample-{len(samples):03d}')
        missing=[k for k in DEBT if k not in legacy]
        if missing: raise RuntimeError(f'missing drain metrics {missing}; {directory}')
        debt={k:legacy[k] for k in DEBT}
        signature=tuple(debt.values())+(total(http,'brewfs_object_put_total'),total(http,'brewfs_object_put_bytes_total'))
        elapsed=time.monotonic()-start
        clear=all(v==0 for v in debt.values())
        if clear and first_clear is None: first_clear=elapsed
        stable=(stable+1 if signature==previous else 1) if clear else 0
        samples.append({'elapsed_seconds':elapsed,'debt':debt,'put_count':signature[-2],'put_bytes':signature[-1],
                        'stable_samples':stable})
        previous=signature
        if stable>=3 or elapsed>=180:
            result={'seconds':elapsed,'first_clear_seconds':first_clear,'stable_samples':stable,
                    'clear':clear and stable>=3,'samples':samples}
            save(directory/'drain.json',result)
            if not result['clear']: raise RuntimeError(f'drain timed out: {directory}')
            return result,http
        time.sleep(.5)

def config(mode,mount,cache,manifest=None):
    cfg={'mount_point':str(mount),'volume_format':'flat-v1' if mode=='flat-v1' else 'packed-metadata-v3',
         'data':{'backend':'s3','s3':{'bucket':BUCKET,'endpoint':ENDPOINT,'region':'us-east-1',
                 'max_concurrency':8,'part_size':16777216,'force_path_style':True,'disable_payload_checksum':True}},
         'layout':{'chunk_size':67108864,'block_size':4194304},
         'fuse':{'workers':16,'max_background':512,'cache_ttl_ms':0,'keep_cache':False,'privileged':True},
         'cache':{'cache_root':str(cache),'read_memory_bytes':0,'read_ssd_bytes':0,'write_memory_bytes':536870912,
                  'write_ssd_bytes':0,'memory_budget_bytes':2147483648,'prefetch_enabled':False,
                  'range_background_prefetch':False,'populate_write_cache_after_upload':False,
                  'persist_write_cache_after_upload':False,'compression':'zstd','zstd_level':1,
                  'upload_concurrency':10,'dirty_slice_target_size':33554432,'dirty_slice_max_age_ms':2000,
                  'writeback_mode':'upload_before_commit'}}
    if mode=='flat-v1': cfg['meta']={'backend':'redis','redis':{'url':'redis://127.0.0.1:26379/0'}}
    else: cfg['packed_manifest_key']=manifest
    return cfg

@contextlib.contextmanager
def mounted(mode,label,manifest=None,binary=BIN,metrics_on=True):
    directory=ROOT/'mounts'/label; directory.mkdir(parents=True,exist_ok=True)
    mount=STATE/'mounts'/label; mount.mkdir(parents=True,exist_ok=True)
    path=directory/'mount.yaml'; path.write_text(yaml.safe_dump(config(mode,mount,STATE/'cache'/label,manifest),sort_keys=False))
    argv=[str(binary),'mount','--config',str(path)]
    if metrics_on: argv+=['--metrics']
    save(directory/'invocation.json',{'argv':argv,'env':{k:v for k,v in ENV.items() if k.startswith(('BREWFS_','RUST_LOG','TOKIO_'))}})
    with (directory/'daemon.log').open('w') as output:
        process=subprocess.Popen(argv,env=ENV,stdout=output,stderr=subprocess.STDOUT,start_new_session=True)
    save(directory/'pid.json',{'pid':process.pid,'started_unix':time.time()})
    try:
        until=time.monotonic()+60
        while not os.path.ismount(mount):
            if process.poll() is not None: raise RuntimeError(f'mount exited; {directory}/daemon.log')
            if time.monotonic()>until: raise RuntimeError('mount startup timeout')
            time.sleep(.1)
        command(['findmnt','--target',str(mount),'-o','TARGET,SOURCE,FSTYPE,OPTIONS'],directory/'findmnt.txt')
        connections={}
        for connection in pathlib.Path('/sys/fs/fuse/connections').glob('*'):
            values={}
            for key in ['max_background','congestion_threshold','waiting']:
                try:values[key]=(connection/key).read_text().strip()
                except OSError:pass
            connections[connection.name]=values
        save(directory/'fuse-connections.json',connections)
        if metrics_on:
            for _ in range(50):
                try: metrics(); break
                except Exception: time.sleep(.1)
            snapshot(mount,directory,'startup')
        yield mount,process.pid,directory
    finally:
        teardown_start=time.monotonic(); unmount_status=None
        if os.path.ismount(mount):
            try: unmount_status=subprocess.run(['fusermount3','-u',str(mount)],capture_output=True,text=True,timeout=30).returncode
            except subprocess.TimeoutExpired: unmount_status='timeout'
        try: process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.send_signal(signal.SIGTERM)
            try: process.wait(timeout=10)
            except subprocess.TimeoutExpired: pass
        status={'mounted':os.path.ismount(mount),'daemon_returncode':process.poll(),
                'unmount_status':unmount_status,'seconds':time.monotonic()-teardown_start}
        save(directory/'teardown.json',status)
        if status['mounted'] or process.poll()!=0 or unmount_status not in [None,0]:
            raise RuntimeError(f'failed teardown {directory}: {status}')

def scrape_loop(stop,directory,pid):
    i=0
    with (directory/'sampler.jsonl').open('w') as output:
        while not stop.is_set():
            t=time.monotonic(); entry={'unix':time.time(),'monotonic':t}
            try:
                (directory/f'metrics-{i:04d}.prom').write_text(metrics())
                entry['proc_stat']=pathlib.Path(f'/proc/{pid}/stat').read_text()
                entry['host_stat']=pathlib.Path('/proc/stat').read_text().splitlines()[0]
                entry['loadavg']=pathlib.Path('/proc/loadavg').read_text().strip()
            except Exception as e: entry['error']=str(e)
            output.write(json.dumps(entry)+'\n'); output.flush(); i+=1
            stop.wait(max(0,1-(time.monotonic()-t)))

def fio_args(name,mount,output):
    rw,bs,jobs,depth,direct={
        'randread':('randread','4k',4,32,1),'randwrite':('randwrite','4k',4,32,0),
        'seqread':('read','1m',1,8,0),'seqwrite':('write','1m',1,8,0),
        'randrw':('randrw','4k',4,32,0)}[name]
    readonly=name in ['randread','seqread']
    prefix='read' if readonly else name
    path=mount/('large' if readonly else 'writes')
    args=['fio',f'--name={name}',f'--directory={path}',f'--filename_format={prefix}.$jobnum',
          f'--rw={rw}',f'--bs={bs}',f'--numjobs={jobs}',f'--iodepth={depth}',f'--direct={direct}',
          '--ioengine=libaio','--size=512m','--runtime=20','--time_based=1','--group_reporting=1',
          '--eta=never','--randrepeat=1','--randseed=20261101','--invalidate=0','--allow_file_create=0',
          '--lat_percentiles=1','--clat_percentiles=1','--slat_percentiles=1','--percentile_list=50:95:99:99.9',
          '--output-format=json',f'--output={output}','--refill_buffers=1','--scramble_buffers=1']
    if readonly: args.append('--readonly')
    if name=='randrw': args.append('--rwmixread=70')
    return args

def run_round(mode,name,mount,pid,round_kind,index):
    directory=ROOT/'results'/mode/name/f'{round_kind}-{index}'
    directory.mkdir(parents=True,exist_ok=False)
    if mode=='flat-v1' and name=='metadata':
        command(['redis-cli','-h','127.0.0.1','-p','26379','INFO','commandstats'],
                directory/'redis-before-commandstats.txt',timeout=10)
    before,_=snapshot(mount,directory,'before')
    stop=threading.Event(); sampler=threading.Thread(target=scrape_loop,args=(stop,directory,pid),daemon=True)
    perf=None; profile_start=None; profile_end=None; measured=round_kind=='measure'
    if measured:
        argv=['perf','record','-F','499','-g','--call-graph','dwarf,8192','-e','cpu-clock','-z',
              '-p',str(pid),'-o',str(directory/'cpu.data'),'--','sleep','3600']
        save(directory/'perf-command.json',argv)
        with (directory/'perf-record.log').open('w') as out:
            perf=subprocess.Popen(argv,stdout=out,stderr=subprocess.STDOUT,start_new_session=True)
        profile_start=time.time(); time.sleep(.2)
        if perf.poll() is not None: raise RuntimeError('perf failed to start')
    sampler.start()
    if name in ['metadata','smallfiles']:
        argv=['python3',str(ROOT/'harness/worker.py'),name,str(mount),str(directory),'--tag',f'{round_kind}-{index}']
        if mode=='packed-v3': argv.append('--readonly')
    else: argv=fio_args(name,mount,directory/'fio.json')
    save(directory/'command.json',argv)
    started=time.time(); monotonic=time.monotonic(); status=None
    log(f'START {mode}/{name}/{round_kind}-{index}')
    try:
        with (directory/'workload.log').open('w') as out:
            p=subprocess.Popen(argv,env=ENV,stdout=out,stderr=subprocess.STDOUT)
            try: status=p.wait(timeout=300)
            except subprocess.TimeoutExpired:
                p.terminate()
                try: p.wait(timeout=10)
                except subprocess.TimeoutExpired: pass
                raise
        elapsed=time.monotonic()-monotonic
        after,_=snapshot(mount,directory,'after-workload')
    finally:
        stop.set(); sampler.join(timeout=6)
        if perf:
            os.killpg(perf.pid,signal.SIGINT)
            try: perf.wait(timeout=30)
            except subprocess.TimeoutExpired: perf.terminate(); perf.wait(timeout=10)
            profile_end=time.time()
    if status: raise RuntimeError(f'workload status {status}: {directory}')
    drained,after_drain=drain(mount,directory/'drain')
    if mode=='flat-v1' and name=='metadata':
        command(['redis-cli','-h','127.0.0.1','-p','26379','INFO','commandstats'],
                directory/'redis-after-commandstats.txt',timeout=10)
    item={'mode':mode,'workload':name,'phase':round_kind,'round':index,'status':status,
          'started_unix':started,'wall_seconds':elapsed,'perf_started_unix':profile_start,
          'perf_ended_unix':profile_end,'perf_window_seconds':None if profile_start is None else profile_end-profile_start,
          'perf_returncode':None if perf is None else perf.returncode,
          'objects_active':object_delta(before,after),'objects_including_drain':object_delta(before,after_drain),
          'histograms':histograms(before,after),'drain':drained,
          'fuse_errors_delta':{k:v-before.get(k,0) for k,v in after.items()
                               if k.startswith('brewfs_fuse_op_errors_total') and v-before.get(k,0)},
          'sources':{'before':str(directory/'before.prom'),'after':str(directory/'after-workload.prom'),
                     'drain':str(directory/'drain/drain.json')}}
    if name in ['metadata','smallfiles']:
        item['posix']=json.loads((directory/'result.json').read_text())
    else:
        data=json.loads((directory/'fio.json').read_text())
        if any(job['error'] for job in data['jobs']): raise RuntimeError(f'fio errors: {directory}')
        job=data['jobs'][0]; reads=job['read']; writes=job['write']
        active=max(reads['runtime'],writes['runtime'])/1000
        size=reads['io_bytes']+writes['io_bytes']
        item['fio']={'bw_mib_s':(reads['bw_bytes']+writes['bw_bytes'])/1048576,
                     'read_bw_mib_s':reads['bw_bytes']/1048576,'write_bw_mib_s':writes['bw_bytes']/1048576,
                     'io_bytes':size,'active_seconds':active,'wall_bw_mib_s':size/1048576/elapsed,
                     'wall_plus_drain_bw_mib_s':size/1048576/(elapsed+drained['seconds']),
                     'active_plus_drain_bw_mib_s':size/1048576/(active+drained['seconds']),
                     'read_latency':reads['lat_ns'],'write_latency':writes['lat_ns'],
                     'read_clat':reads['clat_ns'],'write_clat':writes['clat_ns'],
                     'iodepth_level':job.get('iodepth_level'),'source':str(directory/'fio.json')}
        if active<19.5: raise RuntimeError(f'short fio runtime {active}: {directory}')
    save(directory/'summary.json',item)
    log(f'DONE {mode}/{name}/{round_kind}-{index} wall={elapsed:.3f}s')
    return item

def run_workload(mode,name,mount,pid):
    directory=ROOT/'results'/mode/name
    if (directory/'complete.json').exists():
        log(f'already complete {mode}/{name}'); return
    for kind,n in [('warmup',2),('measure',3)]:
        for i in range(1,n+1): run_round(mode,name,mount,pid,kind,i)
    save(directory/'complete.json',{'complete_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())})

def seed_flat(mount,pid):
    directory=ROOT/'preparation/flat'; directory.mkdir(parents=True,exist_ok=True)
    if (directory/'complete.json').exists(): return
    for folder in ['large','small','writes']: (mount/folder).mkdir(exist_ok=True)
    hashes={k:v for k,v in json.loads((ROOT/'environment/corpus-sha256.json').read_text()).items() if k.startswith('large/')}; verified={}
    for key,expected in hashes.items():
        src=STATE/'source'/key; dst=mount/key
        with src.open('rb') as rd,dst.open('wb') as wr:
            shutil.copyfileobj(rd,wr,1024*1024); wr.flush(); os.fsync(wr.fileno())
        if key.startswith('large'): log(f'prefilled {key}')
    drain(mount,directory/'seed-drain')
    for key,expected in hashes.items():
        digest=hashlib.sha256()
        with (mount/key).open('rb') as stream:
            for block in iter(lambda:stream.read(1024*1024),b''): digest.update(block)
        verified[key]=digest.hexdigest()
        if verified[key]!=expected['sha256']: raise RuntimeError(f'content mismatch {key}')
    save(directory/'verified-sha256.json',verified)
    save(directory/'complete.json',{'verified_files':len(verified)})

def seed_small(mount):
    directory=ROOT/'preparation/flat-small';directory.mkdir(parents=True,exist_ok=True)
    if (directory/'complete.json').exists():return
    verified={}
    for src in sorted((STATE/'source/small').iterdir()):
        data=src.read_bytes();dst=mount/'small'/src.name
        with dst.open('wb') as wr:
            wr.write(data);wr.flush();os.fsync(wr.fileno())
        actual=dst.read_bytes()
        if actual!=data:raise RuntimeError(f'smallfile content mismatch {src.name}')
        verified[src.name]=hashlib.sha256(actual).hexdigest()
    drain(mount,directory/'drain')
    save(directory/'verified-sha256.json',verified)
    save(directory/'complete.json',{'verified_files':len(verified)})

def seed_writes(mount,name):
    directory=ROOT/'preparation'/name; directory.mkdir(parents=True,exist_ok=True)
    if (directory/'complete.json').exists(): return
    jobs=1 if name=='seqwrite' else 4
    for i in range(jobs):
        src=STATE/'source/large'/f'read.{i}'; dst=mount/'writes'/f'{name}.{i}'
        with src.open('rb') as rd,dst.open('wb') as wr:
            shutil.copyfileobj(rd,wr,1024*1024); wr.flush(); os.fsync(wr.fileno())
    drain(mount,directory/'drain')
    save(directory/'complete.json',{'jobs':jobs,'size_per_job':536870912})

def build_packed(profile):
    directory=ROOT/'preparation'/f'packed-{profile}'; directory.mkdir(parents=True,exist_ok=True)
    manifest=directory/'manifest.txt'
    if (directory/'complete.json').exists(): return manifest.read_text().strip()
    argv=[FIXTURE,'--source-directory',STATE/'source','--source-consistency','best-effort-detected',
          '--source-hardlink-policy','visible-links','--endpoint',ENDPOINT,'--bucket',BUCKET,
          '--region','us-east-1','--force-path-style','true','--prefix',f'packed-{profile}',
          '--manifest-output',manifest,'--access-profile',profile,'--data-codec','zstd','--metadata-codec','zstd']
    save(directory/'command.json',[str(x) for x in argv]); log(f'BUILD packed {profile}')
    command(argv,directory/'fixture.log',timeout=900)
    save(directory/'complete.json',{'manifest':manifest.read_text().strip()})
    return manifest.read_text().strip()

def verify_packed(mount,label,small_files=False):
    directory=ROOT/'preparation'/f'verify-{label}'; directory.mkdir(parents=True,exist_ok=True)
    results=[]
    buffer=mmap.mmap(-1,4096)
    offsets=[0,4096,262144-4096,262144,4194304-4096,4194304,8388608-4096,536870912-4096]
    for i in range(4):
        key=f'large/read.{i}'; fd=os.open(mount/key,os.O_RDONLY|os.O_DIRECT)
        try:
            with (STATE/'source'/key).open('rb') as source:
                for offset in offsets:
                    source.seek(offset); expected=source.read(4096)
                    n=os.preadv(fd,[buffer],offset)
                    if n!=4096 or buffer[:]!=expected: raise RuntimeError(f'packed content mismatch {key}@{offset}')
                    results.append({'path':key,'offset':offset,'bytes':n,'sha256':hashlib.sha256(buffer[:]).hexdigest()})
        finally: os.close(fd)
    buffer.close()
    small_digest=hashlib.sha256(); count=0
    if small_files:
        for source in sorted((STATE/'source/small').iterdir()):
            data=(mount/'small'/source.name).read_bytes()
            if data!=source.read_bytes():raise RuntimeError(f'packed smallfile mismatch {source.name}')
            small_digest.update(data);count+=1
    rejected={}
    for operation in ['create','rename']:
        try:
            if operation=='create':
                fd=os.open(mount/'readonly-probe',os.O_CREAT|os.O_WRONLY|os.O_EXCL,0o600);os.close(fd)
            else:os.rename(mount/'small/f00000',mount/'small/readonly-rename-probe')
        except OSError as error:
            if error.errno!=errno.EROFS:raise
            rejected[operation]=error.errno
        else:raise RuntimeError(f'packed mutation succeeded: {operation}')
    save(directory/'verification.json',{'direct_read_checks':results,'small_files_verified':count,
                                      'small_combined_sha256':small_digest.hexdigest(),'mutation_rejected_errno':rejected})
    drain(mount,directory/'drain')

def main():
    p=argparse.ArgumentParser(); p.add_argument('stage',choices=['flat-first','packed-first','flat-rest','packed-rest','prepare-packed'])
    a=p.parse_args()
    if os.geteuid()!=0: raise RuntimeError('run as root for FUSE and perf')
    if a.stage=='prepare-packed':
        build_packed('random-small-file'); build_packed('sequential-small-file'); return
    if a.stage.startswith('flat'):
        with mounted('flat-v1',a.stage) as (mount,pid,_):
            seed_flat(mount,pid)
            names=['randread','randwrite','seqread'] if a.stage=='flat-first' else ['seqwrite','randrw','metadata','smallfiles']
            for name in names:
                if name in ['randwrite','seqwrite','randrw']: seed_writes(mount,name)
                if name in ['metadata','smallfiles']:seed_small(mount)
                run_workload('flat-v1',name,mount,pid)
    elif a.stage=='packed-first':
        manifest=build_packed('random-small-file')
        with mounted('packed-v3','packed-random',manifest) as (mount,pid,_):
            verify_packed(mount,'random')
            run_workload('packed-v3','randread',mount,pid)
        manifest=build_packed('sequential-small-file')
        with mounted('packed-v3','packed-sequential',manifest) as (mount,pid,_):
            verify_packed(mount,'sequential')
            run_workload('packed-v3','seqread',mount,pid)
    elif a.stage=='packed-rest':
        manifest=build_packed('sequential-small-file')
        with mounted('packed-v3','packed-rest',manifest) as (mount,pid,_):
            verify_packed(mount,'rest',small_files=True)
            run_workload('packed-v3','metadata',mount,pid)
            run_workload('packed-v3','smallfiles',mount,pid)
    log(f'STAGE COMPLETE {a.stage}')

if __name__=='__main__':
    try: main()
    except BaseException:
        traceback.print_exc(); raise
