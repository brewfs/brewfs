"""Same deterministic file selection and balanced trace, with raw observations."""
from pathlib import Path
import concurrent.futures,hashlib,itertools,json,mmap,os,random,time
import workloads as wl
from common import atomic,stamp
from progress import Progress

FILE_BYTES=4096
DIRECTORIES=1024
WORKERS=8

def path(root,index):return root/'small'/f'd{index%DIRECTORIES:04d}'/f'f{index:07d}'
def working_set(count,limit):return random.Random(9173).sample(range(count),min(count,limit))
def bounded_map(pool,fn,values):
    iterator=iter(values)
    while batch:=tuple(itertools.islice(iterator,512)):yield from pool.map(fn,batch)

def summary(values):
    return {**{f'p{p}_ms':wl.percentile(values,p) for p in [50,90,95,99]},
        'max_ms':max(values),'samples':len(values)}

def create_source(root,count):
    assert not root.exists();(root/'small').mkdir(parents=True)
    for directory in range(DIRECTORIES):(root/'small'/f'd{directory:04d}').mkdir()
    progress=Progress('source-generation',count,'files')
    def one(index):
        path(root,index).write_bytes(wl.payload(index,FILE_BYTES));progress.advance()
    with concurrent.futures.ThreadPoolExecutor(WORKERS) as pool:
        for _ in bounded_map(pool,one,range(count)):pass
    return dict(files=count,file_bytes=FILE_BYTES,directories=DIRECTORIES,
        content='SHA256(entity-tikv-seed:index:sector), repeated to 4 KiB; codecs raw')

def read_trace(root,count,art,label,working_files=1024,operations=4096):
    chosen=working_set(count,working_files);half=len(chosen)//2
    rng=random.Random(19283)
    trace=[chosen[rng.randrange(half)+(n%2)*half] for n in range(operations)];rng.shuffle(trace)
    trace_bytes=json.dumps(trace,separators=(',',':')).encode()
    (art/(label+'-trace.json')).write_bytes(trace_bytes)
    initial_warm=set(chosen[:half]);rows=[]
    def execute(indices,name):
        progress=Progress(label+'-'+name,len(indices))
        def one(item):
            order,index=item;start=stamp()
            try:
                elapsed=wl.direct_read(path(root,index),0,FILE_BYTES,wl.payload(index,FILE_BYTES))
                value=dict(order=order,index=index,start=start,latency_ms=elapsed,
                    initial_warm=index in initial_warm,checked_bytes=FILE_BYTES,ok=True)
            except BaseException as e:
                rows.append(dict(order=order,index=index,start=start,ok=False,error=repr(e)))
                progress.advance(failed=True);raise
            progress.advance();return value
        start=time.perf_counter()
        values=[]
        def recorded(item):
            result=one(item);values.append(result);return result
        try:
            with concurrent.futures.ThreadPoolExecutor(WORKERS) as pool:list(pool.map(recorded,enumerate(indices)))
            wall=time.perf_counter()-start
        finally:atomic(art/(label+'-'+name+'-samples.json'),sorted(values,key=lambda r:r['order']))
        return wall,values
    try:
        prewarm,warm=execute(chosen[:half],'prewarm');active,samples=execute(trace,'random-read')
    except BaseException:
        atomic(art/(label+'-read-errors.json'),rows);raise
    return dict(prewarm_s=prewarm,active_s=active,**summary([r['latency_ms'] for r in samples]),
        operations=operations,user_bytes=operations*FILE_BYTES,prewarm_files=half,working_files=len(chosen),
        namespace_files=count,trace_sha256=hashlib.sha256(trace_bytes).hexdigest(),
        initial_warm=summary([r['latency_ms'] for r in samples if r['initial_warm']]),
        initial_unwarmed=summary([r['latency_ms'] for r in samples if not r['initial_warm']]),
        semantics='Half the selected set pre-read, balanced initial warm/unwarmed requests; no cache-hit claim')

def cow(root,count,workspace,art,label):
    chosen=working_set(count,100);progress=Progress(label+'-cow',len(chosen),'files');samples=[]
    begin=time.perf_counter()
    def one(index):
        item=dict(index=index,start=stamp());start=time.perf_counter()
        try:
            location=path(root,index)
            item['before_read_ms']=wl.direct_read(location,0,FILE_BYTES,wl.payload(index,FILE_BYTES))
            data=wl.payload(index+(workspace+1)*10000000,FILE_BYTES)
            before=time.perf_counter();fd=os.open(location,os.O_RDWR|os.O_DIRECT)
            item['open_ms']=(time.perf_counter()-before)*1000
            try:
                with mmap.mmap(-1,FILE_BYTES) as buffer:
                    buffer[:]=data;before=time.perf_counter()
                    assert os.pwrite(fd,buffer,0)==len(data)
                    item['pwrite_ms']=(time.perf_counter()-before)*1000
                before=time.perf_counter();os.fsync(fd);item['fsync_ms']=(time.perf_counter()-before)*1000
            finally:
                before=time.perf_counter();os.close(fd);item['close_ms']=(time.perf_counter()-before)*1000
            item['after_read_ms']=wl.direct_read(location,0,FILE_BYTES,data)
            item.update(ok=True,total_ms=(time.perf_counter()-start)*1000)
            samples.append(item);progress.advance();return item
        except BaseException as e:
            item.update(ok=False,error=repr(e));samples.append(item);progress.advance(failed=True);raise
    try:
        with concurrent.futures.ThreadPoolExecutor(WORKERS) as pool:list(pool.map(one,chosen))
    finally:atomic(art/(label+'-cow-samples.json'),samples)
    return dict(active_s=time.perf_counter()-begin,files=len(chosen),user_bytes=len(chosen)*FILE_BYTES,
        modified_indices=chosen,workspace=workspace,
        latency={key:summary([r[key] for r in samples]) for key in
            ['before_read_ms','open_ms','pwrite_ms','fsync_ms','close_ms','after_read_ms','total_ms']})

def verify_source_sample(root,count):
    for index in working_set(count,100):wl.direct_read(path(root,index),0,FILE_BYTES,wl.payload(index,FILE_BYTES))
