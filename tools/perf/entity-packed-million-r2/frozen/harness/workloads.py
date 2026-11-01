"""Deterministic direct-I/O traces shared by both filesystem clients."""
import concurrent.futures
import hashlib
import json
import mmap
import os
from pathlib import Path
import random
import time

SMALL_FILES = 1000
SMALL_BYTES = 16384
SMALL_DIRS = 64
LARGE_FILES = 16
LARGE_BYTES = 8 << 20


def payload(index, size, offset=0):
    assert offset % 4096 == 0
    return b''.join(hashlib.sha256(f'entity-tikv-seed:{index}:{sector}'.encode()).digest()*128
                    for sector in range(offset//4096,(offset+size+4095)//4096))[:size]


def corpus(root):
    root.mkdir(parents=True)
    for kind, count, size, base in [('small', SMALL_FILES, SMALL_BYTES, 0), ('large', LARGE_FILES, LARGE_BYTES, 10000)]:
        directory = root / kind
        directory.mkdir()
        if kind=='small':
            for d in range(SMALL_DIRS):(directory/f'd{d:03d}').mkdir()
        for i in range(count):
            path=small_path(directory,i) if kind=='small' else directory/f'f{i:05d}'
            path.write_bytes(payload(base + i, size))


def small_path(directory,index):return directory/f'd{index%SMALL_DIRS:03d}'/f'f{index:05d}'


def percentile(values, pct):
    values = sorted(values)
    return values[min(len(values)-1, round((len(values)-1)*pct/100))]


def summarize(times, size, wall):
    return dict(operations=len(times), user_bytes=len(times)*size, active_s=wall,
                active_iops=len(times)/wall, active_mib_s=len(times)*size/wall/(1<<20),
                p50_ms=percentile(times,50), p95_ms=percentile(times,95), p99_ms=percentile(times,99))


def read_trace(kind, half, ops=4096):
    count = SMALL_FILES if kind == 'small' else LARGE_FILES
    size = SMALL_BYTES if kind == 'small' else LARGE_BYTES
    rng = random.Random(42)
    if not half:
        indices = list(range(count)); rng.shuffle(indices)
        return [(i, 0, SMALL_BYTES) for i in indices]
    # An identical, exactly balanced initial-warm/unwarmed trace. Each worker
    # sees the whole working set; fast files cannot dominate through per-file jobs.
    indices = [rng.randrange(count//2) + (n%2)*(count//2) for n in range(ops)]
    rng.shuffle(indices)
    return [(i, rng.randrange(size//4096)*4096, 4096) for i in indices]


def direct_read(path, offset, size, expected):
    begin = time.perf_counter()
    fd = os.open(path, os.O_RDONLY | os.O_DIRECT)
    try:
        with mmap.mmap(-1, size) as buffer:
            assert os.preadv(fd, [buffer], offset) == size
            assert buffer[:] == expected
    finally:
        os.close(fd)
    return (time.perf_counter()-begin)*1000


def run_reads(root, kind, half, workers, art, before_active):
    count, size, base = (SMALL_FILES, SMALL_BYTES, 0) if kind == 'small' else (LARGE_FILES,LARGE_BYTES,10000)
    trace = read_trace(kind,half)
    text = json.dumps(trace,separators=(',',':')).encode()
    (art/'trace.json').write_bytes(text)
    prewarm_s = 0
    if half:
        begin=time.perf_counter()
        def prewarm(i):
            path=small_path(root/kind,i) if kind=='small' else root/kind/f'f{i:05d}'
            return direct_read(path,0,size,payload(base+i,size))
        with concurrent.futures.ThreadPoolExecutor(workers) as pool:
            list(pool.map(prewarm,range(count//2)))
        prewarm_s=time.perf_counter()-begin
    before_active()
    begin=time.perf_counter()
    def one(item):
        i,offset,n=item
        path=small_path(root/kind,i) if kind=='small' else root/kind/f'f{i:05d}'
        return direct_read(path,offset,n,payload(base+i,n,offset))
    with concurrent.futures.ThreadPoolExecutor(workers) as pool:
        times=list(pool.map(one,trace))
    result=summarize(times,trace[0][2],time.perf_counter()-begin)
    result.update(trace_sha256=hashlib.sha256(text).hexdigest(), prewarm_s=prewarm_s,
                  prewarm_files=count//2 if half else 0, prewarm_bytes=count//2*size if half else 0,
                  initial_warm_request_fraction=0.5 if half else 0,
                  cache_hit_rate=None, request_semantics='open+O_DIRECT pread+close; data verified')
    return result


def prepare_writes(root,kind):
    directory=root/'created';directory.mkdir()
    if kind=='small-write':
        for d in range(SMALL_DIRS):(directory/f'd{d:03d}').mkdir()


def write_files(root, kind, workers):
    count,size=(SMALL_FILES,SMALL_BYTES) if kind=='small-write' else (LARGE_FILES,LARGE_BYTES)
    directory=root/'created'
    begin=time.perf_counter()
    def one(i):
        data=payload(i,size)
        start=time.perf_counter()
        path=small_path(directory,i) if kind=='small-write' else directory/f'f{i:05d}'
        fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_RDWR|os.O_DIRECT,0o644)
        try:
            with mmap.mmap(-1, min(size,1<<20)) as buffer:
                for offset in range(0,size,len(buffer)):
                    buffer[:]=data[offset:offset+len(buffer)]
                    assert os.write(fd,buffer)==len(buffer)
            sync_started=time.perf_counter()
            os.fsync(fd)
            sync_ms=(time.perf_counter()-sync_started)*1000
        finally: os.close(fd)
        return (time.perf_counter()-start)*1000,sync_ms
    with concurrent.futures.ThreadPoolExecutor(workers) as pool:
        times=list(pool.map(one,range(count)))
    result=summarize([t for t,_ in times],size,time.perf_counter()-begin)
    result.update(fsync_p50_ms=percentile([s for _,s in times],50),fsync_p95_ms=percentile([s for _,s in times],95))
    return result


def cow_files(root, workers):
    begin=time.perf_counter()
    def one(i):
        start=time.perf_counter()
        fd=os.open(small_path(root/'small',i),os.O_RDWR|os.O_DIRECT)
        try:
            with mmap.mmap(-1,4096) as buf:
                buf[:]=payload(i+20000,4096)
                assert os.pwrite(fd,buf,4096)==4096
            sync_started=time.perf_counter()
            os.fsync(fd)
            sync_ms=(time.perf_counter()-sync_started)*1000
        finally: os.close(fd)
        return (time.perf_counter()-start)*1000,sync_ms
    with concurrent.futures.ThreadPoolExecutor(workers) as pool: times=list(pool.map(one,range(100)))
    result=summarize([t for t,_ in times],4096,time.perf_counter()-begin)
    result.update(fsync_p50_ms=percentile([s for _,s in times],50),fsync_p95_ms=percentile([s for _,s in times],95))
    return result
