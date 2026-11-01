"""Deterministic scale workloads. Timings are consumed only by cloud runners."""
from pathlib import Path
import concurrent.futures
import hashlib
import json
import itertools
import mmap
import os
import random
import time

import workloads as wl

FILE_BYTES=4096
DIRECTORIES=1024
WORKERS=8

def bounded_map(pool,fn,values):
    values=iter(values)
    while chunk:=tuple(itertools.islice(values,512)):
        yield from pool.map(fn,chunk)

def namespace_order(count):
    for directory in range(DIRECTORIES):
        yield from range(directory,count,DIRECTORIES)

def path(root,index):
    return root/'small'/f'd{index%DIRECTORIES:04d}'/f'f{index:07d}'

def create_source(root,count):
    assert count in (100000,1000000) and not root.exists()
    (root/'small').mkdir(parents=True)
    for directory in range(DIRECTORIES):
        (root/'small'/f'd{directory:04d}').mkdir()
    def one(index):
        target=path(root,index)
        target.write_bytes(wl.payload(index,FILE_BYTES))
    with concurrent.futures.ThreadPoolExecutor(WORKERS) as pool:
        for _ in bounded_map(pool,one,range(count)): pass
    descriptor=dict(files=count,file_bytes=FILE_BYTES,directories=DIRECTORIES,
        content='SHA256(entity-tikv-seed:index:sector), repeated to 4 KiB; codecs raw')
    (root.parent/(root.name+'-descriptor.json')).write_text(json.dumps(descriptor,indent=2)+'\n')
    return descriptor

def working_set(count,limit):
    return random.Random(9173).sample(range(count),min(count,limit))

def read_trace(root,count,working_files=8192,operations=16384):
    chosen=working_set(count,working_files)
    half=len(chosen)//2
    rng=random.Random(19283)
    trace=[chosen[rng.randrange(half)+(n%2)*half] for n in range(operations)]
    rng.shuffle(trace)
    begin=time.perf_counter()
    def read(index):
        return wl.direct_read(path(root,index),0,FILE_BYTES,wl.payload(index,FILE_BYTES))
    with concurrent.futures.ThreadPoolExecutor(WORKERS) as pool:
        list(pool.map(read,chosen[:half]))
    prewarm=time.perf_counter()-begin
    begin=time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(WORKERS) as pool:
        timings=list(pool.map(read,trace))
    return dict(prewarm_s=prewarm,active_s=time.perf_counter()-begin,
        p95_ms=wl.percentile(timings,95),operations=operations,user_bytes=operations*FILE_BYTES,
        prewarm_files=half,working_files=len(chosen),namespace_files=count,
        trace_sha256=hashlib.sha256(json.dumps(trace,separators=(',',':')).encode()).hexdigest(),
        semantics='Half the selected working set pre-read; half of requests to each initial half; no cache-hit claim')

def full_scan(root,count):
    begin=time.perf_counter()
    def one(index):
        return wl.direct_read(path(root,index),0,FILE_BYTES,wl.payload(index,FILE_BYTES))
    with concurrent.futures.ThreadPoolExecutor(WORKERS) as pool:
        # Enumerate the mounted namespace rather than assuming a precomputed
        # name list. Only one directory (at most 977 files) is materialized.
        with os.scandir(root/'small') as scan:
            directories=sorted(entry.name for entry in scan)
        assert directories==[f'd{n:04d}' for n in range(DIRECTORIES)]
        found=0
        for directory in range(DIRECTORIES):
            with os.scandir(root/'small'/f'd{directory:04d}') as scan:
                names=sorted(entry.name for entry in scan)
            expected=range(directory,count,DIRECTORIES)
            assert names==[f'f{index:07d}' for index in expected]
            for _ in bounded_map(pool,one,expected):found+=1
        assert found==count
    return dict(active_s=time.perf_counter()-begin,operations=count,user_bytes=count*FILE_BYTES,
        semantics='Actual sorted readdir; every file opened, fully read with O_DIRECT, checked byte-for-byte, closed')

def cow(root,count,workspace):
    chosen=working_set(count,100)
    begin=time.perf_counter()
    def one(index):
        location=path(root,index)
        expected=wl.payload(index,FILE_BYTES)
        wl.direct_read(location,0,FILE_BYTES,expected)
        data=wl.payload(index+(workspace+1)*10000000,FILE_BYTES)
        fd=os.open(location,os.O_RDWR|os.O_DIRECT)
        try:
            with mmap.mmap(-1,FILE_BYTES) as buffer:
                buffer[:]=data
                assert os.pwrite(fd,buffer,0)==len(data)
            os.fsync(fd)
        finally: os.close(fd)
        wl.direct_read(location,0,FILE_BYTES,data)
    with concurrent.futures.ThreadPoolExecutor(WORKERS) as pool:
        list(pool.map(one,chosen))
    return dict(active_s=time.perf_counter()-begin,files=len(chosen),user_bytes=len(chosen)*FILE_BYTES,
        modified_indices=chosen,workspace=workspace)

def verify_source_sample(root,count):
    for index in working_set(count,100):
        wl.direct_read(path(root,index),0,FILE_BYTES,wl.payload(index,FILE_BYTES))
