"""Per-node raw telemetry spool. Observation never terminates a workload."""
from pathlib import Path
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import argparse,json,os,re,signal,threading,time,urllib.request
from common import ROOT,ART,Journal,atomic,read,stamp

ALLOWED=('brewfs','juicefs','tikv-server','pd-server','rustfs','nginx',
         'packed_v3_','entity_metadat','python3')
FILES=['stat','meminfo','vmstat','diskstats','loadavg','net/dev','net/snmp','net/netstat',
       'pressure/cpu','pressure/io','pressure/memory','sys/fs/file-nr']

def text(path):
    try:return Path(path).read_text()
    except (FileNotFoundError,PermissionError,ProcessLookupError):return None

def host_snapshot():
    raw={name:text('/proc/'+name) for name in FILES}
    processes=[];groups={}
    for entry in Path('/proc').iterdir():
        if not entry.name.isdecimal():continue
        name=text(entry/'comm')
        if name is None or not name.strip().startswith(ALLOWED):continue
        group=text(entry/'cgroup')
        item=dict(pid=int(entry.name),name=name.strip(),stat=text(entry/'stat'),
            status=text(entry/'status'),io=text(entry/'io'),cgroup=group)
        # Do not collect process environments or full command lines: object
        # credentials can be present in either even on a synthetic test host.
        processes.append(item)
        if group and group.startswith('0::'):
            path=group.strip().split(':',2)[2]
            if path not in groups:
                base=Path('/sys/fs/cgroup')/path.lstrip('/')
                groups[path]={n:text(base/n) for n in ['cpu.stat','memory.current','memory.peak',
                    'memory.stat','memory.events','io.stat','pids.current','cpu.pressure','io.pressure']}
    capacity={}
    for location in [ROOT,Path('/data')]:
        if not location.exists():continue
        usage=os.statvfs(location)
        capacity[str(location)]=dict(total_bytes=usage.f_blocks*usage.f_frsize,
            free_bytes=usage.f_bavail*usage.f_frsize,total_inodes=usage.f_files,
            free_inodes=usage.f_favail)
    fuse={}
    base=Path('/sys/fs/fuse/connections')
    if base.exists():
        for connection in base.iterdir():
            fuse[connection.name]={n:text(connection/n) for n in ['waiting','max_background','congestion_threshold']}
    return dict(**stamp(),boot_id=text('/proc/sys/kernel/random/boot_id'),raw=raw,
        processes=processes,cgroups=groups,fuse=fuse,
        disk=capacity)

class Agent:
    def __init__(self,role,interval):
        self.role=role;self.interval=interval;self.lock=threading.RLock()
        self.stop=threading.Event()
        self.host=Journal(ART/'telemetry','host-'+role)
        self.prom=Journal(ART/'telemetry','prom-'+role)
        self.latest={};self.catalog={};self.scrapes={}
        self.object_position=0;self.object_partial=b'';self.object_counts={}
        self.checkpoints={}
    def checkpoint(self,label):
        assert re.fullmatch(r'[a-zA-Z0-9-]+',label)
        with self.lock:
            if label in self.checkpoints:return self.checkpoints[label]
            self.rotate();self.checkpoints[label]=dict(status='running',label=label,**stamp())
            def archive():
                import archive as evidence
                try:
                    result=evidence.create(label);validation=evidence.verify(result['path'])
                    self.checkpoints[label]=dict(status='complete',archive=result,validation=validation)
                except Exception as e:self.checkpoints[label]=dict(status='failed',error=repr(e))
            threading.Thread(target=archive).start()
            return self.checkpoints[label]
    def objects(self):
        with self.lock:
            path=ROOT/'work/object-requests.jsonl'
            if path.exists():
                with path.open('rb') as source:
                    source.seek(self.object_position);data=source.read()
                    self.object_position=source.tell()
                lines=(self.object_partial+data).split(b'\n');self.object_partial=lines.pop()
                for line in lines:
                    item=json.loads(line)
                    key=item['method']+'_'+str(item['status'])
                    total=self.object_counts.setdefault(key,dict(requests=0,response_bytes=0,
                        request_length_bytes=0,request_time_s=0))
                    total['requests']+=1;total['response_bytes']+=int(item['response_bytes'])
                    total['request_length_bytes']+=int(item['request_length'])
                    total['request_time_s']+=float(item['request_time'])
            return dict(counts=self.object_counts,**stamp())
    def sample(self,only_metrics=False):
        with self.lock:
            if not only_metrics:
                value=host_snapshot();self.host.append(value)
                self.latest={k:value[k] for k in ['utc_ns','monotonic_ns','disk']}
            targets=read(ROOT/'metrics-targets.json',[])
            for target in targets:
                if target.get('state') not in ['starting','active','stopping']:continue
                begin=stamp();record=dict(name=target['name'],state=target['state'],started=begin)
                try:
                    with urllib.request.urlopen(target['url'],timeout=3) as reply:
                        body=reply.read().decode();assert reply.status==200
                    names=set(re.findall(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)[ {]',body,re.M))
                    record.update(ok=True,body=body,bytes=len(body.encode()),ended=stamp())
                    self.catalog.setdefault(target['name'],set()).update(names)
                    progress={}
                    for line in body.splitlines():
                        if line.startswith('#'):continue
                        metric=line.split('{',1)[0].split(' ',1)[0]
                        if ('sync_' in metric and any(w in metric for w in ['copied','failed','pending','handled'])):
                            try:progress[metric]=progress.get(metric,0)+float(line.rsplit(' ',1)[1])
                            except ValueError:pass
                    self.scrapes[target['name']]=dict(ok=True,utc_ns=record['ended']['utc_ns'],
                        bytes=record['bytes'],progress=progress)
                except Exception as e:
                    record.update(ok=False,error=type(e).__name__+': '+str(e),ended=stamp())
                    self.scrapes[target['name']]=dict(ok=False,utc_ns=record['ended']['utc_ns'],
                        expected_startup=target['state']=='starting',
                        expected_shutdown=target['state']=='stopping',error=record['error'])
                self.prom.append(record)
            atomic(ART/'metrics-catalog.json',{k:sorted(v) for k,v in self.catalog.items()})
            summary=dict(role=self.role,updated=stamp(),host=self.latest,scrapes=self.scrapes,
                         phase=read(ROOT/'current-phase.json'))
            if self.role=='objects':summary['objects']=self.objects()
            atomic(ART/'agent-status.json',summary)
            return summary
    def loop(self):
        while not self.stop.is_set():
            start=time.monotonic()
            try:self.sample()
            except Exception as e:atomic(ART/'agent-error.json',dict(error=repr(e),**stamp()))
            self.stop.wait(max(.05,self.interval-(time.monotonic()-start)))
    def rotate(self):
        with self.lock:
            self.host.close();self.prom.close()
            master=ROOT/'work/nginx.pid';active=ROOT/'work/object-requests.jsonl'
            if self.role=='objects' and master.exists() and active.exists():
                pid=int(master.read_text())
                assert Path(f'/proc/{pid}/exe').resolve().name=='nginx'
                assert str(ROOT/'work').encode() in Path(f'/proc/{pid}/cmdline').read_bytes()
                directory=ART/'object-log';directory.mkdir(exist_ok=True)
                self.objects()
                os.replace(active,directory/f'objects-{time.time_ns()}.jsonl')
                os.kill(pid,signal.SIGUSR1)
                # Keep the renamed descriptor until nginx acknowledges reopening.
                # No request is active at a checkpoint, so its final bytes have
                # already been included; rotation is only allowed between cells.
                self.object_position=0;self.object_partial=b''
            return dict(rotated=True,**stamp())

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--role',required=True)
    parser.add_argument('--bind',default='0.0.0.0');parser.add_argument('--port',type=int,default=9910)
    parser.add_argument('--interval',type=float,default=5)
    args=parser.parse_args();ART.mkdir(parents=True,exist_ok=True)
    agent=Agent(args.role,args.interval)
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*_):pass
        def do_GET(self):
            if self.path.startswith('/checkpoint/'):
                self.reply(agent.checkpoints.get(self.path.rsplit('/',1)[-1],dict(status='missing')));return
            if self.path=='/objects' and args.role=='objects':
                self.reply(agent.objects());return
            if self.path not in ['/health','/status']:
                self.send_error(404);return
            self.reply(read(ART/'agent-status.json',dict(role=args.role,starting=True)))
        def do_POST(self):
            if self.path=='/scrape':self.reply(agent.sample(only_metrics=True))
            elif self.path=='/rotate':self.reply(agent.rotate())
            elif self.path=='/checkpoint':
                value=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                self.reply(agent.checkpoint(value['label']))
            elif self.path=='/phase':
                value=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                atomic(ROOT/'current-phase.json',value)
                self.reply(dict(received=value,**stamp()))
            else:self.send_error(404)
        def reply(self,value):
            data=json.dumps(value,separators=(',',':')).encode()
            self.send_response(200);self.send_header('Content-Type','application/json')
            self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
    worker=threading.Thread(target=agent.loop);worker.start()
    server=ThreadingHTTPServer((args.bind,args.port),Handler)
    def stop(*_):
        agent.stop.set()
        threading.Thread(target=server.shutdown,daemon=True).start()
    signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
    try:server.serve_forever()
    finally:
        agent.stop.set();worker.join();agent.rotate();server.server_close()

if __name__=='__main__':main()
