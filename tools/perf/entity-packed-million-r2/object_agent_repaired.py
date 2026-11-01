"""Observation-only adapter; preserve the frozen agent and request counters."""
from pathlib import Path
import hashlib,json,os,signal,sys,time,uuid

def copy_closed(source,destination):
    """Copy a closed immutable segment across mounts without discarding it."""
    source=Path(source);destination=Path(destination)
    assert source.is_file() and not source.is_symlink() and not destination.exists()
    before=source.stat();temporary=destination.with_name(destination.name+'.'+uuid.uuid4().hex+'.tmp')
    digest=hashlib.sha256()
    with source.open('rb') as reader,temporary.open('xb') as writer:
        while data:=reader.read(1<<20):writer.write(data);digest.update(data)
        writer.flush();os.fsync(writer.fileno())
    after=source.stat()
    assert (before.st_ino,before.st_size,before.st_mtime_ns)==(after.st_ino,after.st_size,after.st_mtime_ns)
    with temporary.open('rb') as check:actual=hashlib.file_digest(check,'sha256').hexdigest()
    assert temporary.stat().st_size==before.st_size and actual==digest.hexdigest()
    os.replace(temporary,destination)
    fd=os.open(destination.parent,os.O_RDONLY|os.O_DIRECTORY)
    try:os.fsync(fd)
    finally:os.close(fd)
    return dict(bytes=before.st_size,sha256=actual,source_retained=True)

def open_by_nginx(pid,identity):
    pending=[pid];seen=set()
    while pending:
        current=pending.pop()
        if current in seen:continue
        seen.add(current)
        try:
            pending.extend(int(x) for x in Path(f'/proc/{current}/task/{current}/children').read_text().split())
            for fd in Path(f'/proc/{current}/fd').iterdir():
                try:
                    stat=fd.stat()
                    if (stat.st_dev,stat.st_ino)==identity:return True
                except FileNotFoundError:pass
        except FileNotFoundError:pass
    return False

def install():
    root=Path(__file__).resolve().parent;sys.path.insert(0,str(root/'harness'))
    import node_agent as agent
    from common import ART,ROOT,atomic,read,stamp
    assert root==ROOT
    original_init=agent.Agent.__init__
    def initialize(self,role,interval):
        original_init(self,role,interval)
        if role!='objects':return
        baseline=read(ART/'checkpoint-recovery/counter-continuity.json')
        active=ROOT/'work/object-requests.jsonl'
        assert baseline['active_bytes']==active.stat().st_size
        self.object_counts=baseline['counts'];self.object_position=baseline['active_bytes']
        self.catalog={k:set(v) for k,v in read(ART/'metrics-catalog.json',{}).items()}
        # Existing checkpoint identities remain immutable across collector restarts.
        for path in (ROOT/'work/checkpoints').glob('*.json'):
            value=read(path)
            if value.get('label') and value.get('sha256'):
                self.checkpoints[value['label']]=dict(status='complete',archive=value,
                    validation=dict(verified=True,restored_identity=True))
        atomic(ART/'checkpoint-recovery/collector-restored.json',dict(
            preserved_counts=self.object_counts,active_bytes=self.object_position,**stamp()),durable=True)
    def rotate(self):
        with self.lock:
            self.host.close();self.prom.close()
            master=ROOT/'work/nginx.pid';active=ROOT/'work/object-requests.jsonl'
            if self.role=='objects' and master.exists() and active.exists():
                pid=int(master.read_text());assert Path(f'/proc/{pid}/exe').resolve().name=='nginx'
                assert str(ROOT/'work').encode() in Path(f'/proc/{pid}/cmdline').read_bytes()
                self.objects();assert not self.object_partial,'Checkpoint requires complete request rows'
                stat=active.stat();identity=(stat.st_dev,stat.st_ino)
                closed=active.with_name(f'object-requests-{time.time_ns()}.closed')
                os.replace(active,closed)  # Same directory AND same mount.
                os.kill(pid,signal.SIGUSR1)
                # No execution/ack deadline. Keep the original closed segment
                # even on failure; never truncate a live descriptor.
                while True:
                    assert Path(f'/proc/{pid}').exists(),'Proxy exited during reopen; log preserved'
                    if active.exists() and active.stat().st_ino!=stat.st_ino and not open_by_nginx(pid,identity):break
                    time.sleep(.05)
                assert closed.stat().st_size==stat.st_size,'Requests were active at checkpoint'
                directory=ART/'object-log';directory.mkdir(exist_ok=True)
                proof=copy_closed(closed,directory/(closed.stem+'.jsonl'))
                self.object_position=0;self.object_partial=b''
                atomic(ART/'checkpoint-recovery/last-rotation.json',dict(
                    closed=str(closed),destination=str(directory/(closed.stem+'.jsonl')),
                    counts=self.object_counts,**proof,**stamp()),durable=True)
            return dict(rotated=True,cross_mount_copy=True,**stamp())
    agent.Agent.__init__=initialize;agent.Agent.rotate=rotate
    agent.main()

if __name__=='__main__':install()
