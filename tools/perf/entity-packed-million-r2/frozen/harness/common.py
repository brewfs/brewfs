from pathlib import Path
import gzip,json,os,time,uuid

ROOT=Path(__file__).resolve().parent.parent
ART=ROOT/'artifacts'

def stamp():return dict(utc_ns=time.time_ns(),monotonic_ns=time.monotonic_ns())

def atomic(path,value,durable=False):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    with tmp.open('w') as f:
        json.dump(value,f,separators=(',',':'));f.write('\n');f.flush()
        if durable:os.fsync(f.fileno())
    os.replace(tmp,path)
    if durable:
        directory=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(directory)
        finally:os.close(directory)

def read(path,default=None):
    try:return json.loads(Path(path).read_text())
    except FileNotFoundError:return default

class Journal:
    """Append-only compressed evidence; rotation never removes old segments."""
    def __init__(self,directory,name):
        self.directory=Path(directory);self.directory.mkdir(parents=True,exist_ok=True)
        self.name=name;self.file=None;self.raw_size=0;self.path=None
    def append(self,value):
        if self.file is None or self.raw_size>=16<<20:
            self.close()
            self.path=self.directory/f'{self.name}-{time.time_ns()}-{uuid.uuid4().hex[:8]}.jsonl.gz.open'
            self.file=gzip.open(self.path,'wt');self.raw_size=0
        line=json.dumps(value,separators=(',',':'))+'\n'
        self.file.write(line);self.file.flush();self.raw_size+=len(line)
        os.fsync(self.file.fileno())
    def close(self):
        if self.file is not None:
            self.file.close();self.file=None
            with self.path.open('rb') as f:os.fsync(f.fileno())
            os.replace(self.path,self.path.with_suffix(''))
            directory=os.open(self.directory,os.O_RDONLY|os.O_DIRECTORY)
            try:os.fsync(directory)
            finally:os.close(directory)

def proc_start(pid):
    try:return int(Path(f'/proc/{pid}/stat').read_text().rsplit(')',1)[1].split()[19])
    except (FileNotFoundError,ProcessLookupError):return None
