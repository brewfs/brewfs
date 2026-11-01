"""Own benchmark commands independently of SSH and record actual worker exit."""
from pathlib import Path
import hashlib,json,os,signal,subprocess,time,uuid
from common import ROOT,ART,atomic,read,stamp

RUN='million-r2'

class ManagedProcess:
    def __init__(self,args,*,env=None,stdout=None,stderr=None,**kwargs):
        assert not kwargs,kwargs
        assert stdout is not None and hasattr(stdout,'name')
        self.args=list(map(str,args));self.id=uuid.uuid4().hex
        self.unit=f'{RUN}-{self.id}.service';self.directory=ART/'components'/self.id
        self.directory.mkdir(parents=True)
        self.state=self.directory/'state.json'
        private=ROOT/'private';private.mkdir(mode=0o700,exist_ok=True)
        self.stop_request=private/(self.id+'.stop')
        cfg=private/(self.id+'.json')
        atomic(cfg,dict(command=self.args,env=dict(os.environ if env is None else env),
            state=str(self.state),stdout=str(stdout.name),stop_request=str(self.stop_request)),durable=True)
        cfg.chmod(0o600)
        atomic(self.directory/'identity.json',dict(unit=self.unit,command=self.args,
            command_sha256=hashlib.sha256(json.dumps(self.args).encode()).hexdigest(),**stamp()),durable=True)
        command=['systemd-run','--quiet','--no-block','--unit='+self.unit,
            '--service-type=exec','--property=Restart=no','--property=KillMode=process',
            '--property=SendSIGKILL=no','--property=TimeoutStartSec=infinity',
            '--property=TimeoutStopSec=infinity','--property=CPUAccounting=yes',
            '--property=MemoryAccounting=yes','--property=IOAccounting=yes',
            '--property=WorkingDirectory='+str(ROOT),
            '/usr/bin/python3',str(ROOT/'harness/supervise.py'),str(cfg)]
        subprocess.run(command,check=True,stdout=subprocess.DEVNULL)
        while not self.state.exists():
            failed=subprocess.check_output(['systemctl','show',self.unit,'--property=ActiveState','--value'],text=True).strip()
            if failed=='failed':raise RuntimeError('Component supervisor startup failed: '+self.unit)
            time.sleep(.1)
    @property
    def pid(self):
        s=read(self.state)
        return (s['live'][-1]['pid'] if s['live'] else s['primary_pid'])
    @property
    def returncode(self):return self.poll()
    def poll(self):
        s=read(self.state)
        if s['complete']:return s['returncode']
        if s['primary_returncode'] not in (None,0):return s['primary_returncode']
        return None
    def wait(self,timeout=None):
        # A caller's historical deadline is not a benchmark cancellation policy.
        while (value:=self.poll()) is None:time.sleep(.1)
        return value
    def send_signal(self,sig):
        assert sig==signal.SIGTERM,'Only explicit graceful termination is supported'
        self.stop_request.touch()
    def terminate(self):
        raise RuntimeError('Automatic failure cleanup is disabled; preserve the component for inspection')
    def kill(self):
        raise RuntimeError('No automatic SIGKILL is allowed')

def run(args,**kwargs):
    check=kwargs.pop('check',False);kwargs.pop('timeout',None)
    proc=ManagedProcess(args,**kwargs);code=proc.wait()
    if code and check:raise subprocess.CalledProcessError(code,args)
    return subprocess.CompletedProcess(args,code)
