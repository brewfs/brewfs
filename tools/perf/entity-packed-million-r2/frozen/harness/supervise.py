"""Linux subreaper: retain real worker exits, including daemon/re-exec children.

There is deliberately no execution deadline or automatic data cleanup.
"""
from pathlib import Path
import ctypes,json,os,signal,subprocess,sys,time
from common import atomic,proc_start,stamp

def main(config_path):
    cfg=json.loads(Path(config_path).read_text());out=Path(cfg['state'])
    assert ctypes.CDLL(None,use_errno=True).prctl(36,1,0,0,0)==0,'PR_SET_CHILD_SUBREAPER'
    stop=False
    def request_stop(*_):
        nonlocal stop
        stop=True
    signal.signal(signal.SIGTERM,request_stop)
    signal.signal(signal.SIGINT,request_stop)
    with open(cfg['stdout'],'ab',buffering=0) as log:
        child=subprocess.Popen(cfg['command'],env=cfg['env'],stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        events=[];started=stamp();sent=set();primary_code=None
        own_group=Path('/proc/self/cgroup').read_text().strip().split(':',2)[-1]
        while True:
            while True:
                try:pid,status=os.waitpid(-1,os.WNOHANG)
                except ChildProcessError:pid=-1
                if pid<=0:break
                code=os.waitstatus_to_exitcode(status)
                events.append(dict(pid=pid,returncode=code,**stamp()))
                if pid==child.pid:primary_code=code;child.returncode=code
            live=[]
            # Ownership is the component's dedicated systemd cgroup. Never
            # infer signal targets from a host-wide executable-name match.
            try:pids=(Path('/sys/fs/cgroup')/own_group.lstrip('/')/'cgroup.procs').read_text().split()
            except FileNotFoundError:pids=[]
            for raw in pids:
                candidate=int(raw)
                if candidate==os.getpid():continue
                tick=proc_start(candidate)
                if tick is not None:live.append(dict(pid=candidate,start_ticks=tick))
            if Path(cfg['stop_request']).exists():stop=True
            if stop:
                for item in live:
                    key=(item['pid'],item['start_ticks'])
                    if key not in sent and proc_start(item['pid'])==item['start_ticks']:
                        try:os.kill(item['pid'],signal.SIGTERM);sent.add(key)
                        except ProcessLookupError:pass
            complete=pid==-1 and not live
            failures=[e['returncode'] for e in events if e['returncode']!=0]
            state=dict(started=started,updated=stamp(),supervisor_pid=os.getpid(),primary_pid=child.pid,
                primary_returncode=primary_code,live=live,exits=events,complete=complete,
                returncode=(failures[0] if failures else 0) if complete else None,
                cgroup=own_group,stop_requested=stop)
            atomic(out,state)
            if complete:
                atomic(out,state,durable=True)
                return 0 if not failures else 1
            time.sleep(.25)

if __name__=='__main__':raise SystemExit(main(sys.argv[1]))
