"""Phase boundaries, metric coverage and nondestructive lifecycle evidence."""
from contextlib import contextmanager
from pathlib import Path
import json,time,traceback,urllib.request
from common import ROOT,ART,Journal,atomic,read,stamp

EVENTS=Journal(ART/'timeline','phases')
TARGETS=ROOT/'metrics-targets.json'

def request(url,value=None):
    data=None if value is None else json.dumps(value).encode()
    with urllib.request.urlopen(urllib.request.Request(url,data=data,
            headers={'Content-Type':'application/json'}),timeout=15) as response:
        return json.load(response)

def target(name,url,state):
    rows=[r for r in read(TARGETS,[]) if r['name']!=name]
    rows.append(dict(name=name,url=url,state=state,**stamp()));atomic(TARGETS,rows)

def metric_ready(name,url,required,proc):
    target(name,url,'starting')
    while True:
        if proc.poll() is not None:raise RuntimeError('Process exited before required exporter: '+name)
        try:
            with urllib.request.urlopen(url,timeout=3) as response:body=response.read().decode()
            if all(any(line.startswith(n) for line in body.splitlines()) for n in required):
                target(name,url,'active');return
        except OSError:pass
        time.sleep(.1)

@contextmanager
def stage(name,**details):
    start=stamp();value=dict(name=name,status='running',start=start,**details)
    atomic(ROOT/'current-phase.json',value);EVENTS.append(value)
    try:
        settings=read(ROOT/'harness/settings.json',{})
        receipts={role:request(url+'/phase',value) for role,url in settings.get('agents',{}).items()}
        value['node_phase_receipts']=receipts
        value['entry_observation_s']=(time.monotonic_ns()-start['monotonic_ns'])/1e9
        yield value
    except BaseException:
        value.update(status='failed-preserved',error=traceback.format_exc(),end=stamp())
        value['elapsed_s']=(value['end']['monotonic_ns']-start['monotonic_ns'])/1e9
        EVENTS.append(value);atomic(ART/'failure.json',value,durable=True)
        atomic(ROOT/'current-phase.json',value,durable=True);raise
    else:
        value.update(status='complete',end=stamp())
        value['elapsed_s']=(value['end']['monotonic_ns']-start['monotonic_ns'])/1e9
        EVENTS.append(value);atomic(ROOT/'current-phase.json',value)

def checkpoint(label,settings):
    # Closed files and a node-local immutable archive survive controller loss.
    # There is no local acknowledgement requirement, overwrite or timed delete.
    with stage('checkpoint',label=label):
        EVENTS.close()
        for agent in settings['agents'].values():request(agent+'/checkpoint',dict(label=label))
        completed={}
        while len(completed)<len(settings['agents']):
            for role,agent in settings['agents'].items():
                if role in completed:continue
                value=request(agent+'/checkpoint/'+label)
                if value['status']=='failed':raise RuntimeError('Archive failed on '+role+': '+value['error'])
                if value['status']=='complete':completed[role]=value
            if len(completed)<len(settings['agents']):time.sleep(.5)
        atomic(ART/'checkpoint-ready.json',dict(label=label,nodes=completed,**stamp()),durable=True)

