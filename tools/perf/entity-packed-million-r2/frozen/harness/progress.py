"""Durable task progress and a read-only cost/health monitor; never a killer."""
from pathlib import Path
import argparse,json,signal,threading,time,urllib.request
from common import ROOT,ART,Journal,atomic,read,stamp

class Progress:
    def __init__(self,name,total,unit='operations',**details):
        self.lock=threading.Lock();self.last=0;self.done=0;self.failed=0
        self.path=ART/'progress'/f'{name}.json';self.started=stamp()
        self.value=dict(name=name,total=total,unit=unit,started=self.started,**details)
        self.flush()
    def advance(self,count=1,failed=False):
        with self.lock:
            self.done+=count;self.failed+=int(failed)
            if time.monotonic()-self.last>=1 or self.done==self.value['total']:self.flush()
    def flush(self):
        now=stamp();seconds=(now['monotonic_ns']-self.started['monotonic_ns'])/1e9
        self.value.update(done=self.done,failed=self.failed,updated=now,elapsed_s=seconds,
            rate_per_s=self.done/seconds if seconds else None,
            percent=100*self.done/self.value['total'] if self.value['total'] else None)
        atomic(self.path,self.value);self.last=time.monotonic()

def get(url):
    with urllib.request.urlopen(url,timeout=5) as reply:return json.load(reply)

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--interval',type=float,default=10)
    args=parser.parse_args();settings=read(ROOT/'harness/settings.json')
    journal=Journal(ART/'monitor','status');last_token=None;last_change=time.monotonic();alerted=False
    stop=threading.Event()
    signal.signal(signal.SIGTERM,lambda *_:stop.set())
    signal.signal(signal.SIGINT,lambda *_:stop.set())
    while not stop.is_set():
        phase=read(ROOT/'current-phase.json',{})
        local=[read(p) for p in (ART/'progress').glob('*.json')]
        agents={}
        for role,url in settings['agents'].items():
            try:agents[role]=dict(reachable=True,status=get(url+'/status'))
            except Exception as e:agents[role]=dict(reachable=False,error=repr(e))
        components=[read(p) for p in (ART/'components').glob('*/state.json')]
        native=agents.get('client',{}).get('status',{}).get('scrapes',{}).get('juicefs-sync',{})
        progress=native.get('progress',{})
        objects=agents.get('objects',{}).get('status',{}).get('objects',{}).get('counts',{})
        token=json.dumps(dict(phase=[phase.get('name'),phase.get('status')],
            local=[(p['name'],p['done']) for p in local],native=progress,
            objects=objects,exited=[p['complete'] for p in components]),sort_keys=True)
        if token!=last_token:last_token=token;last_change=time.monotonic();alerted=False
        stalled=time.monotonic()-last_change
        # This is an observation alarm, never a cancellation or deletion timer.
        if stalled>=900 and phase.get('status')=='running' and not alerted:
            atomic(ART/'attention-required.json',dict(reason='No reported progress for 15 minutes',
                phase=phase,action='Inspect processes and service metrics; preserve all state',**stamp()))
            alerted=True
        ledger=read(ROOT/'resource-ledger.json',{})
        estimated_base=0;estimated_fixed_iops=0
        for node in ledger.get('nodes',[]):
            end=node.get('released_utc_ns',time.time_ns())
            estimated_base+=max(0,end-node['created_utc_ns'])/3.6e12*node['confirmed_base_cny_per_h']
            estimated_fixed_iops+=max(0,end-node['created_utc_ns'])/3.6e12*node['confirmed_extra_iops_cny_per_h']
        summary=dict(**stamp(),phase=phase,local_progress=local,agents=agents,
            components_running=sum(not p['complete'] for p in components),
            native_import=progress,seconds_since_reported_progress=stalled,
            cost=dict(confirmed_configuration_estimate_cny=estimated_base+estimated_fixed_iops,
                compute_and_capacity_estimate_cny=estimated_base,extra_iops_estimate_cny=estimated_fixed_iops,
                excludes=['public traffic','billing adjustments'],
                actual_bill_verified=False),campaign=read(ART/'campaign-state.json'))
        atomic(ART/'live-status.json',summary);journal.append(summary)
        campaign=summary['campaign'] or {}
        lines=[f"Phase: {phase.get('name','starting')} [{phase.get('status','unknown')}]",
            f"Cells complete: {campaign.get('completed_cells',0)}/{campaign.get('expected_cells',6)}",
            f"Estimated configuration cost: CNY {estimated_base+estimated_fixed_iops:.2f}; public traffic excluded",
            f"Last reported progress: {stalled:.0f} seconds ago (alert only; never cancel)"]
        if progress:lines.append('JuiceFS native sync counters: '+json.dumps(progress,sort_keys=True))
        if objects:lines.append('Object requests: '+json.dumps(objects,sort_keys=True))
        current=sorted(local,key=lambda p:p['updated']['utc_ns'],reverse=True)[:4]
        for item in current:
            lines.append(f"{item['name']}: {item['done']}/{item['total']} {item['unit']}, "
                f"{item['rate_per_s']:.2f}/s, errors={item['failed']}")
        (ART/'live-progress.txt').write_text('\n'.join(lines)+'\n')
        stop.wait(args.interval)
    journal.close()

if __name__=='__main__':main()
