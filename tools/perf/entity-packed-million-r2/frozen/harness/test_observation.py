"""Local correctness tests for progress, evidence and real descendant lifetime."""
from pathlib import Path
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import gzip,json,os,subprocess,sys,threading,time,unittest,uuid
from common import ROOT,ART,Journal,read
from managed import ManagedProcess
from progress import Progress
from node_agent import Agent,host_snapshot

class ObservationTests(unittest.TestCase):
    def test_progress_raw_counts_and_errors(self):
        progress=Progress('unit-'+uuid.uuid4().hex,10,'files')
        progress.advance(3);progress.advance(7,failed=True)
        value=read(progress.path)
        self.assertEqual((value['done'],value['total'],value['failed'],value['percent']),(10,10,1,100))
        self.assertGreater(value['rate_per_s'],0)
    def test_closed_journal_round_trip(self):
        directory=ART/('test-'+uuid.uuid4().hex);journal=Journal(directory,'evidence')
        journal.append(dict(n=1));journal.close();journal.append(dict(n=2));journal.close()
        self.assertEqual([json.loads(gzip.open(p,'rt').read())['n'] for p in sorted(directory.glob('*.gz'))],[1,2])
        self.assertFalse(list(directory.glob('*.open')))
    def test_capacity_contains_inodes(self):
        value=host_snapshot()['disk'][str(ROOT)]
        self.assertGreater(value['total_inodes'],0);self.assertGreater(value['free_bytes'],0)
    def test_missing_metrics_are_not_zero(self):
        agent=Agent('unit',1)
        from common import atomic
        target=ROOT/'metrics-targets.json';old=target.read_bytes() if target.exists() else None
        try:
            atomic(target,[dict(name='missing',state='active',url='http://127.0.0.1:1/metrics')])
            value=agent.sample(only_metrics=True)['scrapes']['missing']
            self.assertFalse(value['ok']);self.assertNotIn('progress',value)
            self.assertFalse(value['expected_startup'])
        finally:
            if old is not None:target.write_bytes(old)
            else:target.unlink()
            agent.rotate()
    def test_real_detached_worker_exit_and_no_caller_timeout(self):
        code='''import os,time
if os.fork():raise SystemExit(0)
os.setsid()
if os.fork():raise SystemExit(0)
time.sleep(1.2)
raise SystemExit(7)
'''
        with open(ART/'daemon-test.log','w') as log:
            started=time.monotonic();process=ManagedProcess([sys.executable,'-c',code],stdout=log)
            time.sleep(.25);self.assertIsNone(process.poll())
            self.assertEqual(process.wait(timeout=.01),7)
            self.assertGreater(time.monotonic()-started,1.2)
            state=read(process.state)
            self.assertTrue(state['complete']);self.assertFalse(state['stop_requested'])
            self.assertGreaterEqual(len(state['exits']),3)
            self.assertTrue(any(e['returncode']==7 for e in state['exits']))
    def test_failure_does_not_remove_artifacts(self):
        marker=ART/('preserved-'+uuid.uuid4().hex);marker.write_text('retain')
        with open(ART/'failure-test.log','w') as log:
            process=ManagedProcess([sys.executable,'-c','raise SystemExit(3)'],stdout=log)
            self.assertEqual(process.wait(),3)
            self.assertEqual(marker.read_text(),'retain')
            self.assertFalse(read(process.state)['stop_requested'])
            with self.assertRaises(RuntimeError):process.terminate()
    def test_archive_is_complete_verified_and_immutable(self):
        import archive
        label='unit-'+uuid.uuid4().hex
        result=archive.create(label);value=archive.verify(result['path'])
        self.assertTrue(value['verified']);self.assertGreater(value['files'],0)
        with self.assertRaises(AssertionError):archive.create(label)

if __name__=='__main__':
    ART.mkdir(parents=True,exist_ok=True);unittest.main(verbosity=2)
