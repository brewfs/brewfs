#!/usr/bin/env python3
"""Actual ECS/OSS/FUSE campaign; filesystem correctness is mandatory per row."""
import argparse
import datetime
import hashlib
import json
import os
import pathlib
import signal
import subprocess
import sys
import tarfile
import time

ROOT=pathlib.Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT))


def write_json(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value,sort_keys=True,indent=2)+'\n')


def run(command,log,timeout,env=None):
    with log.open('wb') as output:
        process=subprocess.Popen(command,stdout=output,stderr=subprocess.STDOUT,env=env,start_new_session=True)
        try:status=process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid,signal.SIGTERM)
            try:process.wait(timeout=10)
            except subprocess.TimeoutExpired:os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=10)
            raise RuntimeError('bounded command timed out') from None
    if status:raise RuntimeError(f'command failed with exit {status}; see local row log')


def memory(pid):
    fields={}
    for line in pathlib.Path(f'/proc/{pid}/smaps_rollup').read_text().splitlines():
        if line.startswith(('Rss:','Pss:')):fields[line.split(':')[0]+'_kib']=int(line.split()[1])
    for line in pathlib.Path(f'/proc/{pid}/status').read_text().splitlines():
        if line.startswith('VmHWM:'):fields['peak_rss_kib']=int(line.split()[1])
    return fields


class Mount:
    def __init__(self,work,artifacts,config,env):
        self.work=work;self.artifacts=artifacts;self.config=config;self.env=env;self.process=None
        self.path=work/'mnt';self.path.mkdir(exist_ok=True)
    def __enter__(self):
        subprocess.run(['sync'],check=True)
        pathlib.Path('/proc/sys/vm/drop_caches').write_text('3\n')
        write_json(self.artifacts/'cache-proof.json',dict(page_cache='dropped',payload_memory=0,payload_ssd=0,window=0,decoded=0,data_prefetch=False))
        self.log=(self.artifacts/'mount.log').open('wb')
        self.started=time.monotonic()
        self.process=subprocess.Popen([str(ROOT/'brewfs'),'mount','--privileged','--config',str(self.config),str(self.path)],stdout=self.log,stderr=subprocess.STDOUT,env=self.env,start_new_session=True)
        deadline=time.monotonic()+90
        while time.monotonic()<deadline:
            if os.path.ismount(self.path):
                self.ready=time.monotonic();write_json(self.artifacts/'daemon-before.json',memory(self.process.pid));return self
            if self.process.poll() is not None:break
            time.sleep(.25)
        self.stop();raise RuntimeError('mount readiness failed')
    def stop(self):
        if self.process and self.process.poll() is None:
            if os.path.ismount(self.path):
                result=subprocess.run(['fusermount3','-u',str(self.path)],capture_output=True,timeout=20)
                if result.returncode:raise RuntimeError('normal FUSE unmount failed')
            self.process.terminate()
            try:self.process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                pathlib.Path(self.artifacts/'teardown-state.txt').write_text(pathlib.Path(f'/proc/{self.process.pid}/stat').read_text())
                raise RuntimeError('FUSE daemon did not exit after unmount') from None
        if hasattr(self,'log'):self.log.close()
        if os.path.ismount(self.path):raise RuntimeError('FUSE mount remains')
    def __exit__(self,kind,value,trace):
        if self.process and self.process.poll() is None:
            write_json(self.artifacts/'daemon-after.json',memory(self.process.pid))
            try:(self.artifacts/'stats.txt').write_bytes((self.path/'.stats').read_bytes().replace(b'\0',b''))
            except OSError:pass
        self.active_end=time.monotonic();self.stop();self.ended=time.monotonic()
        write_json(self.artifacts/'timing.json',dict(mount_seconds=self.ready-self.started if hasattr(self,'ready') else None,active_seconds=self.active_end-self.ready if hasattr(self,'ready') else None,drain_seconds=self.ended-self.active_end,total_seconds=self.ended-self.started))


def cold_links(root):
    import errno
    assert os.readlink(os.fsencode(root/'.cold-link'))==b'raw-\xff-target'
    assert os.getxattr(root/'.cold-file','user.brewfs.test')==b'\0\xffcold'
    assert 'user.brewfs.test' in os.listxattr(root/'.cold-file')
    assert (root/'.cold-file').read_bytes()==b'cold\n'
    try:os.setxattr(root/'.cold-file','user.brewfs.test',b'forbidden')
    except OSError as error:assert error.errno==errno.EROFS
    else:raise ValueError('readonly xattr mutation succeeded')
    a=root/'.hardlink-a';b=root/'d000'/'.hardlink-b'
    assert a.stat().st_ino==b.stat().st_ino and a.stat().st_nlink==b.stat().st_nlink==2
    assert a.read_bytes()==b.read_bytes()==b'hardlink\n'
    return dict(readlink_raw=True,xattr_binary=True,listxattr=True,ero_fs=True,hardlink_inode=True,nlink=2,hardlink_content=True)


def upload_artifacts(bucket,prefix,role,artifact_dir):
    # OSS-native signing with refreshed temporary instance credentials; do not
    # persist credentials or echo the signature/token anywhere.
    import base64,email.utils,hmac,urllib.request,urllib.parse
    from ecs_role_credentials import credentials
    path=ROOT/'campaign-results.tar.gz'
    with tarfile.open(path,'w:gz') as archive:archive.add(artifact_dir,arcname='artifacts')
    data=path.read_bytes();value=credentials(role)
    key=prefix+'/results/campaign-results.tar.gz';date=email.utils.formatdate(usegmt=True);content_type='application/gzip'
    canonical='x-oss-security-token:'+value['SessionToken']+'\n'
    sign='PUT\n\n'+content_type+'\n'+date+'\n'+canonical+'/'+bucket+'/'+key
    signature=base64.b64encode(hmac.new(value['SecretAccessKey'].encode(),sign.encode(),hashlib.sha1).digest()).decode()
    request=urllib.request.Request('https://'+bucket+'.oss-cn-hangzhou-internal.aliyuncs.com/'+urllib.parse.quote(key,safe='/'),data=data,method='PUT',headers={'Date':date,'Content-Type':content_type,'x-oss-security-token':value['SessionToken'],'Authorization':'OSS '+value['AccessKeyId']+':'+signature})
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request,timeout=120) as response:
        if response.status!=200:raise RuntimeError('result upload failed')


class Runner:
    def __init__(self,args):
        self.args=args;self.work=ROOT/'work';self.work.mkdir(exist_ok=True);self.artifacts=ROOT/'artifacts';self.artifacts.mkdir(exist_ok=True)
        self.deadline=min(time.time()+5.5*3600,args.deadline-1800)
        self.results=[];self.env=os.environ.copy()
        for name in ['AWS_ACCESS_KEY_ID','AWS_SECRET_ACCESS_KEY','AWS_SESSION_TOKEN','HTTP_PROXY','HTTPS_PROXY','ALL_PROXY','http_proxy','https_proxy','all_proxy']:self.env.pop(name,None)
        config=self.work/'aws-config';config.write_text('[default]\nregion = cn-hangzhou\ncredential_process = python3 '+str(ROOT/'ecs_role_credentials.py')+' --role '+args.role+'\n');config.chmod(0o600)
        self.env.update(AWS_CONFIG_FILE=str(config),AWS_SDK_LOAD_CONFIG='1',AWS_EC2_METADATA_DISABLED='true',BREWFS_TEST_RAM_ROLE=args.role,RUST_LOG='warn',BREWFS_CACHE_TTL_MS='0',BREWFS_FUSE_READ_DIRECT_IO='1',BREWFS_FUSE_KEEP_CACHE='0',BREWFS_PACKED_FRAME_WINDOW_CACHE_BYTES='0',BREWFS_PACKED_FRAME_WINDOW_PREFETCH='false',BREWFS_PACKED_DECODED_FRAME_CACHE_BYTES='0')
    def remaining(self,cap=1800):
        remaining=int(self.deadline-time.time())
        if remaining<90:raise RuntimeError('campaign safety deadline reached')
        return min(cap,remaining)
    def fixture(self,name,count,minimum,maximum,wire,codec='raw',profile='random-small-file',posix=False,huge=False):
        if wire!=5:raise ValueError('only current packed encoding 005 is supported')
        folder=self.artifacts/'fixtures'/name;folder.mkdir(parents=True,exist_ok=True)
        manifest=self.work/(name+'.key')
        argv=[str(ROOT/'packed_snapshot_fixture'),'--bucket',self.args.bucket,'--endpoint','https://oss-cn-hangzhou-internal.aliyuncs.com','--region','cn-hangzhou','--prefix',self.args.prefix+'/fixtures/'+name,'--wire-version',str(wire),'--manifest-output',str(manifest),'--dir-levels',str(0 if huge else 2),'--dirs-per-level','10','--files-per-dir',str(count if huge else count//100),'--small-file-min-size',str(minimum),'--small-file-max-size',str(maximum),'--access-profile',profile]
        argv+=['--metadata-codec',codec,'--data-codec',codec]
        if posix:argv+=['--cold-corpus','true','--hardlink-corpus','true']
        before=time.monotonic();run(argv,folder/'publish.log',self.remaining(2400),self.env)
        key=manifest.read_text().strip();write_json(folder/'profile.json',dict(wire=wire,codec=codec,profile=profile,files=count,minimum=minimum,maximum=maximum,logical_shape='huge-directory' if huge else '10x10 leaves',publication_seconds=time.monotonic()-before,manifest_key=key))
        return dict(name=name,key=key,count=count,minimum=minimum,maximum=maximum,wire=wire,codec=codec,profile=profile,posix=posix,huge=huge)
    def row(self,fixture,mode,repeat=0,workers=16,order='lexicographic',metadata_bytes=268435456,warm='off'):
        name=f'{fixture["name"]}-{mode}-{order}-w{workers}-m{metadata_bytes}-{warm}-r{repeat}'
        folder=self.artifacts/'rows'/name;folder.mkdir(parents=True,exist_ok=True)
        env=self.env.copy();env.update(BREWFS_PACKED_METADATA_CACHE_BYTES=str(metadata_bytes),BREWFS_PACKED_METADATA_PREFETCH=warm)
        config=self.work/(name+'.yaml')
        config.write_text('mount_point: '+str(self.work/'mnt')+'\nvolume_format: packed-snapshot\npacked_manifest_key: '+fixture['key']+'\ndata:\n  backend: s3\n  s3:\n    bucket: '+self.args.bucket+'\n    endpoint: https://oss-cn-hangzhou-internal.aliyuncs.com\n    region: cn-hangzhou\n    force_path_style: false\n    disable_payload_checksum: true\nlayout: { chunk_size: 67108864, block_size: 4194304 }\nfuse: { workers: 16, max_background: 512, privileged: true }\ncache:\n  root: '+str(self.work/'cache')+'\n  read_memory_bytes: 0\n  read_ssd_bytes: 0\n  prefetch_enabled: false\n  range_background_prefetch: false\n  compression: none\n')
        record=dict(name=name,fixture=fixture,mode=mode,workers=workers,order=order,metadata_bytes=metadata_bytes,metadata_prefetch=warm,repeat=repeat,status='started')
        write_json(folder/'profile.json',record)
        try:
            with Mount(self.work,folder,config,env) as mount:
                if mode=='posix':write_json(folder/'summary.json',cold_links(mount.path))
                elif mode.startswith('fio'):
                    filename=mount.path.joinpath(*(['d000']*2),'f000000')
                    run(['fio','--name='+mode,'--filename='+str(filename),'--readonly','--rw='+('read' if mode=='fio-seqread' else 'randread'),'--ioengine=sync','--direct=1','--iodepth=1','--numjobs=4','--bs=4k','--size='+str(fixture['minimum']),'--runtime=20','--time_based=1','--group_reporting','--output-format=json','--output='+str(folder/'summary.json')],folder/'scanner.log',self.remaining(90),env)
                else:
                    common=['--root',str(mount.path),'--expected-files',str(fixture['count']),'--min-size',str(fixture['minimum']),'--max-size',str(fixture['maximum']),'--dir-levels',str(0 if fixture['huge'] else 2),'--dirs-per-level','10','--files-per-leaf',str(fixture['count'] if fixture['huge'] else fixture['count']//100),'--workers',str(workers),'--json-output',str(folder/'summary.json')]
                    if mode=='partial':command=['python3',str(ROOT/'packed_partial_scan.py'),*common,'--max-files','1024' if workers==16 else '128']
                    else:command=['python3',str(ROOT/'smallfiles_scan.py'),*common,'--mode',mode,'--order',order,'--shuffle-seed','20261003','--epochs','1']
                    run(command,folder/'scanner.log',self.remaining(1800),env)
                    summary=json.loads((folder/'summary.json').read_text());assert summary['errors']==0
                    if mode=='full':assert summary['files']==fixture['count'] and summary['payload_bytes']==summary['logical_bytes']
            timing=json.loads((folder/'timing.json').read_text());summary=json.loads((folder/'summary.json').read_text())
            if mode in ['full','partial']:
                logical=summary['payload_bytes'];timing.update(active_bw_mib_s=logical/1048576/timing['active_seconds'],active_plus_drain_bw_mib_s=logical/1048576/(timing['active_seconds']+timing['drain_seconds']));write_json(folder/'timing.json',timing)
            record['status']='passed'
        except Exception as error:
            record.update(status='failed',error_class=type(error).__name__)
            raise
        finally:
            write_json(folder/'profile.json',record);self.results.append(record);write_json(self.artifacts/'campaign-summary.json',dict(rows=self.results,unsupported=['packed-workspace lifecycle','source-backed sparse','external large-file placement','POSIX ACL application','005 pipeline/warm cache'],deadline_utc=datetime.datetime.fromtimestamp(self.deadline,datetime.timezone.utc).isoformat()))
            print(json.dumps(dict(row=name,status=record['status'])),flush=True)
    def execute(self):
        tiny=self.fixture('005-posix',100,102400,102400,5,'raw',posix=True)
        self.row(tiny,'posix');self.row(tiny,'full')
        fixtures=[]
        for wire,codec in [(5,'raw'),(5,'zstd')]:fixtures.append(self.fixture(f'10k-fixed-{wire}-{codec}',10000,102400,102400,wire,codec))
        for repeat in range(2):
            arms=fixtures if repeat==0 else list(reversed(fixtures))
            for fixture in arms:
                for mode,order in [('tree','lexicographic'),('stat','shuffle'),('full','lexicographic'),('full','shuffle'),('partial','shuffle')]:self.row(fixture,mode,repeat=repeat,order=order)
        for fixture in fixtures[:1]:
            self.row(fixture,'partial',workers=1,order='shuffle');self.row(fixture,'full',metadata_bytes=8388608,order='shuffle')
        self.row(fixtures[0],'full',warm='auto');self.row(fixtures[0],'fio-seqread');self.row(fixtures[0],'fio-randread')
        for profile in ['random-small-file','sequential-small-file']:
            fixture=self.fixture('10k-mixed-005-'+profile,10000,102400,1048576,5,'raw',profile)
            self.row(fixture,'full',order='shuffle');self.row(fixture,'partial',order='shuffle')
        # Every preceding required 10k row has succeeded; do not scale an error.
        write_json(self.artifacts/'10k-scale-gate.json',dict(passed=True,rows=len(self.results)))
        large=[]
        for wire,codec in [(5,'raw'),(5,'zstd')]:large.append(self.fixture(f'100k-fixed-{wire}-{codec}',100000,102400,102400,wire,codec))
        for repeat in range(2):
            for fixture in large if repeat==0 else list(reversed(large)):
                for mode,order in [('tree','lexicographic'),('stat','shuffle'),('full','lexicographic'),('full','shuffle'),('partial','shuffle')]:self.row(fixture,mode,repeat=repeat,order=order)
        self.row(large[0],'full',metadata_bytes=8388608,order='shuffle');self.row(large[0],'partial',workers=1,order='shuffle')
        huge=self.fixture('100k-huge-directory-005-raw',100000,4096,4096,5,'raw',huge=True);self.row(huge,'tree');self.row(huge,'full',order='shuffle')
        mixed=self.fixture('100k-mixed-005-raw',100000,102400,1048576,5,'raw');self.row(mixed,'full',order='shuffle');self.row(mixed,'partial',order='shuffle')
        write_json(self.artifacts/'completion.json',dict(supported_required_rows_passed=True,rows=len(self.results),reference='historical JuiceFS; fresh matched row not supplied by this runner',max_scale=100000))


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--bucket',required=True);parser.add_argument('--prefix',required=True);parser.add_argument('--role',required=True);parser.add_argument('--deadline',type=float,required=True);args=parser.parse_args()
    runner=Runner(args);status=0
    try:runner.execute()
    except Exception as error:
        status=1;write_json(runner.artifacts/'failure.json',dict(error_class=type(error).__name__,rows_completed=len(runner.results)));print('campaign failed; details in artifact row logs',flush=True)
    finally:
        try:upload_artifacts(args.bucket,args.prefix,args.role,runner.artifacts);print('artifact upload completed',flush=True)
        except Exception:status=1;print('artifact upload failed; preserve instance diagnostics before cleanup',flush=True)
    return status


if __name__=='__main__':raise SystemExit(main())
