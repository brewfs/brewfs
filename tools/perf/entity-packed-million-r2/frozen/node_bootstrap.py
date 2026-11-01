"""Prepare one dedicated owned role; no benchmark restarts or error cleanup."""
from pathlib import Path
import hashlib,json,os,subprocess,sys,time,urllib.request,urllib.error
from cloud_storage import format_mount

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'harness'))
from common import atomic,read,stamp
ART=ROOT/'artifacts'

def command(args,name):
    with open(ART/(name+'.log'),'w') as log:subprocess.run(args,check=True,stdout=log,stderr=subprocess.STDOUT)

def unit(name,arguments,environment=False):
    assert name.startswith('brewfs-million-r2-')
    text='''[Unit]
After=network-online.target
Wants=network-online.target
[Service]
Type=exec
Restart=no
KillMode=process
SendSIGKILL=no
TimeoutStartSec=infinity
TimeoutStopSec=infinity
CPUAccounting=yes
MemoryAccounting=yes
IOAccounting=yes
WorkingDirectory=@ROOT@
Environment=PYTHONPATH=@ROOT@/python-vendor
@ENV@
ExecStart=@COMMAND@
StandardOutput=append:@ROOT@/artifacts/@NAME@.log
StandardError=inherit
'''.replace('@ROOT@',str(ROOT)).replace('@ENV@','EnvironmentFile='+str(ROOT/'private/runtime.env') if environment else '')
    assert all(' ' not in arg and '\n' not in arg for arg in arguments)
    text=text.replace('@COMMAND@',' '.join(arguments)).replace('@NAME@',name)
    path=Path('/etc/systemd/system')/(name+'.service')
    if path.exists():assert path.read_text()==text,'Existing unit content changed'
    else:path.write_text(text)
    subprocess.run(['systemctl','daemon-reload'],check=True)
    subprocess.run(['systemctl','start',name],check=True)

def http(url,value=None):
    data=None if value is None else json.dumps(value).encode()
    with urllib.request.urlopen(urllib.request.Request(url,data=data,headers={'Content-Type':'application/json'}),timeout=3) as response:
        return response.read()

def ready(url):
    while True:
        try:return http(url)
        except OSError as e:atomic(ART/'readiness.json',dict(url=url,error=repr(e),**stamp()));time.sleep(1)

def main():
    assert ROOT==Path('/opt/entity-packed-million-r2') and os.getuid()==0
    assert (ROOT/'packages-ready').read_text().strip()=='ready'
    cfg=read(ROOT/'private/role.json');role=cfg['role'];assert role in ['client','objects','metadata']
    assert not (ART/'bootstrap-complete.json').exists(),'Inspect completed bootstrap; do not rerun it blindly'
    ART.mkdir(exist_ok=True)
    format_mount(cfg,ART/'storage')
    work=Path('/data')/cfg['run_token']/role/'work';work.mkdir(parents=True,exist_ok=True)
    target=ROOT/'work';target.mkdir(exist_ok=True)
    assert not target.is_symlink(),'FUSE mountinfo requires a stable canonical mount path'
    if os.path.ismount(target):assert os.path.samefile(target,work)
    else:
        assert not list(target.iterdir()),'Preserve an existing work directory'
        subprocess.run(['mount','--bind',str(work),str(target)],check=True)
        with open('/etc/fstab','a') as f:
            f.write(f'\n{work} {target} none bind,x-systemd.requires-mounts-for=/data 0 0\n')
            f.flush();os.fsync(f.fileno())
    assert os.stat(target).st_dev==os.stat('/data').st_dev and os.path.samefile(target,work)
    for args,name in [(['lscpu','--json'],'cpu'),(['lsblk','--json','--bytes','--output','NAME,PATH,TYPE,SIZE,SERIAL,FSTYPE,MOUNTPOINTS,MAJ:MIN'],'storage-devices'),
            (['uname','-a'],'kernel'),(['findmnt','--json'],'mounts'),(['timedatectl','show'],'clock')]:command(args,name)
    command(['docker','version'],'docker-version');command(['fio','--version'],'fio-version')
    atomic(ROOT/'deployment-public.json',{k:v for k,v in cfg.items() if k not in ['credentials']},durable=True)
    unit('brewfs-million-r2-agent',['/usr/bin/python3',str(ROOT/'harness/node_agent.py'),
        '--role',role,'--bind',cfg['private_ip'],'--port','9910','--interval','5'])
    ready('http://'+cfg['private_ip']+':9910/health')
    command(['/usr/bin/python3',str(ROOT/'disk_preflight.py')],'disk-preflight')
    if role!='client':
        lock=read(ROOT/'harness/backend-image-lock.json')
        required=['ax-retest/rustfs:a87e8f94'] if role=='objects' else ['pingcap/pd:v8.5.0','pingcap/tikv:v8.5.0']
        actual={}
        for image in required:
            expected=lock[image]['reported_id']
            identity=json.loads(subprocess.check_output(['docker','image','inspect',expected]))[0]
            assert identity['Id']==expected
            actual[image]={k:identity[k] for k in ['Id','RepoTags','RepoDigests']}
        atomic(ART/'backend-images.json',actual,durable=True)
        ip=cfg['private_ip'];services={}
        if role=='objects':
            env=read(ROOT/'private/environment.json')
            services['rustfs']=dict(image=lock[required[0]]['reported_id'],network_mode='host',
                command=['--address','127.0.0.1:9000','--access-key',env['AWS_ACCESS_KEY_ID'],
                    '--secret-key',env['AWS_SECRET_ACCESS_KEY'],'/data'],
                volumes=[str(work/'rustfs')+':/data'],ulimits={'nofile':{'soft':65536,'hard':65536}})
            (work/'rustfs').mkdir()
            template=(ROOT/'object_proxy.conf.in').read_text()
            (work/'nginx.conf').write_text(template.replace('@WORK@',str(work)).replace('@IP@',ip))
        else:
            (work/'pd').mkdir();(work/'tikv').mkdir()
            services['pd']=dict(image=lock['pingcap/pd:v8.5.0']['reported_id'],network_mode='host',
                command=['--name=pd','--data-dir=/data/pd',f'--client-urls=http://{ip}:2379',
                    f'--advertise-client-urls=http://{ip}:2379',f'--peer-urls=http://{ip}:2380',
                    f'--advertise-peer-urls=http://{ip}:2380',f'--initial-cluster=pd=http://{ip}:2380','--log-level=warn'],
                volumes=[str(work/'pd')+':/data/pd'])
            services['tikv']=dict(image=lock['pingcap/tikv:v8.5.0']['reported_id'],network_mode='host',
                command=[f'--addr={ip}:20160',f'--advertise-addr={ip}:20160',f'--status-addr={ip}:20180',
                    f'--pd={ip}:2379','--data-dir=/data/tikv','--log-level=warn'],
                volumes=[str(work/'tikv')+':/data/tikv'],ulimits={'nofile':{'soft':1048576,'hard':1048576}})
        for name,item in services.items():item['labels']={'experiment.run_id':cfg['run_token'],'experiment.role':name}
        compose=ROOT/'private/compose.json';atomic(compose,dict(services=services),durable=True);compose.chmod(0o600)
        args=['docker','compose','-p','million-r2-'+role,'-f',str(compose),'up','-d']
        command(args,'service-start')
        if role=='objects':
            ready('http://127.0.0.1:9000/health')
            command(['nginx','-t','-p',str(ROOT/'work'),'-c',str(ROOT/'work/nginx.conf')],'nginx-config-check')
            unit('brewfs-million-r2-proxy',['/usr/sbin/nginx','-p',str(ROOT/'work'),'-c',str(ROOT/'work/nginx.conf')])
        else:
            while True:
                try:
                    stores=json.loads(http(f'http://{ip}:2379/pd/api/v1/stores')).get('stores',[])
                    if len(stores)==1 and stores[0]['store']['state_name']=='Up':
                        http(f'http://{ip}:2379/pd/api/v1/config',{'max-replicas':1});break
                except OSError as e:atomic(ART/'pd-readiness.json',dict(error=repr(e),**stamp()))
                time.sleep(1)
            atomic(ART/'pd-config.json',json.loads(http(f'http://{ip}:2379/pd/api/v1/config')))
            atomic(ART/'tikv-stores.json',json.loads(http(f'http://{ip}:2379/pd/api/v1/stores')))
            atomic(ROOT/'metrics-targets.json',[dict(name='pd',state='active',url=f'http://{ip}:2379/metrics'),
                dict(name='tikv',state='active',url=f'http://{ip}:20180/metrics')])
    else:
        identities=read(ROOT/'binary-identities.json')
        for name,expected in identities.items():
            with (ROOT/'bin'/name).open('rb') as source:actual=hashlib.file_digest(source,'sha256').hexdigest()
            assert actual==expected
            result=subprocess.run(['ldd',str(ROOT/'bin'/name)],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
            (ART/('ldd-'+name+'.log')).write_text(result.stdout)
            assert 'not found' not in result.stdout,'Missing runtime library: '+name
            assert result.returncode==0 or 'not a dynamic executable' in result.stdout or 'statically linked' in result.stdout
    atomic(ART/'bootstrap-complete.json',dict(role=role,private_ip=cfg['private_ip'],**stamp()),durable=True)

if __name__=='__main__':
    try:main()
    except BaseException:
        import traceback
        ART.mkdir(exist_ok=True);atomic(ART/'bootstrap-failed.json',dict(error=traceback.format_exc(),**stamp()),durable=True)
        raise
