"""Format only the exact newly owned blank data disk, never a root/used disk."""
from pathlib import Path
import json,os,subprocess

SIZE=200*1024**3
def normalized(value):return str(value or '').strip().replace('-','').lower()
def walk(items):
    for item in items:
        yield item
        yield from walk(item.get('children',[]))
def disk_for_identity(tree,expected):
    aliases={normalized(expected)}
    # ECS NVMe exposes the full unique disk component without the API's d- class
    # prefix. Accept complete known representations, never a truncated prefix.
    if str(expected).startswith('d-'):aliases.add(normalized(str(expected)[2:]))
    matches=[d for d in tree if d['type']=='disk' and normalized(d.get('serial')) in aliases]
    assert len(matches)==1,'Exact cloud disk serial is missing or ambiguous; inspect without formatting'
    disk=matches[0];assert int(disk['size'])==SIZE,'Unexpected data disk capacity'
    assert disk['path'].startswith('/dev/')
    return disk
def validate_blank(disk,root_major_minor):
    assert disk['type']=='disk' and int(disk['size'])==SIZE
    assert not disk.get('children'),'Data disk has partitions; preserve it'
    for item in walk([disk]):
        assert item.get('maj:min')!=root_major_minor,'Refuse the root filesystem device'
        assert not any(item.get('mountpoints') or []),'Data disk is mounted; preserve it'
        assert not item.get('fstype'),'Data disk contains a filesystem; preserve it'
    return disk['path']

def format_mount(cfg,art):
    art=Path(art);art.mkdir(parents=True,exist_ok=True)
    tree=json.loads(subprocess.check_output(['lsblk','--json','--bytes','--output',
        'NAME,PATH,TYPE,SIZE,SERIAL,FSTYPE,MOUNTPOINTS,MAJ:MIN']))['blockdevices']
    (art/'block-devices-before.json').write_text(json.dumps(tree,indent=2)+'\n')
    disk=disk_for_identity(tree,cfg['data_disk_id']);path=disk['path']
    owner=Path('/data/.brewfs-million-r2-owner.json')
    if os.path.ismount('/data'):
        current=json.loads(owner.read_text())
        assert current==dict(disk_id=cfg['data_disk_id'],run_token=cfg['run_token'],role=cfg['role'])
        actual=subprocess.check_output(['findmnt','--noheadings','--output','SOURCE','--target','/data'],text=True).strip()
        assert os.path.realpath(actual)==os.path.realpath(path)
        return path
    root_dev=os.stat('/').st_dev;root_number=f'{os.major(root_dev)}:{os.minor(root_dev)}'
    validate_blank(disk,root_number)
    signatures=json.loads(subprocess.check_output(['wipefs','--json','--no-act',path]))
    assert not signatures.get('signatures'),'Disk signatures exist; preserve the disk'
    mount=Path('/data');mount.mkdir(exist_ok=True)
    assert not list(mount.iterdir()),'Nonempty data mountpoint must be preserved'
    # Persist the exact destructive intent before issuing it. No -F or wipe flag.
    intent=dict(disk_id=cfg['data_disk_id'],serial=disk['serial'],path=path,size=SIZE,
        role=cfg['role'],run_token=cfg['run_token'],root_major_minor=root_number)
    (art/'format-intent.json').write_text(json.dumps(intent,indent=2)+'\n')
    command=['mkfs.ext4','-L','brewfs-r2-data','-m','0',
        '-E','lazy_itable_init=0,lazy_journal_init=0',path]
    with open(art/'format.log','w') as log:subprocess.run(command,stdout=log,stderr=subprocess.STDOUT,check=True)
    subprocess.run(['mount','-t','ext4','-o','noatime',path,'/data'],check=True)
    current=dict(disk_id=cfg['data_disk_id'],run_token=cfg['run_token'],role=cfg['role'])
    with owner.open('x') as output:json.dump(current,output);output.flush();os.fsync(output.fileno())
    uuid=subprocess.check_output(['blkid','-s','UUID','-o','value',path],text=True).strip()
    assert uuid and '/' not in uuid
    fstab=Path('/etc/fstab')
    assert not any(line.split()[1:2]==['/data'] for line in fstab.read_text().splitlines() if line.strip() and not line.startswith('#'))
    with fstab.open('a') as output:
        output.write(f'\nUUID={uuid} /data ext4 defaults,noatime 0 2\n');output.flush();os.fsync(output.fileno())
    (art/'data-mount.txt').write_bytes(subprocess.check_output(['findmnt','--target','/data']))
    return path
