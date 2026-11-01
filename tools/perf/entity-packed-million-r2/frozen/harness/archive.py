"""Immutable evidence archives with embedded per-file checksums; no deletion."""
from pathlib import Path
import argparse,gzip,hashlib,io,json,os,re,tarfile,tempfile,time
from common import ROOT,ART,atomic,stamp

def create(label):
    assert re.fullmatch(r'[a-zA-Z0-9-]+',label),'Invalid checkpoint label'
    directory=ROOT/'work/checkpoints';directory.mkdir(parents=True,exist_ok=True)
    output=directory/(label+'.tar.gz');result=directory/(label+'.json')
    assert not output.exists() and not result.exists(),'Checkpoint identities cannot be overwritten'
    started=stamp();manifest=[];excluded=[]
    candidates=sorted(ART.rglob('*'))
    candidates+=sorted((ROOT/'harness').glob('*.py'))
    candidates+=sorted(ROOT.glob('*.py'))+sorted(ROOT.glob('*.sh'))+sorted(ROOT.glob('*.conf.in'))
    candidates+=[ROOT/name for name in ['experiment-contract.json','resource-ledger.json','binary-identities.json',
        'metrics-targets.json','harness/settings.json','deployment-public.json','frozen-files.json']]
    temporary=directory/(label+'.tar.gz.partial')
    with temporary.open('xb') as raw:
        with gzip.GzipFile(fileobj=raw,mode='wb',compresslevel=3,mtime=0) as compressed:
            with tarfile.open(fileobj=compressed,mode='w|') as archive:
                for path in candidates:
                    if not path.is_file() or path.is_symlink():continue
                    if path.name.endswith('.open') or path.name.endswith('.tmp'):
                        excluded.append(str(path.relative_to(ROOT)));continue
                    name=str(path.relative_to(ROOT));digest=hashlib.sha256()
                    # Capture an exact finite snapshot even if a text log grows.
                    with path.open('rb') as source,tempfile.TemporaryFile(dir=directory) as snapshot:
                        stat=os.fstat(source.fileno());remaining=stat.st_size
                        while remaining:
                            data=source.read(min(1<<20,remaining))
                            if not data:raise RuntimeError('Source shortened during snapshot: '+name)
                            snapshot.write(data);digest.update(data);remaining-=len(data)
                        snapshot.seek(0);info=tarfile.TarInfo(name);info.size=stat.st_size;info.mtime=stat.st_mtime
                        info.mode=0o600;archive.addfile(info,snapshot)
                    manifest.append(dict(path=name,bytes=stat.st_size,sha256=digest.hexdigest()))
                value=dict(label=label,started=started,files=manifest,
                    active_segments_deferred=excluded,private_credentials_excluded=True)
                body=(json.dumps(value,sort_keys=True)+'\n').encode()
                info=tarfile.TarInfo('archive-manifest.json');info.size=len(body);info.mode=0o600
                archive.addfile(info,io.BytesIO(body))
        raw.flush();os.fsync(raw.fileno())
    os.replace(temporary,output)
    with output.open('rb') as source:digest=hashlib.file_digest(source,'sha256').hexdigest()
    value=dict(label=label,path=str(output),bytes=output.stat().st_size,sha256=digest,
        file_count=len(manifest),active_segments_deferred=len(excluded),started=started,ended=stamp())
    atomic(result,value,durable=True);return value

def verify(path):
    path=Path(path);seen={}
    with tarfile.open(path,'r:gz') as archive:
        for member in archive:
            assert member.isfile() and not Path(member.name).is_absolute() and '..' not in Path(member.name).parts
            source=archive.extractfile(member)
            if member.name=='archive-manifest.json':manifest=json.load(source)
            else:
                assert member.name not in seen
                seen[member.name]=dict(bytes=member.size,sha256=hashlib.file_digest(source,'sha256').hexdigest())
    expected={r['path']:dict(bytes=r['bytes'],sha256=r['sha256']) for r in manifest['files']}
    assert expected==seen,'Missing, extra or corrupted archive members'
    return dict(verified=True,files=len(seen),deferred=manifest['active_segments_deferred'])

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('action',choices=['create','verify']);parser.add_argument('value')
    args=parser.parse_args();print(json.dumps(create(args.value) if args.action=='create' else verify(args.value)))
