"""Portable image identity across Docker classic/containerd image-ID semantics."""
import hashlib,json,os,subprocess,sys

def identity(name):
    prefix=['wsl','-d','Ubuntu-24.04','--'] if os.name=='nt' else []
    item=json.loads(subprocess.check_output(prefix+['docker','image','inspect',name],text=True,encoding='utf-8'))[0]
    config=item['Config']
    stable=dict(os=item['Os'],architecture=item['Architecture'],layers=item['RootFS']['Layers'],
                config={key:config.get(key) for key in ['User','Env','Entrypoint','Cmd','WorkingDir']})
    digest=hashlib.sha256(json.dumps(stable,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    return dict(fingerprint_sha256=digest,reported_id=item['Id'],repo_digests=item.get('RepoDigests',[]))

if __name__=='__main__':print(json.dumps({name:identity(name) for name in sys.argv[1:]},indent=2))
