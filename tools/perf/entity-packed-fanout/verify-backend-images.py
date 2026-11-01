import json,runpy,subprocess
from pathlib import Path
expected=json.loads((Path(__file__).parent/'backend-image-lock.json').read_text())
identity=runpy.run_path(str(Path(__file__).parent/'image-identity.py'))['identity']
for name,locked in expected.items():
    actual=identity(name)
    assert actual['fingerprint_sha256']==locked['fingerprint_sha256'],(name,actual,locked)
print('Pinned service image filesystems and runtime configurations verified')
