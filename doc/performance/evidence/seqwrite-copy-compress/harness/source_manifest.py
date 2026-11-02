#!/usr/bin/env python3
import hashlib
import json
import pathlib
import subprocess
import sys

repo = pathlib.Path('/home/hxy/brewfs-main')
files = subprocess.check_output(['git', 'ls-files', '-z'], cwd=repo).decode().split('\0')
manifest = {
    'revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip(),
    'files': {name: hashlib.sha256((repo / name).read_bytes()).hexdigest()
              for name in sorted(files) if name and (repo / name).is_file()},
}
pathlib.Path(sys.argv[1]).write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
