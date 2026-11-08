#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -o StrictHostKeyChecking=no -i $K root@10.20.2.164 'bash -s' <<'EOS' 2>&1
set -u
OUT=/srv/ax-data/sealtest
echo "=== fork.json 关键字段 ==="
python3 - <<'PY'
import json
j = json.load(open('/srv/ax-data/sealtest/fork.json'))
def walk(o, path=''):
    if isinstance(o, dict):
        for k,v in o.items():
            if any(s in k.lower() for s in ('base','parent','revision','upper','lower','root','layer','state','id')):
                if not isinstance(v,(dict,list)): print(f"  {path}{k} = {v}")
            walk(v, path+k+'.')
    elif isinstance(o, list):
        for i,v in enumerate(o[:2]): walk(v, path+f'[{i}].')
walk(j)
PY
echo "=== 二进制是否支持 mount --base / --from ==="
BIN=target/release/brewfs
cd /srv/ax-data/src 2>/dev/null || cd /
$BIN mount --help 2>&1 | grep -iE 'base|from|workspace' | head -8 | cut -c1-120 | sed 's/^/  /'
echo "=== workspace fork --help ==="
$BIN workspace fork --help 2>&1 | grep -iE 'base|from|revision|data' | head -8 | cut -c1-120 | sed 's/^/  /'
EOS
