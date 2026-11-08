#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== 历史 pilot 输出目录内容 ==="
ls -1 /srv/ax-data/c2b2/p1/run 2>/dev/null | head -10 | sed 's/^/  /'
ls -1 /srv/ax-data/c3ws4/r1/run 2>/dev/null | head -10 | sed 's/^/  /'
echo
echo "=== result.json 顶层字段 ==="
for f in /srv/ax-data/c3ws4/r1/run/result.json /srv/ax-data/c3ws3/r1/run/result.json; do
  [ -f "$f" ] && { echo "  $f:"; python3 -c "import json,sys;d=json.load(open('$f'));print('   ',list(d.keys())[:20])"; }
done
echo
echo "=== smallfiles run --help（找 touched 列表参数）==="
/srv/ax-data/artifacts/bin/smallfiles run --help 2>&1 | grep -iE 'touched|expected|output|manifest' | head -8 | sed 's/^/  /'
EOS
