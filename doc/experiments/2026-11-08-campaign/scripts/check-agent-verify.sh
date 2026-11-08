#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
D=/srv/ax-data/c3/segverify10-20261107
echo "=== agent 复核产物 ($D) ==="
ls -A $D 2>/dev/null | head -12 | sed 's/^/  /'
echo
echo "=== 已完成段（part json）==="
for f in $D/part-*.json; do
  [ -f "$f" ] || continue
  python3 -c "import json;d=json.load(open('$f'));print('  $(basename $f):',d.get('files'),'files',d.get('mismatches'),'mismatch')" 2>/dev/null
done
echo
echo "=== 段进度日志尾 5 ==="
tail -5 /srv/ax-data/c3/segverify10-resume-driver.log 2>/dev/null | cut -c1-150 | sed 's/^/  /'
echo
echo "=== part 文件数: $(ls $D/part-*.json 2>/dev/null | wc -l) / 10 ==="
EOS
