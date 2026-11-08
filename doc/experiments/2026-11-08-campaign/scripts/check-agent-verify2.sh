#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
D=/srv/ax-data/c3/segverify10-20261107
echo "=== agent 复核结果 ==="
echo "  segment json 数: $(ls $D/segment-*.json 2>/dev/null | wc -l) / 10"
echo "  汇总文件: $(ls $D/*summary* $D/*final* 2>/dev/null | head -3 | tr '\n' ' ')"
echo
echo "=== 每段结果 ==="
for f in $D/segment-*.json; do
  python3 -c "
import json,sys
d=json.load(open('$f'))
print('  $(basename $f):', d.get('files','?'), 'files,', d.get('mismatches','?'), 'mismatch,', d.get('wall_seconds','?'), 's')" 2>/dev/null
done
echo
echo "=== 驱动日志关键行 ==="
grep -aE 'SEGMENT_DONE|SEGVERIFY10_DONE|MISMATCH' /srv/ax-data/c3/segverify10-resume-driver.log 2>/dev/null | tail -12 | cut -c1-140 | sed 's/^/  /'
echo
echo "=== 重挂统计 ==="
echo "  REMOUNT_OK: $(grep -ac 'REMOUNT_OK' /srv/ax-data/c3/segverify10-resume-driver.log 2>/dev/null || echo 0)"
echo "  PART_PASS:  $(grep -ac 'PART_PASS' /srv/ax-data/c3/segverify10-resume-driver.log 2>/dev/null || echo 0)"
echo "  max_rss:    $(grep -ao 'max_rss_mb=[0-9]*' /srv/ax-data/c3/segverify10-resume-driver.log 2>/dev/null | tail -1)"
EOS
