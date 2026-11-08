#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -o StrictHostKeyChecking=no -i $K root@10.20.2.161 'bash -s' <<'EOS' 2>&1
set -u
pkill -9 -f write-rest 2>/dev/null || true
pkill -9 -f c1v2 2>/dev/null || true
pkill -9 -f 'fio ' 2>/dev/null || true
sleep 2
echo "  fio=$(pgrep -fc 'fio ' || echo 0) matrix=$(pgrep -fc write-rest || echo 0)"
rm -f /mnt/jfs-c1v2/*.dat /mnt/bfs-c1v2/*.dat 2>/dev/null || true
wc -l < /srv/ax-data/c1v2/c1v2.tsv
python3 - <<'PY'
import csv, statistics, collections
rows = list(csv.DictReader(open('/srv/ax-data/c1v2/c1v2.tsv'), delimiter='\t'))
print(f"  rows={len(rows)}")
agg = collections.defaultdict(list)
for r in rows:
    try:
        bw = float(r['bw_mib_s']); p50 = float(r['p50_ms'])
    except Exception:
        continue
    agg[(r['op'], r['bs'], r['system'])].append((bw, p50))
print(f"  {'op':<10}{'bs':<5}{'system':<9}{'bw MiB/s':>10}{'p50 ms':>10}{'n':>3}")
for (op, bs, sysn), vals in sorted(agg.items(), key=lambda kv:(kv[0][1], kv[0][0], kv[0][2])):
    bws = [v[0] for v in vals]; p50s = [v[1] for v in vals]
    print(f"  {op:<10}{bs:<5}{sysn:<9}{statistics.median(bws):>10.1f}{statistics.median(p50s):>10.2f}{len(vals):>3}")
PY
EOS
