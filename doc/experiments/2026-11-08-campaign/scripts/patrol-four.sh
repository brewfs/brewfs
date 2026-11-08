#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
echo "=== tmux（jump）==="
tmux ls 2>/dev/null | sed 's/^/  /'
for s in gbrew gc1 gc2 grust; do
  echo "  $s log: $(stat -c%s /root/$s.log 2>/dev/null || echo 0) B"
done
echo
echo "=== gc2 现场（a-meta 10.20.0.12）==="
ssh -i $K -o StrictHostKeyChecking=no -o ConnectTimeout=8 root@10.20.0.12 'bash -s' <<'EOS' 2>/dev/null
set -u
echo "  挂载: $(mountpoint -q /mnt/brewfs-c1m && echo y || echo n)"
echo "  c1m 目录: $(ls /srv/ax-data/c1m 2>/dev/null | tr '\n' ' ')"
echo "  fio: $(pgrep -fc 'fio ' || echo 0)  brewfs: $(pgrep -fc brewfs-patched || echo 0)"
echo "  raw: $(ls /srv/ax-data/c1m/raw 2>/dev/null | wc -l) 个"
echo "  磁盘: $(df -h /srv/ax-data | tail -1 | awk '{print $3"/"$2" ("$5")"}')"
EOS
echo
echo "=== gc1 现场（a-client 10.20.0.11）==="
ssh -i $K -o StrictHostKeyChecking=no -o ConnectTimeout=8 root@10.20.0.11 'bash -s' <<'EOS' 2>/dev/null
set -u
echo "  C1 raw: $(ls /srv/ax-data/c1c2/c1/raw/*.fio.json 2>/dev/null | wc -l) 个"
ls -lt /srv/ax-data/c1c2/c1/raw 2>/dev/null | head -3 | awk '{print "    "$6" "$7" "$8"  "$9}'
echo "  fio: $(pgrep -fc 'fio ' || echo 0)"
EOS
