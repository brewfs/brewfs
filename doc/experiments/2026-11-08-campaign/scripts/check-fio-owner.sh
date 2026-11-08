#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== fio 进程的父进程 ==="
for p in $(pgrep -f '/usr/bin/fio' | head -2); do
  echo "  pid=$p ppid=$(ps -o ppid= -p $p | tr -d ' ') cmd=$(ps -o args= -p $p | cut -c1-100)"
  pp=$(ps -o ppid= -p $p | tr -d ' ')
  ps -o args= -p $pp 2>/dev/null | cut -c1-100 | sed 's/^/    parent: /'
done
echo
echo "=== 19:19 之后 c1k 产物 ==="
ls -lt /srv/ax-data/c1k/raw 2>/dev/null | head -5 | awk '{print "  "$6" "$7" "$8"  "$9}'
echo "  meta: $(ls /srv/ax-data/c1k/meta 2>/dev/null | tr '\n' ' ')"
echo "  logs: $(ls -t /srv/ax-data/c1k/logs 2>/dev/null | head -4 | tr '\n' ' ')"
echo
echo "=== 当前 fio 任务名 ==="
pgrep -af '/usr/bin/fio' | head -1 | grep -o 'name=[A-Za-z0-9._-]*' | head -1 | sed 's/^/  /'
EOS
