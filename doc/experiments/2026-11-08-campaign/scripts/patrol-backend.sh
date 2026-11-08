#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
echo "=== gc1j 是否重挂 JuiceFS ==="
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "  juicefs 挂载数: $(mount | grep -c juicefs)"
mountpoint -q /mnt/juicefs-c1 && echo "  /mnt/juicefs-c1: y" || echo "  /mnt/juicefs-c1: n"
echo "  juicefs 进程: $(pgrep -fc 'juicefs mount' || echo 0)"
echo "  fio: $(pgrep -fc '/usr/bin/fio' || echo 0)"
echo "  c1j2 目录: $(ls /srv/ax-data/c1j2 2>/dev/null | tr '\n' ' ')"
echo "  c1j2 raw: $(ls /srv/ax-data/c1j2/raw/*.json 2>/dev/null | wc -l) 个"
echo "  磁盘: $(df -h /srv/ax-data | tail -1 | awk '{print $3"/"$2" ("$5")"}')"
echo "  内存: $(free -g | awk '/Mem:/{print $3"G/"$2"G"}')"
EOS
echo
echo "=== 后端 ==="
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.12 'bash -s' <<'EOS' 2>&1
set -u
echo "  TiKV: $(pgrep -c tikv-server || echo 0)  PD: $(pgrep -c pd-server || echo 0)"
echo "  TiKV RSS: $(ps -C tikv-server -o rss= --no-headers 2>/dev/null | awk '{printf "%.1fGB", $1/1048576}')"
echo "  load: $(uptime | sed 's/.*load average: //')"
EOS
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.13 'bash -s' <<'EOS' 2>&1
set -u
echo "  RustFS: $(pgrep -c rustfs || echo 0)  RSS: $(ps -C rustfs -o rss= --no-headers 2>/dev/null | awk '{printf "%.1fGB", $1/1048576}')"
echo "  health: $(curl -s -o /dev/null -w '%{http_code}' --max-time 4 http://127.0.0.1:9000/health)"
echo "  磁盘: $(df -h /srv/ax-data | tail -1 | awk '{print $3"/"$2" ("$5")"}')"
EOS
