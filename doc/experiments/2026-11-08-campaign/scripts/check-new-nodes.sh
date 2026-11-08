#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
for ip in 10.20.2.161 10.20.2.162 10.20.2.163; do
  echo "=== $ip ==="
  timeout 25 ssh -o StrictHostKeyChecking=no -o ConnectTimeout=15 -o BatchMode=yes -i $K root@$ip 'hostname; nproc; free -g | awk "/Mem:/{print \$2\" GB\"}"; lsblk -b -dn -o NAME,SIZE 2>/dev/null | head -4' 2>&1 | sed 's/^/  /'
done
