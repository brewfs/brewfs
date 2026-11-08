#!/bin/bash
# 冷读 v4：RustFS 重启带凭据；每次 prep 前 remount
set -u
K=/root/.ssh/ax-c1-20261106
ssh -o StrictHostKeyChecking=no -i $K root@10.20.2.161 'bash -s' <<'EOS' 2>&1
set -u
pkill -9 -f c1v2-cold3.py 2>/dev/null || true
pkill -9 -f 'fio ' 2>/dev/null || true
sleep 2
sed -i "s|obj_ssh(\"setsid nohup /srv/ax-data/bin/rustfs|obj_ssh(\"export RUSTFS_ACCESS_KEY=\$(cat /etc/ax-c1/rustfs-access); export RUSTFS_SECRET_KEY=\$(cat /etc/ax-c1/rustfs-secret); setsid nohup /srv/ax-data/bin/rustfs|" /root/c1v2-cold3.py
grep -n 'RUSTFS_ACCESS_KEY' /root/c1v2-cold3.py | head -2
python3 -c "import ast;ast.parse(open('/root/c1v2-cold3.py').read())" && echo "  py ok"
setsid nohup python3 /root/c1v2-cold3.py > /srv/ax-data/c1v2-cold4-console.log 2>&1 &
sleep 120
echo "  进程: $(pgrep -fc c1v2-cold3.py || echo 0)"
tail -4 /srv/ax-data/c1v2/run.log 2>/dev/null | cut -c1-165 | sed 's/^/  /'
EOS
