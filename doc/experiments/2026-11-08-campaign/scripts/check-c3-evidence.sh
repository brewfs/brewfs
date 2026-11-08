#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
echo "=== a-obj bucket 实况 ==="
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.13 'ls -la /srv/ax-data/rustfs | head -12' 2>&1 | sed 's/^/  /'
echo
echo "=== C3 证据（a-client 本地盘）==="
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "  c3 目录大小: $(du -sh /srv/ax-data/c3 2>/dev/null | cut -f1)"
echo "  关键文件:"
for f in /srv/ax-data/c3/segverify10-20261107 /srv/ax-data/c3/pilot-brew-segverify-20261107 /srv/ax-data/c3/AGENT-REPORT.md /srv/ax-data/c3/verify-base-par-20261107/result.json; do
  [ -e "$f" ] && echo "    ✅ $f" || echo "    ❌ $f"
done
EOS
