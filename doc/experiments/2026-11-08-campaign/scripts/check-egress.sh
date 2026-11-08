#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
echo "=== a-client 到模型端点的连通性 ==="
ssh -i $K -o StrictHostKeyChecking=no -o ConnectTimeout=10 root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "  curl 130.33.96.80:8080 ..."
timeout 12 curl -s -o /dev/null -w "    http=%{http_code} time=%{time_total}\n" --max-time 10 http://130.33.96.80:8080/ 2>&1 || echo "    ❌ 不可达"
echo "  默认路由: $(ip route | grep default | head -1)"
echo "  DNS: $(timeout 5 getent hosts 130.33.96.80 2>/dev/null || echo n/a)"
EOS

echo
echo "=== jump 到同一端点 ==="
timeout 15 curl -s -o /dev/null -w "  jump http=%{http_code} time=%{time_total}\n" --max-time 10 http://130.33.96.80:8080/ 2>&1 || echo "  jump ❌"
