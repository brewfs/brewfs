#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
echo "=== obj: rustfs 进程环境里的凭据 ==="
ssh -o StrictHostKeyChecking=no -i $K root@10.20.2.163 'bash -s' <<'EOS' 2>&1
set -u
p=$(pgrep -x rustfs | head -1)
echo "  pid=$p"
if [ -n "$p" ]; then
  tr '\0' '\n' < /proc/$p/environ | grep -E 'RUSTFS_(ACCESS|SECRET)' | sed -E 's/(=.{6}).*/\1…/' | sed 's/^/  /'
fi
echo "  健康: $(curl -s -o /dev/null -w '%{http_code}' --max-time 3 http://127.0.0.1:9000/health)"
EOS
echo "=== client 凭据 ==="
ssh -o StrictHostKeyChecking=no -i $K root@10.20.2.161 'bash -s' <<'EOS' 2>&1
set -u
echo "  access: $(cut -c1-6 /etc/ax-c1/rustfs-access)…"
echo "  secret: $(cut -c1-6 /etc/ax-c1/rustfs-secret)…"
EOS
echo "=== 直接用 client 凭据调 S3 ==="
ssh -o StrictHostKeyChecking=no -i $K root@10.20.2.161 'bash -s' <<'EOS' 2>&1
set -u
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
aws --version >/dev/null 2>&1 || { echo "  无 aws cli"; exit 0; }
AWS_ACCESS_KEY_ID=$AK AWS_SECRET_ACCESS_KEY=$SK AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url http://10.20.2.163:9000 s3 ls 2>&1 | head -3 | sed 's/^/  /'
EOS
