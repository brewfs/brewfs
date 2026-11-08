#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
for T in pytest-full-02 django rust; do
  F=/srv/ax-data/traces/extracted/$T/capture/events.jsonl
  [ -f "$F" ] || { echo "  $T: 无 events.jsonl"; continue; }
  n=$(wc -l < "$F")
  xa=$(grep -ac 'xattr' "$F" || true)
  echo "  $T: events=$n 含 xattr 的事件行=$xa"
done
echo
echo "=== pytest 前 3 条 xattr 事件 ==="
grep -a 'xattr' /srv/ax-data/traces/extracted/pytest-full-02/capture/events.jsonl 2>/dev/null | head -3 | cut -c1-200 | sed 's/^/  /'
EOS
