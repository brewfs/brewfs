#!/bin/bash
# django 在 BrewFS 上（限时 20 分钟，先看是否会撞 xattr 门槛）
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
BIN=/srv/ax-data/artifacts/bin/full-fuse-replay
C=/srv/ax-data/traces/extracted/django
ROOT=/mnt/bfs-c1fio/c2-django-01
OUT=/srv/ax-data/c2b2/brew-django
mkdir -p /srv/ax-data/c2b2
rm -rf "$ROOT"; mkdir -p "$ROOT"
[ -d "$C/initial" ] && cp -a "$C/initial/." "$ROOT/" 2>/dev/null
echo "start $(date -u +%FT%TZ) files=$(find "$ROOT" -type f | wc -l)"
T0=$(date +%s)
timeout 1200 "$BIN" "$C/capture/events.jsonl" "$C/capture/blobs" "$ROOT" "$OUT.json" > "$OUT.out" 2>&1
RC=$?
T1=$(date +%s)
echo "exit=$RC wall=$((T1-T0))s"
echo "--- 输出尾 ---"
tail -6 "$OUT.out" 2>/dev/null | cut -c1-160 | sed 's/^/  /'
echo "--- 进度行数 ---"
wc -l < "$OUT.out" 2>/dev/null | sed 's/^/  lines: /'
EOS
