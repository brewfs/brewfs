#!/bin/bash
set -u
BIN=/srv/ax-data/artifacts/bin/full-fuse-replay
C=/srv/ax-data/traces/extracted/django
ROOT=/mnt/juicefs-c2/django-01
OUT=/srv/ax-data/c2/django-replay-long
rm -rf "$ROOT" 2>/dev/null
mkdir -p "$ROOT"
cp -a "$C/initial/." "$ROOT/" 2>/dev/null
echo "start $(date -u +%FT%TZ) files=$(find "$ROOT" -type f | wc -l)" > "$OUT.out"
timeout 10800 "$BIN" "$C/capture/events.jsonl" "$C/capture/blobs" "$ROOT" "$OUT.json" >> "$OUT.out" 2>&1
echo "exit=$? $(date -u +%FT%TZ)" >> "$OUT.out"
