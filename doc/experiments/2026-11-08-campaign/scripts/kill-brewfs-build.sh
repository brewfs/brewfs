#!/bin/bash
set -u
pkill -f build-xattr-brewfs.sh 2>/dev/null || true
pkill -f 'cargo build' 2>/dev/null || true
sleep 3
echo "  rustc: $(pgrep -fc rustc || echo 0)"
free -g | head -2 | sed 's/^/  /'
df -h /home/hxy | tail -1 | sed 's/^/  /'
