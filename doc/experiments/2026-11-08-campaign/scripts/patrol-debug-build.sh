#!/bin/bash
set -u
echo "  log: $(stat -c%s /home/hxy/build-seal-debug.log 2>/dev/null || echo 0) bytes"
tail -3 /home/hxy/build-seal-debug.log 2>/dev/null | cut -c1-110 | sed 's/^/  /'
echo "  cargo=$(pgrep -fc 'cargo build' || echo 0) rustc=$(pgrep -fc rustc || echo 0)"
free -g | head -2 | sed 's/^/  /'
df -h /home/hxy | tail -1 | sed 's/^/  /'
