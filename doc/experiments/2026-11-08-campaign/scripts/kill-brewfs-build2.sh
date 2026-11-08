#!/bin/bash
set -u
pkill -9 -f 'rustc' 2>/dev/null || true
sleep 3
echo "  rustc: $(pgrep -fc rustc || echo 0)  cargo: $(pgrep -fc 'cargo' || echo 0)"
free -g | head -2 | sed 's/^/  /'
