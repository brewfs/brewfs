#!/bin/bash
set -u
pkill -9 -f build-seal.sh 2>/dev/null || true
pkill -9 -f 'cargo build' 2>/dev/null || true
pkill -9 -f 'rustc' 2>/dev/null || true
sleep 3
echo "  cargo=$(pgrep -fc cargo || echo 0) rustc=$(pgrep -fc rustc || echo 0)"
free -g | head -2 | sed 's/^/  /'
df -h /home/hxy | tail -1 | sed 's/^/  /'
echo "  seal 代码是否还在: $(cd /home/hxy/brewfs-main && git status --short | wc -l) 个改动文件"
