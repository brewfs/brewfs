#!/bin/bash
set -u
echo "=== jump 能否装 redis ==="
command -v redis-server 2>/dev/null || echo "  无 redis-server"
(apt-get install -y --no-install-recommends redis-server >/tmp/apt.log 2>&1 && echo "  apt 安装成功") || tail -3 /tmp/apt.log | sed 's/^/  apt: /'
command -v redis-server 2>/dev/null | sed 's/^/  path: /'
echo "=== jump 私网 IP ==="
hostname -I | sed 's/^/  /'
