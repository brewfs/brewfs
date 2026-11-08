#!/bin/bash
# 起 TiKV 容器（供 xattr 测试用）
set -u
K=/home/hxy/.ssh/id_ed25519
SSHO="ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i $K"
echo "=== 1. TiKV 机连通性 ==="
$SSHO hxy@172.24.86.194 'echo "  build 机 OK: $(hostname)"' 2>/dev/null
echo
echo "=== 2. 从 Windows 侧起 TiKV 容器（需要公网）==="
echo "  TiKV 机内网 172.24.86.193 / 公网 47.237.154.110"
