#!/bin/bash
set -u
K=/home/hxy/.ssh/id_ed25519
SSHO="ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i $K"
echo "=== $(date '+%H:%M:%S') #424 编译结局 ==="
$SSHO hxy@172.24.86.194 'bash -lc "
  echo \"--- 内存 ---\"; free -g | sed -n 2p
  echo \"--- 最近的 OOM 记录 ---\"
  sudo -n journalctl --since \"-30min\" --no-pager 2>/dev/null | grep -iE \"oom-kill|killed process\" | tail -4 | cut -c1-150 || echo \"  无\"
  echo \"--- cargo unit 结果 ---\"
  for u in \$(systemctl list-units \"cargo-*\" --all --no-pager --plain 2>/dev/null | awk \"/cargo-/{print \\\$1}\" | tail -3); do
    echo \"  \$u\"; systemctl show \$u -p Result -p ExecMainStatus --no-pager | sed \"s/^/    /\"
  done
  echo \"--- 测试二进制是否存在 ---\"
  find /home/hxy/brewfs-424/target /home/hxy/e1-target -maxdepth 3 -name \"brewfs-*\" -type f -newermt \"-40 minutes\" 2>/dev/null | head -5 | while read f; do printf \"  %6.0f MB %s\\n\" \$(echo \"\$(stat -c%s \$f)/1048576\" | bc) \"\$f\"; done
  echo \"--- gates 脚本输出 ---\"
  ls -lt /tmp/brewfs-424-gates* 2>/dev/null | head -3
"' 2>/dev/null | sed 's/^/  /'
