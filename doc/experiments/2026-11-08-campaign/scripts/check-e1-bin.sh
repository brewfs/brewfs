#!/bin/bash
set -u
K=/home/hxy/.ssh/id_ed25519
SSHO="ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i $K"
echo "=== $(date '+%H:%M:%S') e1 二进制状态 ==="
$SSHO hxy@172.24.86.194 'bash -lc "
  echo \"  rustc=\$(pgrep -cx rustc || echo 0)\"
  ps -C rustc -o etime,pcpu,rss --no-headers 2>/dev/null | head -2 | awk \"{printf \\\"    etime=%s cpu=%s%% rss=%.1fG\\n\\\", \\\$1, \\\$2, \\\$3/1048576}\"
  echo \"  内存: \$(free -g | awk \"/Mem:/{print \\\$3\\\"G used / \\\"\\\$7\\\"G avail\\\"}\")\"
  echo \"  --- 二进制 ---\"
  ls -l /home/hxy/e1-target/release/brewfs 2>/dev/null | awk \"{printf \\\"    %.0f MB  %s %s\\n\\\", \\\$5/1048576, \\\$6, \\\$7}\" || echo \"    未生成\"
  echo \"  --- cargo 日志尾 3 ---\"
  tail -3 /home/hxy/logs/e1-cargo.log 2>/dev/null | cut -c1-100 | sed \"s/^/    /\"
"' 2>/dev/null | sed 's/^/  /'
