#!/bin/bash
# 探测 OSS 上是否还有 ripgrep-20261026 / archive-io-20261026 两份 trace
set -u
B=https://brewfs-agent-traces-20261023-4ccdae73.oss-cn-hangzhou-internal.aliyuncs.com
for p in \
  full-fuse/20261023/deepseek-opencode-20261026/ripgrep-20261026.tar.gz \
  full-fuse/20261023/deepseek-opencode-20261026/archive-io-20261026.tar.gz \
  full-fuse/20261023/ripgrep-20261026.tar.gz \
  full-fuse/20261023/archive-io-20261026.tar.gz ; do
  code=$(curl -s -o /dev/null -w '%{http_code}' -I --max-time 10 "$B/$p")
  size=$(curl -s -I --max-time 10 "$B/$p" | awk 'BEGIN{IGNORECASE=1}/content-length/{print $2}' | tr -d '\r')
  printf '  %-70s http=%s size=%s\n' "$p" "$code" "${size:-?}"
done
