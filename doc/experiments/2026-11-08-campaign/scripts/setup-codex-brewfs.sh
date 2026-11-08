#!/bin/bash
# 给 brewfs 上的 hxy 用户配置 codex（DeepSeek），再起 agent
set -u
DSK="$1"
mkdir -p /home/hxy/.codex
cat > /home/hxy/.codex/config.toml <<EOF
sandbox_mode = "danger-full-access"
approval_policy = "never"
model_provider = "dpsk"
model = "deepseek-flash"
model_reasoning_effort = "high"

[model_providers.dpsk]
name = "dpsk"
wire_api = "responses"
requires_openai_auth = false
base_url = "https://api.deepseek.com/v1"
experimental_bearer_token = "$DSK"

[tui]
EOF
chown -R hxy:hxy /home/hxy/.codex
chmod 600 /home/hxy/.codex/config.toml
echo "  config: $(ls -l /home/hxy/.codex/config.toml | awk '{print $1, $5}')"

# 冒烟测试
cd /home/hxy/brewfs-main
setpriv --reuid=1000 --regid=1000 --clear-groups env HOME=/home/hxy \
  timeout 120 codex exec --sandbox danger-full-access --cd /home/hxy/brewfs-main \
  -c model_reasoning_effort=high "Reply with exactly: CODEX_OK" </dev/null 2>&1 | tail -5
