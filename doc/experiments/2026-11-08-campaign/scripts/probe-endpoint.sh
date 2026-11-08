#!/bin/bash
# 探测模型端点是否恢复（/responses 需要 POST，用 curl 试一次最小请求）
set -u
TOKEN=$(grep experimental_bearer_token /root/.codex/config.toml | sed 's/.*= *"//; s/"//')
URL=$(grep base_url /root/.codex/config.toml | sed 's/.*= *"//; s/"//')
echo "  base_url=$URL  token=${TOKEN:0:12}…"
echo "  --- POST /responses 探针 ---"
CODE=$(curl -s -o /tmp/probe.out -w '%{http_code}' --max-time 20 \
  -X POST "$URL/responses" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-6-astra","input":"ping","stream":false}' 2>/dev/null)
echo "  HTTP=$CODE"
head -c 300 /tmp/probe.out 2>/dev/null | sed 's/^/  body: /'
