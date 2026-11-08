#!/bin/bash
set -u
TOKEN=$(grep experimental_bearer_token /root/.codex/config.toml | sed 's/.*= *"//; s/"//')
BASE=http://130.33.96.80:8080
probe() { # $1 path  $2 body
  CODE=$(curl -s -o /tmp/pb.out -w '%{http_code}' --max-time 25 -X POST "$BASE$1" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d "$2" 2>/dev/null)
  printf '  POST %-22s -> %s | %s\n' "$1" "$CODE" "$(head -c 160 /tmp/pb.out | tr -d '\n')"
}
probe /v1/chat/completions '{"model":"gpt-6-astra","messages":[{"role":"user","content":"ping"}],"max_tokens":8}'
probe /chat/completions   '{"model":"gpt-6-astra","messages":[{"role":"user","content":"ping"}],"max_tokens":8}'
probe /v1/completions     '{"model":"gpt-6-astra","prompt":"ping","max_tokens":8}'
echo
echo "  模型清单（前 3 个 id）: $(curl -s --max-time 10 -H "Authorization: Bearer $TOKEN" $BASE/v1/models | grep -o '"id":"[^"]*"' | head -3 | tr '\n' ' ')"
