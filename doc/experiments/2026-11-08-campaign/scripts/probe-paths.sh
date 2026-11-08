#!/bin/bash
set -u
TOKEN=$(grep experimental_bearer_token /root/.codex/config.toml | sed 's/.*= *"//; s/"//')
for path in /v1/models /v1/responses /responses; do
  CODE=$(curl -s -o /tmp/p.out -w '%{http_code}' --max-time 15 \
    -X POST "http://130.33.96.80:8080${path}" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d '{"model":"gpt-6-astra","input":"ping","stream":false}' 2>/dev/null)
  CT=$(head -c 60 /tmp/p.out 2>/dev/null | tr -d '\n')
  printf '  POST %-14s -> %s | %s\n' "$path" "$CODE" "$CT"
done
echo
echo "  GET /v1/models: $(curl -s -o /tmp/g.out -w '%{http_code}' --max-time 10 -H "Authorization: Bearer $TOKEN" http://130.33.96.80:8080/v1/models) | $(head -c 120 /tmp/g.out | tr -d '\n')"
