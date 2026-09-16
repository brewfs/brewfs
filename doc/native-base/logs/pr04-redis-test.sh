#!/usr/bin/env bash
set -u
name="brewfs-pr04-redis-$$"
docker run -d --rm --name "$name" -p 127.0.0.1:16379:6379 redis:7.4-alpine >/dev/null || exit $?
for _ in $(seq 1 30); do
  if docker exec "$name" redis-cli ping 2>/dev/null | grep -q PONG; then
    break
  fi
  sleep 1
done
cd /mnt/d/code/brewfs/.claude/worktrees/vigorous-solomon-7caff5 || exit $?
BREWFS_REDIS_TEST_URL=redis://127.0.0.1:16379 \
CARGO_TARGET_DIR="$HOME/brewfs-target-vigorous-solomon-7caff5" \
"$HOME/.cargo/bin/cargo" test -p brewfs --lib native_base::write::tests_redis -- --ignored
status=$?
docker rm -f "$name" >/dev/null 2>&1 || true
exit "$status"
