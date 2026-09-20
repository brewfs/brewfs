#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
printf 'warning: docker/run_perf_redis.sh is deprecated; use docker/compose-xfstests/run_redis_perf.sh\n' >&2
exec "$SCRIPT_DIR/compose-xfstests/run_redis_perf.sh" "$@"
