#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
printf 'warning: docker/run_perf_etcd.sh is deprecated; use docker/compose-xfstests/run_etcd_perf.sh\n' >&2
exec "$SCRIPT_DIR/compose-xfstests/run_etcd_perf.sh" "$@"
