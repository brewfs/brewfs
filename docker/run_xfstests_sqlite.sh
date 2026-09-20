#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
printf 'warning: docker/run_xfstests_sqlite.sh is deprecated; use docker/kvm-xfstests/run_xfstests_sqlite.sh\n' >&2
exec "$SCRIPT_DIR/kvm-xfstests/run_xfstests_sqlite.sh" "$@"
