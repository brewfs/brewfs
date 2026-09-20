#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
printf 'warning: docker/manage_xfstests_backend_services.sh is deprecated; use docker/kvm-xfstests/manage_xfstests_backend_services.sh\n' >&2
exec "$SCRIPT_DIR/kvm-xfstests/manage_xfstests_backend_services.sh" "$@"
