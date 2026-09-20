#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
printf 'warning: docker/install_xfstests_deps.sh is deprecated; use docker/kvm-xfstests/install_xfstests_deps.sh\n' >&2
exec "$SCRIPT_DIR/kvm-xfstests/install_xfstests_deps.sh" "$@"
