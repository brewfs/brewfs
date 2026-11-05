#!/usr/bin/env bash
set -euo pipefail
source <(sed '/^main "\$@"$/d' /usr/local/bin/run_perf_in_container.sh)
eval "$(declare -f cleanup | sed '1s/cleanup/cleanup_original/')"
cleanup() {
    local previous_pid=''
    [[ ! -f /run/brewfs/perf-mount.pid ]] || previous_pid=$(cat /run/brewfs/perf-mount.pid)
    cleanup_original
    if [[ -n "$previous_pid" ]]; then
        python3 - "$previous_pid" "$artifact_dir/exit-waits.jsonl" <<'PY'
import json, pathlib, sys, time
pid = int(sys.argv[1])
path = pathlib.Path('/proc', str(pid), 'stat')
started = time.monotonic()
while path.exists():
    try:
        if path.read_text().rsplit(')', 1)[1].split()[0] == 'Z':
            break
    except FileNotFoundError:
        break
    time.sleep(.01)
with pathlib.Path(sys.argv[2]).open('a') as stream:
    stream.write(json.dumps(dict(pid=pid, exit_wait_ms=(time.monotonic()-started)*1000))+'\n')
PY
    fi
}
# The changed cleanup protocol is diagnostic only. Preserve the original
# failed artifact and do not present these runs as unchanged-runner passes.
mkdir -p "$artifact_dir"
cat >"$artifact_dir/diagnostic-contract.json" <<'EOF'
{"diagnostic_only":true,"change":"Wait for the previous mount process to exit after original cleanup, before remount.","original_guard_failure":"guard-mixed-r1-candidate-d0: EADDRINUSE before timed fio"}
EOF
main
