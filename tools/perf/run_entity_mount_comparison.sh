#!/usr/bin/env bash
# Preliminary real-FUSE WRITE/READBACK comparison. Never builds binaries.
# Uses the existing smallfiles_rw workload; this is not admission/GC qualification.
set -Eeuo pipefail
umask 077

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT/docker/compose-xfstests/docker-compose.packed-lifecycle-backends.yml"
DIAGNOSTIC_HELPER="$ROOT/tools/perf/entity_redis_diagnostics.py"
PRESERVED_FIXTURE=/mnt/brewfs-data/artifacts/overlay-packed-lifecycle-e980d59-20261012T153232Z/build/r31/packed_v3_snapshot_fixture

usage() {
    cat <<'EOF'
Usage: ENTITY_MOUNT_ARTIFACT_ROOT=/absolute/path ENTITY_MOUNT_BINARY=/release/brewfs \
  bash tools/perf/run_entity_mount_comparison.sh

First mounts each available arm and verifies one create, write, fsync,
readback and unmount. Only then runs 1/16/32 workload workers,
2,000 x 16 KiB files, write + readback, fsync per file, two reversed arm orders:
native/overlay-native/overlay-packed then the reverse. All measurements are preliminary.
overlay-native captures the same quiescent source as the packed fixture, using
the new entity frozen native lower and the shared entity upper.

Requires existing release binaries, Linux FUSE, Python+boto3, Docker Compose,
passwordless sudo, and allow_other support. Owns fresh Redis/RustFS resources.
No build, full benchmark, crash test, admission qualification or GC test runs.

Optional ENTITY_MOUNT_NATIVE_BINARY (defaults to ENTITY_MOUNT_BINARY),
ENTITY_MOUNT_FIXTURE (default preserved packed fixture producer),
ENTITY_MOUNT_SMOKE_ONLY=1 (no comparison), and backend image overrides.
ENTITY_MOUNT_WRITEBACK_MODE defaults to upload_before_commit; commit_before_upload
applies only to the two entity arms. Their fsync then means local durability;
phase throughput excludes final remote drain and is not end-to-end throughput.

Diagnostic mode: ENTITY_MOUNT_DIAGNOSTIC=1 defaults to 200 files and captures
Redis MONITOR, commandstats and instrumented overlay timer snapshots per phase.
Mount process CPU and Redis INFO CPU are sampled separately from elapsed spans.
Requires a binary supporting BREWFS_ENTITY_DIAGNOSTICS=1 and SIGUSR1 snapshots;
the native arm is never signaled. MONITOR/timers perturb timings: diagnostic
throughput is NOT acceptance evidence. Smokes and workload semantics are unchanged.
ENTITY_MOUNT_FILES, ENTITY_MOUNT_WORKERS (comma/space list), and
ENTITY_MOUNT_REPETITIONS override the profile; ordinary defaults remain
2000 files, workers 1,16,32 and 2 repetitions. Odd/even repetitions reverse arms.
At most 9 native cells fit the runner's distinct Redis databases.
EOF
}
if [[ "${1:-}" == --help || "${1:-}" == -h ]]; then usage; exit 0; fi
[[ $# == 0 ]] || { usage >&2; exit 2; }
diagnostic="${ENTITY_MOUNT_DIAGNOSTIC:-0}"
entity_writeback="${ENTITY_MOUNT_WRITEBACK_MODE:-upload_before_commit}"
case "$entity_writeback" in upload_before_commit|commit_before_upload) ;; *) echo 'invalid ENTITY_MOUNT_WRITEBACK_MODE' >&2; exit 2 ;; esac
settings="$(python3 - "$diagnostic" "${ENTITY_MOUNT_FILES:-}" "${ENTITY_MOUNT_WORKERS:-1,16,32}" "${ENTITY_MOUNT_REPETITIONS:-2}" <<'PY'
import re, sys
diagnostic, files, workers, repetitions = sys.argv[1:]
if diagnostic not in ('0', '1'):
    raise SystemExit('ENTITY_MOUNT_DIAGNOSTIC must be 0 or 1')
files = files or ('200' if diagnostic == '1' else '2000')
workers = re.split(r'[,\s]+', workers.strip())
if not all(re.fullmatch(r'[1-9][0-9]*', value) for value in [files, repetitions, *workers]):
    raise SystemExit('files, workers and repetitions must be positive decimal integers')
if len(set(workers)) != len(workers) or len(workers) * int(repetitions) > 9:
    raise SystemExit('workers must be unique and workers x repetitions must be <= 9 (fresh native Redis DBs 7..15)')
print(files, repetitions, ','.join(workers))
PY
)"
read -r file_count repetitions worker_csv <<<"$settings"
IFS=, read -ra worker_values <<<"$worker_csv"
measurement_kind=preliminary
[[ "$diagnostic" != 1 ]] || measurement_kind=diagnostic
OUT="${ENTITY_MOUNT_ARTIFACT_ROOT:?set absolute ENTITY_MOUNT_ARTIFACT_ROOT}"
BINARY="${ENTITY_MOUNT_BINARY:?set ENTITY_MOUNT_BINARY to the existing release binary}"
NATIVE="${ENTITY_MOUNT_NATIVE_BINARY:-$BINARY}"
FIXTURE="${ENTITY_MOUNT_FIXTURE:-$PRESERVED_FIXTURE}"
[[ "$OUT" == /* && "$BINARY" == /* && "$NATIVE" == /* && "$FIXTURE" == /* ]] || {
    echo 'artifact/binary paths must be absolute' >&2; exit 2;
}
[[ -x "$BINARY" && -x "$NATIVE" && -x "$FIXTURE" && -e /dev/fuse ]] || {
    echo 'existing binaries, fixture and /dev/fuse required' >&2; exit 2;
}
for command in python3 docker git sha256sum mountpoint fusermount3 timeout sudo; do
    command -v "$command" >/dev/null || { echo "missing command: $command" >&2; exit 2; }
done
sudo -n true
python3 -c 'import boto3, botocore.config'
DOCKER=(docker)
if ! docker info >/dev/null 2>&1; then
    sudo -n docker info >/dev/null 2>&1
    DOCKER=(sudo -n docker)
fi
"${DOCKER[@]}" compose version >/dev/null

uuid() { python3 -c 'import uuid; print(uuid.uuid4())'; }
run_uuid="$(uuid)"
group="$OUT/entity-mount-$(date -u +%Y%m%dT%H%M%SZ)-$run_uuid"
mkdir -p "$OUT"
mkdir "$group"
mkdir "$group/runs"
project="brewfs-entity-perf-$run_uuid"
work="$(mktemp -d /tmp/brewfs-entity-perf.XXXXXX)"
env_file="$group/.compose.env"
mount_pid=""
mount_path=""
mount_art=""
mount_arm_name=""
workload_pid=""
monitor_pid=""
all_mounts_ok=1
python3 - "$group/run-config.json" "$diagnostic" "$file_count" "$repetitions" "$worker_csv" "$entity_writeback" <<'PY'
import json, pathlib, sys
pathlib.Path(sys.argv[1]).write_text(json.dumps({'diagnostic': sys.argv[2] == '1',
    'files': int(sys.argv[3]), 'repetitions': int(sys.argv[4]),
    'workers': [int(value) for value in sys.argv[5].split(',')],
    'entity_writeback_mode': sys.argv[6]}, indent=2) + '\n')
PY
read -r redis_port rustfs_port < <(python3 - <<'PY'
import socket
sockets = [socket.socket(), socket.socket()]
for sock in sockets:
    sock.bind(('127.0.0.1', 0))
print(*(sock.getsockname()[1] for sock in sockets))
PY
)
access="entity$(python3 -c 'import secrets; print(secrets.token_hex(8))')"
secret="$(python3 -c 'import secrets; print(secrets.token_hex(24))')"
printf '%s\n' \
    "PACKED_LIFECYCLE_REDIS_PORT=$redis_port" "PACKED_LIFECYCLE_RUSTFS_PORT=$rustfs_port" \
    "PACKED_LIFECYCLE_REDIS_IMAGE=${PACKED_LIFECYCLE_REDIS_IMAGE:-docker.io/library/redis:7.2-alpine}" \
    "PACKED_LIFECYCLE_RUSTFS_IMAGE=${PACKED_LIFECYCLE_RUSTFS_IMAGE:-w23geq7ncegg6etqsl.xuanyuan.run/rustfs/rustfs:latest}" \
    "PACKED_LIFECYCLE_RUSTFS_ROOT_ACCESS_KEY=$access" "PACKED_LIFECYCLE_RUSTFS_ROOT_SECRET_KEY=$secret" \
    PACKED_LIFECYCLE_PD_PORT=1 PACKED_LIFECYCLE_PD_PEER_PORT=1 \
    PACKED_LIFECYCLE_TIKV_PORT=1 PACKED_LIFECYCLE_TIKV_STATUS_PORT=1 >"$env_file"
unset PACKED_LIFECYCLE_REDIS_PORT PACKED_LIFECYCLE_RUSTFS_PORT \
    PACKED_LIFECYCLE_REDIS_IMAGE PACKED_LIFECYCLE_RUSTFS_IMAGE \
    PACKED_LIFECYCLE_RUSTFS_ROOT_ACCESS_KEY PACKED_LIFECYCLE_RUSTFS_ROOT_SECRET_KEY \
    PACKED_LIFECYCLE_PD_PORT PACKED_LIFECYCLE_PD_PEER_PORT \
    PACKED_LIFECYCLE_TIKV_PORT PACKED_LIFECYCLE_TIKV_STATUS_PORT
printf 'services:\n  rustfs:\n    healthcheck:\n      disable: true\n' >"$work/health-override.yaml"
compose=("${DOCKER[@]}" compose --env-file "$env_file" -f "$COMPOSE_FILE" -f "$work/health-override.yaml" \
    -p "$project" --profile redis --profile rustfs)
endpoint="http://127.0.0.1:$rustfs_port"
redis_url="redis://127.0.0.1:$redis_port"
export AWS_ACCESS_KEY_ID="$access" AWS_SECRET_ACCESS_KEY="$secret"
export AWS_DEFAULT_REGION=us-east-1 AWS_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
unset AWS_SESSION_TOKEN AWS_PROFILE AWS_DEFAULT_PROFILE
export HTTP_PROXY= HTTPS_PROXY= ALL_PROXY= http_proxy= https_proxy= all_proxy=
export NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost
unset BREWFS_WORKSPACE_ROW_CACHE_TTL_MS

stop_monitor() {
    [[ -n "$monitor_pid" ]] || return 0
    local monitor_exit=0
    for _ in {1..600}; do kill -0 "$monitor_pid" 2>/dev/null || break; sleep .05; done
    # Normal capture exits after observing the phase's final ECHO. This signal
    # only stops an interrupted/incomplete capture; the helper returns failure.
    kill -TERM "$monitor_pid" 2>/dev/null || true
    wait "$monitor_pid" 2>/dev/null || monitor_exit=$?
    monitor_pid=""
    return "$monitor_exit"
}

start_monitor() {
    local phase="$1" end_marker="$2"
    python3 "$DIAGNOSTIC_HELPER" capture --port "$redis_port" \
        --output "$phase/redis-monitor.txt" --ready "$phase/monitor-ready.json" \
        --status "$phase/monitor-capture.json" --end-marker "$end_marker" \
        >"$phase/monitor-capture.log" 2>&1 &
    monitor_pid=$!
    for _ in {1..200}; do
        [[ ! -e "$phase/monitor-ready.json" ]] || return 0
        kill -0 "$monitor_pid" 2>/dev/null || break
        sleep .05
    done
    echo "Redis MONITOR did not become ready: $phase" >&2
    return 1
}

stop_mount() {
    [[ -n "$mount_pid" ]] || return 0
    local result=0 forced=0 process_exit=0 mounted=false alive=false attempts=300 started=$SECONDS
    [[ "$mount_arm_name" != overlay-* ]] || attempts=1800
    if kill -0 "$mount_pid" 2>/dev/null; then
        kill -TERM "$mount_pid" 2>/dev/null || true
        for ((attempt=0; attempt<attempts; attempt++)); do kill -0 "$mount_pid" 2>/dev/null || break; sleep .1; done
    fi
    if mountpoint -q "$mount_path"; then fusermount3 -u "$mount_path" || result=1; fi
    for _ in {1..300}; do kill -0 "$mount_pid" 2>/dev/null || break; sleep .1; done
    if kill -0 "$mount_pid" 2>/dev/null; then
        forced=1
        kill -KILL "$mount_pid" 2>/dev/null || true
    fi
    wait "$mount_pid" 2>/dev/null || process_exit=$?
    mountpoint -q "$mount_path" && mounted=true
    kill -0 "$mount_pid" 2>/dev/null && alive=true
    if (( forced != 0 || process_exit != 0 )) || [[ "$mounted" != false || "$alive" != false ]]; then result=1; fi
    if [[ "$mount_arm_name" == overlay-* ]]; then
        python3 - "$mount_art/brewfs.log" "$mount_art/remote-ready.json" <<'PY' || result=1
import json, pathlib, sys
records = [json.loads(line.split('=', 1)[1]) for line in pathlib.Path(sys.argv[1]).read_text().splitlines() if line.startswith('entity_stopped=')]
if len(records) != 1 or not records[0].get('remote_ready'):
    raise SystemExit('entity mount did not complete remote drain')
pathlib.Path(sys.argv[2]).write_text(json.dumps(records[0], indent=2) + '\n')
PY
    fi
    printf 'pid=%s\nprocess_exit=%s\nforced_kill=%s\nprocess_alive=%s\nmounted=%s\n' \
        "$mount_pid" "$process_exit" "$forced" "$alive" "$mounted" >"$mount_art/unmount-proof.txt"
    printf 'shutdown_wall_seconds=%s\n' "$((SECONDS-started))" >>"$mount_art/unmount-proof.txt"
    (( result == 0 )) || all_mounts_ok=0
    mount_pid=""
    mount_path=""
    mount_art=""
    mount_arm_name=""
    return "$result"
}

cleanup() {
    local status=$?
    trap - EXIT INT TERM
    set +e
    if [[ -n "$workload_pid" ]]; then
        sudo -n kill -TERM -- "-$workload_pid" 2>/dev/null
        for _ in {1..50}; do kill -0 "$workload_pid" 2>/dev/null || break; sleep .1; done
        sudo -n kill -KILL -- "-$workload_pid" 2>/dev/null
        wait "$workload_pid" 2>/dev/null
    fi
    stop_monitor || status=1
    stop_mount || status=1
    "${compose[@]}" logs --no-color >"$group/backend-services.log" 2>&1
    "${compose[@]}" down --timeout 30 -v --remove-orphans >"$group/compose-down.log" 2>&1
    local down=$? containers volumes container_rc volume_rc mounts_left
    containers="$("${DOCKER[@]}" ps -aq --filter "label=com.docker.compose.project=$project" 2>"$group/container-query.log")"
    container_rc=$?
    volumes="$("${DOCKER[@]}" volume ls -q --filter "label=com.docker.compose.project=$project" 2>"$group/volume-query.log")"
    volume_rc=$?
    mounts_left="$(python3 - "$work" <<'PY'
import sys
print(sum(line.split()[1].startswith(sys.argv[1] + '/') for line in open('/proc/mounts')))
PY
)"
    printf 'compose_down_exit=%s\ncontainer_query_exit=%s\nvolume_query_exit=%s\ncontainers_left=%s\nvolumes_left=%s\nfuse_mounts_left=%s\nall_mount_processes_clean=%s\n' \
        "$down" "$container_rc" "$volume_rc" "$containers" "$volumes" "$mounts_left" "$all_mounts_ok" >"$group/cleanup-proof.txt"
    if (( down != 0 || container_rc != 0 || volume_rc != 0 || all_mounts_ok != 1 )) || [[ -n "$containers$volumes" || "$mounts_left" != 0 ]]; then status=1; fi
    rm -f "$env_file"
    [[ "$mounts_left" == 0 ]] && rm -rf "$work"
    python3 - "$group" "$status" "${ENTITY_MOUNT_SMOKE_ONLY:-0}" <<'PY'
import json, pathlib, statistics, sys
group = pathlib.Path(sys.argv[1]); exit_status = int(sys.argv[2]); smoke_only = sys.argv[3] == '1'
config = json.loads((group / 'run-config.json').read_text())
kind = 'diagnostic' if config['diagnostic'] else 'preliminary'
expected_files = config['files']
phase_slots = len(config['workers']) * config['repetitions'] * 2
rows = []
for directory in sorted((group / 'runs').glob('*')):
    result = directory / 'result.json'
    if result.exists():
        cell = json.loads(result.read_text())
    else:
        worker, repetition, arm = directory.name.split('-', 2)
        cell = {'arm': arm, 'repetition': int(repetition[1:]), 'exit': None}
    for mode in ('write', 'readback'):
        summary = directory / mode / 'summary.json'
        if summary.exists():
            row = json.loads(summary.read_text())
            row.update(status=kind, arm=cell['arm'], repetition=cell['repetition'], cell_exit=cell['exit'], artifact=str(summary.relative_to(group)))
            diagnostic = directory / mode / 'redis-diagnostic.json'
            if diagnostic.exists():
                row['redis_diagnostic'] = str(diagnostic.relative_to(group))
            rows.append(row)
expected = 0 if smoke_only else phase_slots * 3
def valid(row):
    return row['cell_exit'] == 0 and row['errors'] == 0 and row['files'] == expected_files and row['expected_files'] == expected_files
identity_ok = all(len({r['trace_sha256'] for r in rows if r['mode'] == mode and valid(r)}) <= 1 for mode in ('write', 'readback'))
complete = len(rows) == expected and all(valid(r) for r in rows) and identity_ok
smokes = {}
for arm in ('native', 'overlay-native', 'overlay-packed'):
    smoke = group / 'smoke' / arm / 'smoke.json'
    smokes[arm] = json.loads(smoke.read_text()) if smoke.exists() else {'status': 'not-completed'}
smoke_ok = all(smoke.get('status') == 'passed' for smoke in smokes.values())
status = f'completed-available-{kind}' if exit_status == 0 and complete and smoke_ok else 'incomplete' if len(rows) < expected else 'failed'
medians = {}
for workers in config['workers']:
    for mode in ('write', 'readback'):
        values = {}
        for arm in ('native', 'overlay-native', 'overlay-packed'):
            rs = [r for r in rows if r['workers'] == workers and r['mode'] == mode and r['arm'] == arm and valid(r)]
            values[arm] = {'status': kind, 'valid': len(rs), 'files_per_sec': statistics.median(r['files_per_sec'] for r in rs) if rs else None}
        medians[f'{workers}/{mode}'] = values
result = {'schema': 'brewfs-entity-mount-preliminary-v1', 'status': status, 'exit': exit_status,
          'measurement_kind': kind, 'profile': config,
          'requested_rows': 0 if smoke_only else phase_slots * 3, 'expected_available_rows': expected,
          'skipped_overlay_native_phase_rows': 0,
          'rows': rows, 'medians': medians, 'smoke_ok': smoke_ok, 'smokes': smokes,
          'skipped_arms': [],
          'note': 'Experimental entity mounts; not production admission, crash-recovery or GC qualification. All arms use 4 MiB chunks. Phase throughput excludes final remote drain; commit_before_upload fsync is local durability. Diagnostic runs include MONITOR/timer overhead and cannot establish throughput acceptance.'}
(group / 'summary.json').write_text(json.dumps(result, indent=2) + '\n')
print(f'entity-mount: {status}; artifacts={group}; available_rows={len(rows)}/{expected}; overlay-native=enabled')
sys.exit(0 if status == f'completed-available-{kind}' else 1)
PY
    local summary_rc=$?
    (( summary_rc == 0 )) || { (( status != 0 )) || status=1; }
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

git -C "$ROOT" rev-parse HEAD >"$group/revision.txt"
git -C "$ROOT" status --short >"$group/worktree-status.txt"
git -C "$ROOT" diff HEAD --binary >"$group/tracked-worktree.patch"
python3 - "$ROOT" "$group/rust-source-sha256.json" <<'PY'
import hashlib, json, pathlib, subprocess, sys
root = pathlib.Path(sys.argv[1])
names = subprocess.check_output(['git', '-C', str(root), 'ls-files', '-z', '--cached', '--others', '--exclude-standard', '--', '*.rs'])
files = []
for name in sorted(set(names.decode().split('\0')) - {''}):
    path = root / name
    files.append({'path': name, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None})
pathlib.Path(sys.argv[2]).write_text(json.dumps({'scope': 'tracked and nonignored untracked Rust files; null means absent', 'files': files}, indent=2) + '\n')
PY
sha256sum "$BINARY" "$NATIVE" "$FIXTURE" >"$group/binary.sha256"
sha256sum "$ROOT/tools/perf/run_entity_mount_comparison.sh" "$ROOT/tools/perf/smallfiles_rw.py" "$DIAGNOSTIC_HELPER" "$COMPOSE_FILE" >"$group/runner.sha256"
cp "$ROOT/tools/perf/run_entity_mount_comparison.sh" "$group/runner.sh"
if [[ "$diagnostic" == 1 ]]; then
    cp "$DIAGNOSTIC_HELPER" "$group/entity_redis_diagnostics.py"
    python3 "$DIAGNOSTIC_HELPER" inventory --root "$ROOT" --output "$group/redis-script-inventory.json"
fi
cat >"$group/profile.txt" <<EOF
status=$measurement_kind
redis_monitor=$diagnostic
overlay_timer_snapshots=$diagnostic
process_and_redis_cpu_snapshots=$diagnostic
overlay_packed_binary=$BINARY
overlay_native_binary=$BINARY
native_binary=$NATIVE
fixture_binary=$FIXTURE
fixture_role=immutable packed fixture producer only; not a measured filesystem arm
project=$project
workers=$worker_csv
repetitions=$repetitions
orders=odd:native,overlay-native,overlay-packed;even:overlay-packed,overlay-native,native
requested_phase_rows=$((${#worker_values[@]} * repetitions * 6))
expected_available_phase_rows=$((${#worker_values[@]} * repetitions * 6))
skipped_overlay_native_phase_rows=0
overlay_native_status=enabled
entity_writeback_mode=$entity_writeback
files=$file_count
file_bytes=16384
fsync_per_file=1
workload_uid=0
chunk_size=4194304
block_size=4194304
fuse_workers=16
fuse_max_background=256
metadata_ttl_ms=0
direct_io=1
kernel_writeback=0
overlay_packed_default_permissions=0
native_default_permissions=1
overlay_packed_max_read=1048576
overlay_packed_max_write=1048576
overlay_packed_max_readahead=0
native_max_write=4194304
native_default_max_read=4194304
native_max_readahead=16777216
request_cap_note=all requests in this 16KiB workload are below both adapters' request caps
read_payload_caches=0
prefetch=0
compression=none
overlay_packed_noatime=1
native_atime=existing_behavior
cache_note=both entity arms use direct I/O and $entity_writeback; native uses existing VFS writeback and metadata caches; fsync per file in all arms
durability_note=commit_before_upload fsync is local durability; phase throughput excludes final remote drain
EOF
printf '[entity-mount] starting owned services; artifacts=%s\n' "$group"
"${compose[@]}" up -d --wait --wait-timeout 180 redis rustfs >"$group/backend-start.log" 2>&1
python3 - "$endpoint" "$("${compose[@]}" ps -q rustfs)" "${DOCKER[@]}" >"$group/health.log" 2>&1 <<'PY'
import subprocess, sys, time, urllib.request
endpoint, container = sys.argv[1:3]
for attempt in range(90):
    if subprocess.check_output(sys.argv[3:] + ['inspect', '--format', '{{.State.Running}}', container]).strip() != b'true':
        raise SystemExit('owned RustFS process exited')
    try:
        with urllib.request.urlopen(endpoint + '/health', timeout=2) as response:
            if response.status == 200: break
    except OSError: pass
    time.sleep(1)
else: raise SystemExit('owned RustFS health endpoint timed out')
PY
while IFS= read -r container; do
    "${DOCKER[@]}" inspect --format '{{.Name}} {{.Image}}' "$container"
done < <("${compose[@]}" ps -q redis rustfs) >"$group/backend-image-ids.txt"
python3 - "$endpoint" <<'PY'
import boto3, botocore.config, sys
client = boto3.client('s3', endpoint_url=sys.argv[1], region_name='us-east-1',
                      config=botocore.config.Config(s3={'addressing_style': 'path'}))
client.create_bucket(Bucket='entity-packed')
PY
mkdir "$work/source"
python3 - "$work/source/lower.bin" <<'PY'
import pathlib, sys
pathlib.Path(sys.argv[1]).write_bytes(bytes(range(256)) * 64)
PY
"$FIXTURE" --source-directory "$work/source" --output-dir "$work/producer-spool" --source-consistency best-effort-detected \
    --source-hardlink-policy visible-links --bucket entity-packed --endpoint "$endpoint" \
    --region us-east-1 --force-path-style true --prefix comparison \
    --manifest-output "$group/manifest-key.txt" --metadata-codec raw --data-codec raw \
    --inline-data off --chunk-size 4194304 --block-size 4194304 >"$group/packed-fixture.log" 2>&1
manifest="$(cat "$group/manifest-key.txt")"

data_yaml() {
    cat <<EOF
data:
  backend: s3
  s3:
    bucket: $1
    endpoint: $endpoint
    region: us-east-1
    force_path_style: true
    disable_payload_checksum: true
EOF
}
mount_arm() { # arm tag native-db
    local arm="$1" tag="$2" native_db="$3" binary="$BINARY" bucket=entity-packed db=0 writeback=upload_before_commit
    local ns="entity-$run_uuid-$tag" dir="$work/$tag" art="$group/runs/$tag"
    [[ "$tag" != smoke-* ]] || art="$group/smoke/$arm"
    mkdir -p "$dir/mnt" "$dir/cache" "$art"
    local -a extra=()
    case "$arm" in
        overlay-packed|overlay-native)
            writeback="$entity_writeback"
            extra=(--volume-format workspace-entity-v1 --experimental-entity-mount \
                --entity-volume-id "$(uuid)" --workspace "$(uuid)" \
                --workspace-namespace "$ns")
            if [[ "$arm" == overlay-packed ]]; then extra+=(--packed-manifest-key "$manifest");
            else extra+=(--entity-native-source "$work/source"); fi
            ;;
        native)
            binary="$NATIVE"; bucket="entity-native-$tag"; db="$native_db"; extra=(--volume-format flat-v1)
            python3 - "$endpoint" "$bucket" <<'PY'
import boto3, botocore.config, sys
client = boto3.client('s3', endpoint_url=sys.argv[1], region_name='us-east-1',
                      config=botocore.config.Config(s3={'addressing_style': 'path'}))
client.create_bucket(Bucket=sys.argv[2])
PY
            ;;
        *) echo "unsupported comparison arm: $arm" >&2; return 2 ;;
    esac
    {
        data_yaml "$bucket"
        cat <<EOF
mount_point: $dir/mnt
meta:
  backend: redis
  redis:
    url: "$redis_url/$db"
layout: { chunk_size: 4194304, block_size: 4194304 }
fuse: { workers: 16, max_background: 256, privileged: false }
cache:
  root: $dir/cache
  read_memory_bytes: 0
  read_ssd_bytes: 0
  prefetch_enabled: false
  range_background_prefetch: false
  compression: none
  writeback_mode: $writeback
EOF
    } >"$art/mount.yaml"
    local -a command=("$binary" mount --config "$art/mount.yaml" "${extra[@]}" "$dir/mnt")
    printf '%q ' "${command[@]}" >>"$group/commands.log"
    printf '\n' >>"$group/commands.log"
    mount_path="$dir/mnt"; mount_art="$art"; mount_arm_name="$arm"
    local -a diagnostic_env=()
    if [[ "$diagnostic" == 1 && "$arm" == overlay-* ]]; then
        diagnostic_env=(BREWFS_ENTITY_DIAGNOSTICS=1)
    fi
    env -u BREWFS_ENTITY_DIAGNOSTICS "${diagnostic_env[@]}" \
        BREWFS_CACHE_TTL_MS=0 BREWFS_FUSE_READ_DIRECT_IO=1 BREWFS_FUSE_KEEP_CACHE=0 BREWFS_FUSE_WRITEBACK=0 \
        RUST_LOG=warn,brewfs=info "${command[@]}" >"$art/brewfs.log" 2>&1 &
    mount_pid=$!
    for _ in {1..1200}; do
        mountpoint -q "$mount_path" && break
        kill -0 "$mount_pid" 2>/dev/null || return 1
        sleep .1
    done
    mountpoint -q "$mount_path" || return 1
    if [[ "$arm" == native ]]; then
        sudo -n python3 - "$work/source/lower.bin" "$mount_path/lower.bin" <<'PY'
import os, pathlib, sys
with open(sys.argv[2], 'xb') as target:
    target.write(pathlib.Path(sys.argv[1]).read_bytes())
    target.flush()
    os.fsync(target.fileno())
PY
    fi
}

for arm in native overlay-native overlay-packed; do
    mount_arm "$arm" "smoke-$arm" 1
    sudo -n python3 - "$mount_path" "$group/smoke/$arm/smoke.json" "$arm" <<'PY'
import json, os, pathlib, sys
root = pathlib.Path(sys.argv[1]); payload = bytes(range(256)) * 64
if (root / 'lower.bin').read_bytes() != payload:
    raise RuntimeError('smoke lower readback content mismatch')
fd = os.open(root / 'smoke.bin', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
try:
    remaining = memoryview(payload)
    while remaining:
        written = os.write(fd, remaining)
        if written <= 0:
            raise RuntimeError('write made no progress')
        remaining = remaining[written:]
    os.fsync(fd)
finally:
    os.close(fd)
if (root / 'smoke.bin').read_bytes() != payload:
    raise RuntimeError('smoke readback content mismatch')
pathlib.Path(sys.argv[2]).write_text(json.dumps({'status': 'data-passed-unmount-pending', 'arm': sys.argv[3], 'bytes': len(payload),
    'checks': ['O_EXCL create', 'write', 'fsync', 'exact readback']}) + '\n')
PY
    sudo -n chown "$(id -u):$(id -g)" "$group/smoke/$arm/smoke.json"
    if ! mountpoint -q "$mount_path" || ! kill -0 "$mount_pid" 2>/dev/null; then exit 1; fi
    stop_mount
    python3 - "$group/smoke/$arm/smoke.json" <<'PY'
import json, pathlib, sys
path = pathlib.Path(sys.argv[1])
smoke = json.loads(path.read_text())
smoke.update(status='passed', clean_unmount=True, process_exited=True)
path.write_text(json.dumps(smoke, indent=2) + '\n')
PY
done
[[ "${ENTITY_MOUNT_SMOKE_ONLY:-0}" == 1 ]] && exit 0

native_db=6
for workers in "${worker_values[@]}"; do
    for ((repetition = 1; repetition <= repetitions; repetition++)); do
        if (( repetition % 2 == 1 )); then arms=(native overlay-native overlay-packed); else arms=(overlay-packed overlay-native native); fi
        for arm in "${arms[@]}"; do
            tag="w$workers-r$repetition-$arm"
            art="$group/runs/$tag"
            [[ "$arm" != native ]] || native_db=$((native_db + 1))
            printf '[entity-mount] %s\n' "$tag"
            sync
            sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches'
            mount_arm "$arm" "$tag" "$native_db"
            rc=0
            for mode in write readback; do
                mkdir "$art/$mode"
                if ! mountpoint -q "$mount_path" || ! kill -0 "$mount_pid" 2>/dev/null; then exit 1; fi
                sync
                sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches'
                begin_marker="brewfs-phase:$run_uuid:$tag:$mode:begin"
                end_marker="brewfs-phase:$run_uuid:$tag:$mode:end"
                if [[ "$diagnostic" == 1 ]]; then
                    start_monitor "$art/$mode" "$end_marker"
                fi
                if [[ "$diagnostic" == 1 ]]; then
                    python3 "$DIAGNOSTIC_HELPER" redis-snapshot --section commandstats --port "$redis_port" \
                        --output "$art/$mode/redis-before.txt" \
                        --timing-output "$art/$mode/redis-commandstats-before.timing.json"
                else
                    "${compose[@]}" exec -T redis redis-cli -p "$redis_port" INFO commandstats >"$art/$mode/redis-before.txt"
                fi
                if [[ "$diagnostic" == 1 ]]; then
                    python3 "$DIAGNOSTIC_HELPER" redis-snapshot --section CPU --port "$redis_port" \
                        --output "$art/$mode/redis-cpu-before.txt" \
                        --timing-output "$art/$mode/redis-cpu-before.timing.json"
                    if [[ "$arm" == overlay-* ]]; then
                        python3 "$DIAGNOSTIC_HELPER" snapshot --pid "$mount_pid" \
                            --log "$art/brewfs.log" --output "$art/$mode/entity-before.json"
                    fi
                    python3 "$DIAGNOSTIC_HELPER" process-snapshot --pid "$mount_pid" \
                        --output "$art/$mode/process-cpu-before.json"
                    "${compose[@]}" exec -T redis redis-cli -p "$redis_port" ECHO "$begin_marker" >"$art/$mode/monitor-begin.txt"
                fi
                command=(sudo -n python3 "$ROOT/tools/perf/smallfiles_rw.py" --root "$mount_path" \
                    --subdir rw --mode "$mode" --files "$file_count" --file-bytes 16384 \
                    --workers "$workers" --seed 20261001 --label "$arm" --json-output "$art/$mode/summary.json")
                printf '%q ' "${command[@]}" >>"$group/commands.log"
                printf '\n' >>"$group/commands.log"
                timeout 1800 "${command[@]}" >"$art/$mode/scanner.log" 2>&1 &
                workload_pid=$!
                if wait "$workload_pid"; then step_rc=0; else step_rc=$?; fi
                workload_pid=""
                if ! mountpoint -q "$mount_path" || ! kill -0 "$mount_pid" 2>/dev/null; then step_rc=1; fi
                if [[ "$diagnostic" == 1 ]]; then
                    "${compose[@]}" exec -T redis redis-cli -p "$redis_port" ECHO "$end_marker" >"$art/$mode/monitor-end.txt" || step_rc=1
                    if kill -0 "$mount_pid" 2>/dev/null; then
                        python3 "$DIAGNOSTIC_HELPER" process-snapshot --pid "$mount_pid" \
                            --output "$art/$mode/process-cpu-after.json" || step_rc=1
                    fi
                    if [[ "$arm" == overlay-* ]] && kill -0 "$mount_pid" 2>/dev/null; then
                        python3 "$DIAGNOSTIC_HELPER" snapshot --pid "$mount_pid" \
                            --log "$art/brewfs.log" --output "$art/$mode/entity-after.json" || step_rc=1
                    fi
                    python3 "$DIAGNOSTIC_HELPER" redis-snapshot --section CPU --port "$redis_port" \
                        --output "$art/$mode/redis-cpu-after.txt" \
                        --timing-output "$art/$mode/redis-cpu-after.timing.json" || step_rc=1
                fi
                printf '%s\n' "$step_rc" >"$art/$mode/exit.txt"
                if [[ -e "$art/$mode/summary.json" ]]; then
                    sudo -n chown "$(id -u):$(id -g)" "$art/$mode/summary.json"
                fi
                python3 - "$art/$mode/summary.json" "$measurement_kind" <<'PY'
import json, pathlib, sys
path = pathlib.Path(sys.argv[1])
if path.exists():
    row = json.loads(path.read_text())
    row['status'] = sys.argv[2]
    path.write_text(json.dumps(row, indent=2) + '\n')
PY
                if [[ "$diagnostic" == 1 ]]; then
                    python3 "$DIAGNOSTIC_HELPER" redis-snapshot --section commandstats --port "$redis_port" \
                        --output "$art/$mode/redis-after.txt" \
                        --timing-output "$art/$mode/redis-commandstats-after.timing.json" || step_rc=1
                else
                    "${compose[@]}" exec -T redis redis-cli -p "$redis_port" INFO commandstats >"$art/$mode/redis-after.txt"
                fi
                if [[ "$diagnostic" == 1 ]]; then
                    stop_monitor || step_rc=1
                    if [[ -e "$art/$mode/summary.json" ]]; then
                        python3 "$DIAGNOSTIC_HELPER" analyze --phase "$art/$mode" \
                            --inventory "$group/redis-script-inventory.json" \
                            --begin-marker "$begin_marker" --end-marker "$end_marker" \
                            >"$art/$mode/redis-analysis.log" 2>&1 || step_rc=1
                    else
                        step_rc=1
                    fi
                    printf '%s\n' "$step_rc" >"$art/$mode/exit.txt"
                fi
                (( step_rc == 0 )) || { rc=$step_rc; break; }
            done
            stop_mount || rc=1
            python3 - "$art/result.json" "$arm" "$workers" "$repetition" "$rc" <<'PY'
import json, pathlib, sys
pathlib.Path(sys.argv[1]).write_text(json.dumps({'arm': sys.argv[2], 'workers': int(sys.argv[3]),
    'repetition': int(sys.argv[4]), 'exit': int(sys.argv[5])}) + '\n')
PY
            (( rc == 0 )) || exit "$rc"
        done
    done
done
