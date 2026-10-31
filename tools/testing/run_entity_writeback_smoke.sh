#!/usr/bin/env bash
# Four normal FUSE cells plus two blocked-remote/process-crash recovery cells.
# Uses only owned loopback services. No performance numbers are produced.
set -Eeuo pipefail
umask 077
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
metadata="${ENTITY_SMOKE_META:-redis}"
[[ "$metadata" == redis || "$metadata" == tikv ]] || { echo 'ENTITY_SMOKE_META must be redis or tikv' >&2; exit 2; }
: "${ENTITY_SMOKE_ARTIFACTS:?set an absolute artifact directory}"
[[ "$ENTITY_SMOKE_ARTIFACTS" == /* ]]
if [[ "${ENTITY_SMOKE_TEST_ONLY:-0}" == 1 ]]; then
    : "${ENTITY_SMOKE_TEST_BINARY:?test-only mode requires a library test executable}"
else
    : "${ENTITY_SMOKE_BINARY:?set the workspace-overlay brewfs binary}"
    : "${ENTITY_SMOKE_FIXTURE:?set the packed_v3_snapshot_fixture binary}"
    [[ -x "$ENTITY_SMOKE_BINARY" && -x "$ENTITY_SMOKE_FIXTURE" && -c /dev/fuse ]]
    if [[ "$metadata" == tikv ]]; then
        : "${ENTITY_SMOKE_PROBE:?TiKV smoke requires the entity_metadata_probe example}"
        [[ -x "$ENTITY_SMOKE_PROBE" ]]
    fi
fi
for tool in python3 docker fusermount3 mountpoint timeout; do command -v "$tool" >/dev/null; done
DOCKER=(docker)
if ! docker info >/dev/null 2>&1; then DOCKER=(sudo -n docker); fi
run="$(python3 -c 'import uuid; print(uuid.uuid4())')"
project="brewfs-entity-cbu-$run"
art="$ENTITY_SMOKE_ARTIFACTS/$run"
mkdir -p "$art"
work="$(mktemp -d /tmp/brewfs-entity-cbu.XXXXXXXX)"
mount_pid="" mount_path="" paused_container=""
read -r redis_port object_port pd_port pd_peer_port tikv_port tikv_status_port < <(python3 - <<'PY'
import socket
s = [socket.socket() for _ in range(6)]
for x in s: x.bind(('127.0.0.1', 0))
print(*(x.getsockname()[1] for x in s))
PY
)
export AWS_ACCESS_KEY_ID="entity$(python3 -c 'import secrets; print(secrets.token_hex(8))')"
export AWS_SECRET_ACCESS_KEY="$(python3 -c 'import secrets; print(secrets.token_hex(24))')"
export AWS_DEFAULT_REGION=us-east-1 AWS_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
export HTTP_PROXY= HTTPS_PROXY= ALL_PROXY= http_proxy= https_proxy= all_proxy=
export NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost
export PACKED_LIFECYCLE_REDIS_PORT="$redis_port" PACKED_LIFECYCLE_RUSTFS_PORT="$object_port"
export PACKED_LIFECYCLE_REDIS_IMAGE="${ENTITY_SMOKE_REDIS_IMAGE:-redis:7.2-alpine}"
export PACKED_LIFECYCLE_RUSTFS_IMAGE="${ENTITY_SMOKE_RUSTFS_IMAGE:-rustfs/rustfs:latest}"
export PACKED_LIFECYCLE_RUSTFS_ROOT_ACCESS_KEY="$AWS_ACCESS_KEY_ID" PACKED_LIFECYCLE_RUSTFS_ROOT_SECRET_KEY="$AWS_SECRET_ACCESS_KEY"
export PACKED_LIFECYCLE_PD_PORT="$pd_port" PACKED_LIFECYCLE_PD_PEER_PORT="$pd_peer_port"
export PACKED_LIFECYCLE_TIKV_PORT="$tikv_port" PACKED_LIFECYCLE_TIKV_STATUS_PORT="$tikv_status_port"
# Pinned minimal RustFS images need not contain curl. Probe their host endpoint
# explicitly instead of depending on an executable inside the server image.
printf 'services:\n  rustfs:\n    healthcheck:\n      disable: true\n' >"$work/health-override.yaml"
compose=("${DOCKER[@]}" compose -f "$ROOT/docker/compose-xfstests/docker-compose.packed-lifecycle-backends.yml" -f "$work/health-override.yaml" -p "$project" --profile redis --profile rustfs)
if [[ "$metadata" == tikv ]]; then compose+=(--profile tikv); fi
cleanup() {
    status=$?
    trap - EXIT INT TERM
    if [[ -n "$paused_container" ]]; then "${DOCKER[@]}" unpause "$paused_container" >/dev/null 2>&1 || true; fi
    if [[ -n "$mount_path" ]] && mountpoint -q "$mount_path"; then fusermount3 -u "$mount_path" || true; fi
    if [[ -n "$mount_pid" ]]; then kill -TERM "$mount_pid" 2>/dev/null || true; wait "$mount_pid" 2>/dev/null || true; fi
    "${compose[@]}" logs --no-color >"$art/services.log" 2>&1 || true
    "${compose[@]}" down -v --remove-orphans >"$art/cleanup.log" 2>&1 || status=1
    # Source/cache files are task-owned; keep failure evidence, including staging.
    printf '%s\n' "$work" >"$art/local-work-directory.txt"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM
"${compose[@]}" up -d --wait --wait-timeout 180 redis rustfs >"$art/services-start.log" 2>&1
if [[ "$metadata" == tikv ]]; then
    "${compose[@]}" up -d pd tikv >>"$art/services-start.log" 2>&1
    python3 "$ROOT/tools/testing/wait_entity_tikv.py" "$pd_port" >>"$art/services-start.log" 2>&1
    export BREWFS_TEST_TIKV_PD_ENDPOINTS="127.0.0.1:$pd_port"
    export BREWFS_TEST_TIKV_STATUS_ENDPOINT="http://127.0.0.1:$tikv_status_port"
else
    unset BREWFS_TEST_TIKV_PD_ENDPOINTS BREWFS_TEST_TIKV_STATUS_ENDPOINT
fi
"${compose[@]}" ps -q | while IFS= read -r container; do
    "${DOCKER[@]}" inspect --format '{{.Name}} {{.Image}}' "$container"
done >"$art/backend-image-ids.txt"
endpoint="http://127.0.0.1:$object_port"
python3 - "$endpoint" "$("${compose[@]}" ps -q rustfs)" "${DOCKER[@]}" >"$art/health.log" 2>&1 <<'PY'
import subprocess, sys, time, urllib.request
endpoint, container = sys.argv[1:3]
for attempt in range(90):
    running = subprocess.check_output(sys.argv[3:] + ['inspect', '--format', '{{.State.Running}}', container]).strip()
    if running != b'true': raise SystemExit('owned RustFS process exited')
    try:
        with urllib.request.urlopen(endpoint + '/health', timeout=2) as response:
            if response.status == 200: break
    except OSError: pass
    time.sleep(1)
else: raise SystemExit('owned RustFS health endpoint timed out')
print('owned RustFS health endpoint passed')
PY
if [[ -n "${ENTITY_SMOKE_TEST_BINARY:-}" ]]; then
    [[ -x "$ENTITY_SMOKE_TEST_BINARY" ]]
    export BREWFS_TEST_REDIS_URL="redis://127.0.0.1:$redis_port/0"
    export BREWFS_PACKED_GRAPH_TEST_S3_ENDPOINT="$endpoint"
    for filter in packed_entity_lower workspace_overlay::entity; do
        export BREWFS_PACKED_GRAPH_TEST_S3_BUCKET="brewfs-graph-test-$(python3 -c 'import uuid; print(uuid.uuid4())')"
        test_args=("$filter" --include-ignored --test-threads=1)
        if [[ "$metadata" != tikv ]]; then test_args+=(--skip tikv_entity); fi
        "$ENTITY_SMOKE_TEST_BINARY" "${test_args[@]}" >"$art/${filter//::/-}.log" 2>&1
    done
fi
if [[ "${ENTITY_SMOKE_TEST_ONLY:-0}" == 1 ]]; then printf 'integration artifacts=%s\n' "$art"; exit 0; fi
bucket="brewfs-entity-smoke-$run"
"${DOCKER[@]}" run --rm --network host -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_EC2_METADATA_DISABLED \
    "${ENTITY_SMOKE_AWS_IMAGE:-amazon/aws-cli:latest}" --region us-east-1 --endpoint-url "$endpoint" \
    s3api create-bucket --bucket "$bucket" >"$art/create-bucket.log" 2>&1
mkdir "$work/source"
python3 - "$work/source" <<'PY'
import os, pathlib, sys
p = pathlib.Path(sys.argv[1])
(p / 'lower.bin').write_bytes(bytes(range(256)) * 32768)
os.link(p / 'lower.bin', p / 'alias.bin')
os.symlink('lower.bin', p / 'link.bin')
os.setxattr(p / 'lower.bin', b'user.entity-test', b'native-and-packed')
PY
"$ENTITY_SMOKE_FIXTURE" --source-directory "$work/source" --output-dir "$work/producer" \
    --source-consistency best-effort-detected --source-hardlink-policy visible-links \
    --bucket "$bucket" --endpoint "$endpoint" --region us-east-1 --force-path-style true \
    --prefix entity-smoke --manifest-output "$art/manifest-key.txt" --metadata-codec raw --data-codec raw \
    --inline-data off --chunk-size 4194304 --block-size 4194304 >"$art/fixture.log" 2>&1
manifest="$(cat "$art/manifest-key.txt")"
snapshot_metadata() {
    if [[ "$metadata" == tikv ]]; then
        printf '{"op":"snapshot","namespace":"%s"}\n' "$namespace" | \
            "$ENTITY_SMOKE_PROBE" --backend tikv --endpoint "127.0.0.1:$pd_port" --volume "$volume" >"$1"
        return
    fi
    python3 - "$1" "$redis_port" "$("${compose[@]}" ps -q redis)" "{brewfs-entity:$volume}/workspace/$namespace/" "${DOCKER[@]}" <<'PY'
import json, pathlib, subprocess, sys
output, port, container, prefix = sys.argv[1:5]
command = sys.argv[5:] + ['exec', container, 'redis-cli', '-p', port]
keys = subprocess.check_output(command + ['--raw', 'KEYS', prefix+'*']).decode().splitlines()
keys = sorted(k for k in keys if k and not k.startswith(prefix+'upload/') and k != prefix+'remote-pending')
values = json.loads(subprocess.check_output(command + ['--json', 'MGET'] + keys))
pathlib.Path(output).write_text(json.dumps(dict(zip(keys, values)), sort_keys=True))
PY
}
for lower in native packed; do
    for scenario in upload_before_commit commit_before_upload crash_recovery; do
        mode="$scenario"
        [[ "$scenario" != crash_recovery ]] || mode=commit_before_upload
        cell="$lower-$scenario"
        mkdir -p "$art/$cell" "$work/$cell/mnt" "$work/$cell/cache"
        mount_path="$work/$cell/mnt"
        volume="$(python3 -c 'import uuid; print(uuid.uuid4())')"
        namespace="$(python3 -c 'import uuid; print(uuid.uuid4())')"
        extra=(--packed-manifest-key "$manifest")
        if [[ "$lower" == native ]]; then extra=(--entity-native-source "$work/source"); fi
        cat >"$art/$cell/mount.yaml" <<EOF
data:
  backend: s3
  s3:
    bucket: $bucket
    endpoint: $endpoint
    region: us-east-1
    force_path_style: true
meta:
  backend: $metadata
  $metadata: $(if [[ "$metadata" == redis ]]; then printf '{ url: "redis://127.0.0.1:%s/0" }' "$redis_port"; else printf '{ pd_endpoints: ["127.0.0.1:%s"] }' "$pd_port"; fi)
layout: { chunk_size: 4194304, block_size: 4194304 }
fuse: { workers: 4, max_background: 64, privileged: false }
cache:
  root: $work/$cell/cache
  writeback_mode: $mode
  write_ssd_bytes: 67108864
EOF
        "$ENTITY_SMOKE_BINARY" mount "$mount_path" --config "$art/$cell/mount.yaml" \
            --volume-format workspace-entity-v1 --experimental-entity-mount \
            --entity-volume-id "$volume" --workspace "$namespace" \
            --workspace-namespace "entity-smoke-$namespace" "${extra[@]}" >"$art/$cell/mount.log" 2>&1 &
        mount_pid=$!
        for _ in {1..1200}; do
            mountpoint -q "$mount_path" && break
            kill -0 "$mount_pid" 2>/dev/null || { cat "$art/$cell/mount.log"; exit 1; }
            sleep .1
        done
        mountpoint -q "$mount_path"
        if [[ "$scenario" == crash_recovery ]]; then
            paused_container="$("${compose[@]}" ps -q rustfs)"
            "${DOCKER[@]}" pause "$paused_container" >/dev/null
            # The worker blocks at remote I/O, while both writes and fsync must
            # complete using the durable spool. Keep this operation bounded.
            timeout 15 python3 - "$mount_path" <<'PY'
import os, pathlib, sys
p = pathlib.Path(sys.argv[1]) / 'crash.bin'
fd = os.open(p, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
assert os.write(fd, b'original') == 8
assert os.pwrite(fd, b'NEW', 0) == 3
os.ftruncate(fd, 3); os.fsync(fd)
assert os.pread(fd, 8, 0) == b'NEW'
os.close(fd)
PY
            kill -KILL "$mount_pid"
            wait "$mount_pid" 2>/dev/null || true
            mount_pid=""
            fusermount3 -u "$mount_path"
            snapshot_metadata "$art/$cell/metadata-before.json"
            "${DOCKER[@]}" unpause "$paused_container" >/dev/null
            paused_container=""
            "$ENTITY_SMOKE_BINARY" mount "$mount_path" --config "$art/$cell/mount.yaml" \
                --volume-format workspace-entity-v1 --experimental-entity-mount \
                --entity-volume-id "$volume" --workspace "$namespace" \
                --workspace-namespace "entity-smoke-$namespace" --entity-recover-uploads >"$art/$cell/recovery.log" 2>&1
            snapshot_metadata "$art/$cell/metadata-after.json"
            for sid in 1 2; do
                "${DOCKER[@]}" run --rm --network host -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_EC2_METADATA_DISABLED \
                    -v "$work:/out" "${ENTITY_SMOKE_AWS_IMAGE:-amazon/aws-cli:latest}" --region us-east-1 --endpoint-url "$endpoint" \
                    s3api get-object --bucket "$bucket" --key "workspace-entity-v1/$volume/$namespace/$sid" \
                    "/out/recovered-$sid" >"$art/$cell/object-$sid.log" 2>&1
            done
            python3 - "$work" "$art/$cell" <<'PY'
import json, pathlib, sys
p, a = map(pathlib.Path, sys.argv[1:])
assert (p/'recovered-1').read_bytes() == b'original'
assert (p/'recovered-2').read_bytes() == b'NEW'
assert (a/'metadata-before.json').read_bytes() == (a/'metadata-after.json').read_bytes()
records = [json.loads(line.split('=',1)[1]) for line in (a/'recovery.log').read_text().splitlines() if line.startswith('entity_upload_recovery=')]
assert len(records) == 1 and records[0]['pending_objects'] == 0
(a/'result.json').write_text(json.dumps({'local_ack_while_remote_blocked': True, 'original_objects_recovered': True, 'namespace_metadata_unchanged': True, 'pending_objects': 0}, indent=2)+'\n')
PY
            printf '%s passed\n' "$cell"
            continue
        fi
        python3 - "$mount_path" "$art/$cell/result.json" <<'PY'
import json, os, pathlib, sys
p = pathlib.Path(sys.argv[1]); original = bytes(range(256)) * 32768
assert (p/'lower.bin').read_bytes() == original
assert (p/'link.bin').read_bytes() == original
assert os.stat(p/'lower.bin').st_ino == os.stat(p/'alias.bin').st_ino
assert os.getxattr(p/'lower.bin', b'user.entity-test') == b'native-and-packed'
with open(p/'lower.bin', 'r+b', buffering=0) as f:
    f.seek(4194301); f.write(b'UPPER-replacement'); os.fsync(f.fileno())
expected = bytearray(original); expected[4194301:4194318] = b'UPPER-replacement'
assert (p/'alias.bin').read_bytes() == expected
with open(p/'new.bin', 'w+b', buffering=0) as f:
    payload = bytes(range(251))*10000
    f.write(payload); os.fsync(f.fileno()); f.seek(0); assert f.read() == payload
    f.truncate(17); f.seek(0); assert f.read() == payload[:17]
    os.unlink(p/'new.bin'); f.seek(0); assert f.read() == payload[:17]
assert not (p/'new.bin').exists()
pathlib.Path(sys.argv[2]).write_text(json.dumps({'data': 'passed', 'bytes': len(payload), 'lower_bytes': len(original), 'cross_chunk_cow': True}))
PY
        kill -TERM "$mount_pid"
        for _ in {1..1800}; do kill -0 "$mount_pid" 2>/dev/null || break; sleep .1; done
        if kill -0 "$mount_pid" 2>/dev/null; then echo 'entity drain timed out' >&2; exit 1; fi
        wait "$mount_pid"
        mount_pid=""
        ! mountpoint -q "$mount_path"
        python3 - "$art/$cell/mount.log" "$art/$cell/result.json" <<'PY'
import json, pathlib, sys
text = pathlib.Path(sys.argv[1]).read_text()
records = [json.loads(line.split('=',1)[1]) for line in text.splitlines() if line.startswith('entity_stopped=')]
assert len(records) == 1 and records[0]['remote_ready']
p = pathlib.Path(sys.argv[2]); result = json.loads(p.read_text()); result['remote_ready'] = True
p.write_text(json.dumps(result, indent=2)+'\n')
PY
        printf '%s passed\n' "$cell"
    done
done
printf 'artifacts=%s\n' "$art"
