#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/.." && pwd)
[[ "$root" == /opt/entity-packed-tikv-fanout-v1 ]]
export PATH="$root/venv/bin:$PATH" ENTITY_CLOUD_VALIDATED=1
test -f "$root/local-validation-passed.txt"
test -f "$root/runtime-ready"
test ! -e "$root/work"
mkdir "$root/work"
work="$root/work"
touch "$root/focus.log"
project="entity-tikv-$(python3 -c 'import uuid;print(uuid.uuid4())')"
printf '%s\n' "$project" > "$root/artifacts/compose-project.txt"
read -r redis_port object_port pd_port pd_peer_port tikv_port tikv_status_port proxy_port < <(python3 - <<'PY'
import socket
s=[socket.socket() for _ in range(7)]
for x in s:x.bind(('127.0.0.1',0))
print(*(x.getsockname()[1] for x in s))
PY
)
export PACKED_LIFECYCLE_REDIS_PORT="$redis_port" PACKED_LIFECYCLE_RUSTFS_PORT="$object_port"
export PACKED_LIFECYCLE_PD_PORT="$pd_port" PACKED_LIFECYCLE_PD_PEER_PORT="$pd_peer_port"
export PACKED_LIFECYCLE_TIKV_PORT="$tikv_port" PACKED_LIFECYCLE_TIKV_STATUS_PORT="$tikv_status_port"
export PACKED_LIFECYCLE_REDIS_IMAGE=redis:7.2-alpine PACKED_LIFECYCLE_RUSTFS_IMAGE=ax-retest/rustfs:a87e8f94
export AWS_ACCESS_KEY_ID="entity$(python3 -c 'import secrets;print(secrets.token_hex(8))')"
export AWS_SECRET_ACCESS_KEY="$(python3 -c 'import secrets;print(secrets.token_hex(24))')"
export PACKED_LIFECYCLE_RUSTFS_ROOT_ACCESS_KEY="$AWS_ACCESS_KEY_ID" PACKED_LIFECYCLE_RUSTFS_ROOT_SECRET_KEY="$AWS_SECRET_ACCESS_KEY"
export AWS_DEFAULT_REGION=us-east-1 AWS_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
unset AWS_PROFILE AWS_SESSION_TOKEN
export HTTP_PROXY= HTTPS_PROXY= ALL_PROXY= http_proxy= https_proxy= all_proxy= NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost
printf 'services:\n  rustfs:\n    healthcheck:\n      disable: true\n' > "$work/health-override.yaml"
compose=(docker compose -f "$root/repo/docker/compose-xfstests/docker-compose.packed-lifecycle-backends.yml" -f "$work/health-override.yaml" -p "$project" --profile redis --profile rustfs --profile tikv)
phase=starting
cleanup() {
    result=$?;trap - EXIT
    "${compose[@]}" logs --no-color > "$root/artifacts/services.log" 2>&1 || true
    if [[ -f "$work/nginx.pid" ]];then nginx -p "$work" -c "$work/nginx.conf" -s quit || result=1;fi
    for name in nginx-error.log object-requests.log;do
        if [[ -f "$work/$name" ]];then cp "$work/$name" "$root/artifacts/$name";fi
    done
    "${compose[@]}" down -v --remove-orphans > "$root/artifacts/cleanup.log" 2>&1 || result=1
    docker ps -aq --filter "label=com.docker.compose.project=$project" > "$root/artifacts/containers-left.txt"
    docker volume ls -q --filter "label=com.docker.compose.project=$project" > "$root/artifacts/volumes-left.txt"
    [[ ! -s "$root/artifacts/containers-left.txt" && ! -s "$root/artifacts/volumes-left.txt" ]] || result=1
    [[ "$phase" == complete ]] || result=1
    printf 'phase=%s\nexit=%s\n' "$phase" "$result" > "$root/performance-result.txt"
    exit "$result"
}
trap cleanup EXIT
"${compose[@]}" up -d redis rustfs pd tikv > "$root/artifacts/service-start.log" 2>&1
python3 "$root/repo/tools/testing/wait_entity_tikv.py" "$pd_port"
for _ in {1..90};do curl -fsS "http://127.0.0.1:$object_port/health" >/dev/null && break;sleep 1;done
curl -fsS "http://127.0.0.1:$object_port/health" >/dev/null
curl -fsS "http://127.0.0.1:$pd_port/pd/api/v1/config" > "$root/artifacts/pd-config.json"
curl -fsS "http://127.0.0.1:$tikv_status_port/config" > "$root/artifacts/tikv-config.json"
"${compose[@]}" ps -q | while IFS= read -r id;do docker inspect --format '{{.Name}} {{.Image}}' "$id";done > "$root/artifacts/backend-images.txt"
cat > "$work/nginx.conf" <<EOF
worker_processes 2;
pid $work/nginx.pid;
error_log $work/nginx-error.log warn;
events { worker_connections 2048; }
http {
  log_format object_counts '\$request_method \$status \$body_bytes_sent \$http_range \$msec \$request_time';
  access_log $work/object-requests.log object_counts;
  client_max_body_size 64m;
  client_body_temp_path $work/client-temp;
  proxy_temp_path $work/proxy-temp;
  upstream objects { server 127.0.0.1:$object_port; keepalive 128; }
  server {
    listen 127.0.0.1:$proxy_port;
    location / {
      proxy_pass http://objects;
      proxy_http_version 1.1;
      proxy_set_header Host \$http_host;
      proxy_set_header Connection "";
      proxy_buffering off;
      proxy_request_buffering off;
      proxy_read_timeout 180s;
    }
  }
}
EOF
nginx -p "$work" -c "$work/nginx.conf"
endpoint="http://127.0.0.1:$proxy_port"
bucket="brewfs-entity-tikv-$(python3 -c 'import uuid;print(uuid.uuid4())')"
python3 - "$here/settings.json" "$endpoint" "$bucket" "$pd_port" "$redis_port" "$tikv_status_port" <<'PY'
import boto3,json,pathlib,sys
path,endpoint,bucket,pd,redis,status=sys.argv[1:]
boto3.client('s3',endpoint_url=endpoint,region_name='us-east-1').create_bucket(Bucket=bucket)
pathlib.Path(path).write_text(json.dumps(dict(endpoint=endpoint,bucket=bucket,pd_port=int(pd),
    redis_port=int(redis),tikv_status_port=int(status),manifest=''),indent=2))
PY
phase=focused-workspace-fanout
python3 "$here/focus.py" > "$root/focus.log" 2>&1
phase=complete
