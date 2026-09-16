#!/usr/bin/env bash
set -u
suffix="$$"
network="brewfs-pr04-tikv-$suffix"
pd="brewfs-pr04-pd-$suffix"
tikv="brewfs-pr04-tikv-$suffix"
cleanup() {
  docker rm -f "$tikv" "$pd" >/dev/null 2>&1 || true
  docker network rm "$network" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker network create "$network" >/dev/null || exit $?
docker run -d --name "$pd" --network "$network" \
  --network-alias pd \
  pingcap/pd:v8.5.0 \
  --name=pd \
  --data-dir=/data/pd \
  --client-urls=http://0.0.0.0:2379 \
  --advertise-client-urls=http://pd:2379 \
  --peer-urls=http://0.0.0.0:2380 \
  --advertise-peer-urls=http://pd:2380 \
  --initial-cluster=pd=http://pd:2380 \
  --log-level=warn >/dev/null || exit $?

docker run -d --name "$tikv" --network "$network" \
  --network-alias tikv \
  pingcap/tikv:v8.5.0 \
  --addr=0.0.0.0:20160 \
  --advertise-addr=tikv:20160 \
  --status-addr=0.0.0.0:20180 \
  --pd=pd:2379 \
  --data-dir=/data/tikv \
  --log-level=warn >/dev/null || exit $?

if ! docker run --rm --network "$network" curlimages/curl:8.10.1 \
  sh -ec 'for _ in $(seq 1 120); do body=$(curl -fsS http://pd:2379/pd/api/v1/stores 2>/dev/null || true); echo "$body" | grep -Eq '\''"state_name"[[:space:]]*:[[:space:]]*"Up"'\'' && exit 0; sleep 1; done; exit 1'; then
  docker logs "$pd" >&2 || true
  docker logs "$tikv" >&2 || true
  echo "scratch TiKV cluster did not become ready" >&2
  exit 1
fi

docker run --rm --network "$network" \
  -e BREWFS_TIKV_PD_ENDPOINTS=pd:2379 \
  -v /mnt/d/code/brewfs/.claude/worktrees/vigorous-solomon-7caff5/.claude/pr04-test-bin:/test-bin:ro \
  ubuntu:24.04 \
  /test-bin native_base::write::tests_tikv --ignored
