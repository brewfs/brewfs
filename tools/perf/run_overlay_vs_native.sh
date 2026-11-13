#!/usr/bin/env bash
# Quick overlay-vs-native read comparison on one host.
#
#   native  : flat BrewFS volume, Redis metadata + RustFS objects. The corpus
#             is written through the mount, drained, remounted cold and read.
#   overlay : workspace workspace (Redis catalog) whose read-only lower is a
#             packed snapshot of the same corpus on the same RustFS.
#
# Both arms use the same release binary, file tree/sizes/bytes, scanner seed,
# FUSE TTL0/direct-io, zero payload caches, worker count and cleanup. Arms
# alternate per repetition. The corpus write and the packed import are setup
# and are timed separately from the measured mount/scan.
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT="${OVN_ARTIFACT_ROOT:?set OVN_ARTIFACT_ROOT}"
BINARY="${OVN_BINARY:?set OVN_BINARY (release brewfs with workspace)}"
FIXTURE="${OVN_FIXTURE:?set OVN_FIXTURE (packed_snapshot_fixture)}"
FILES="${OVN_FILES:-2000}"
FILE_BYTES="${OVN_FILE_BYTES:-102400}"
WORKERS="${OVN_WORKERS:-16}"
EPOCHS="${OVN_EPOCHS:-2}"
REPEATS="${OVN_REPEATS:-3}"
MODES="${OVN_MODES:-full stat rw}"
# rw: small-file write -> readback -> mixed in one mount per arm/rep.
RW_FILES="${OVN_RW_FILES:-2000}"
RW_BYTES="${OVN_RW_FILE_BYTES:-16384}"
RW_MIXED_OPS="${OVN_RW_MIXED_OPS:-2000}"
RW_WRITE_RATIO="${OVN_RW_WRITE_RATIO:-0.3}"
SEED="${OVN_SCANNER_SEED:-20261001}"
(( FILES >= 100 && FILES % 100 == 0 )) || { echo "OVN_FILES must be a multiple of 100" >&2; exit 2; }
[[ -x "$BINARY" && -x "$FIXTURE" && -e /dev/fuse ]] || { echo "binary/fixture/FUSE required" >&2; exit 2; }

ts="$(date -u +%Y%m%dT%H%M%SZ)-$$"
group="$OUT/ovn-$ts"
[[ ! -e "$group" ]] || exit 2
mkdir -p "$group/runs"
project="brewfs-ovn-$(printf '%s' "$ts" | tr 'A-Z' 'a-z' | tr -cd 'a-z0-9')"
base=$(( 40000 + ($$ % 200) * 10 ))
redis_port=$((base + 1)); rustfs_port=$((base + 2))
access="ovn$(python3 -c 'import secrets;print(secrets.token_hex(6))')"
secret="$(python3 -c 'import secrets;print(secrets.token_hex(20))')"
env_file="$group/.compose.env"
( umask 077
  printf 'PACKED_LIFECYCLE_REDIS_PORT=%s\nPACKED_LIFECYCLE_RUSTFS_PORT=%s\nPACKED_LIFECYCLE_RUSTFS_ROOT_ACCESS_KEY=%s\nPACKED_LIFECYCLE_RUSTFS_ROOT_SECRET_KEY=%s\nPACKED_LIFECYCLE_PD_PORT=1\nPACKED_LIFECYCLE_PD_PEER_PORT=1\nPACKED_LIFECYCLE_TIKV_PORT=1\nPACKED_LIFECYCLE_TIKV_STATUS_PORT=1\n' \
    "$redis_port" "$rustfs_port" "$access" "$secret" >"$env_file" )
compose=(sudo -n docker compose --env-file "$env_file" -f "$ROOT/docker/compose-xfstests/docker-compose.packed-lifecycle-backends.yml" -p "$project" --profile redis --profile rustfs)
endpoint="http://127.0.0.1:$rustfs_port"
redis_url="redis://127.0.0.1:$redis_port"
export AWS_ACCESS_KEY_ID="$access" AWS_SECRET_ACCESS_KEY="$secret" AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
export HTTP_PROXY= HTTPS_PROXY= ALL_PROXY= http_proxy= https_proxy= all_proxy= NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost
work="$(mktemp -d /tmp/brewfs-ovn.XXXXXX)"
pid=""

stop_mount() {
    local mnt="$1"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null || true
        for _ in $(seq 1 300); do kill -0 "$pid" 2>/dev/null || break; sleep .1; done
        kill -0 "$pid" 2>/dev/null && { kill -KILL "$pid" 2>/dev/null || true; return 1; }
        wait "$pid" 2>/dev/null || true
    fi
    pid=""
    if mountpoint -q "$mnt"; then fusermount3 -u "$mnt" || return 1; fi
}

cleanup() {
    local status=$?
    trap - EXIT INT TERM
    set +e
    for mnt in "$work"/*/mnt; do [[ -d "$mnt" ]] && stop_mount "$mnt"; done
    local mounts_left
    mounts_left="$(grep -c " $work/" /proc/mounts || true)"
    "${compose[@]}" logs --no-color >"$group/backend-services.log" 2>&1
    "${compose[@]}" down -v --remove-orphans >"$group/compose-down.log" 2>&1
    local down=$?
    local left
    left=$(( $(sudo -n docker ps -aq --filter "label=com.docker.compose.project=$project" | wc -l) \
           + $(sudo -n docker volume ls -q --filter "label=com.docker.compose.project=$project" | wc -l) ))
    rm -f "$env_file"
    [[ "$mounts_left" == 0 ]] && rm -rf "$work"
    printf 'compose_down_exit=%s\ncompose_resources_left=%s\nfuse_mounts_left=%s\nwork_removed=%s\n' \
        "$down" "$left" "$mounts_left" "$([[ -e $work ]] && echo false || echo true)" >"$group/cleanup-proof.txt"
    (( down == 0 && left == 0 )) && [[ "$mounts_left" == 0 ]] || status=1
    python3 - "$group" "$status" <<'PY'
import json, pathlib, statistics, sys
group = pathlib.Path(sys.argv[1]); status = int(sys.argv[2])
rows = [json.loads(p.read_text()) for p in sorted((group / "runs").glob("*/row.json"))]
ok = lambda r: r.get("exit") == 0 and r.get("files_ok")
summary = {"schema": "brewfs-overlay-vs-native-v1", "rows": rows, "groups": {}}
for key in sorted({(r["mode"], r["arm"]) for r in rows}):
    rs = [r for r in rows if (r["mode"], r["arm"]) == key and ok(r)]
    med = lambda k: statistics.median(r[k] for r in rs) if rs else None
    summary["groups"][f"{key[0]}/{key[1]}"] = {"valid": len(rs), "runs": sum((r["mode"], r["arm"]) == key for r in rows),
        **{k: med(k) for k in ("mount_s", "active_s", "files_per_sec", "p50_ms", "p95_ms", "p99_ms")}}
# Identity is per mode: stat mode reads no payload, so its checksum is 0.
modes = sorted({r["mode"] for r in rows})
ident = {m: {"traces": len({tuple(r.get("trace_sha256", [])) for r in rows if ok(r) and r["mode"] == m}),
             "checksums": len({tuple(r.get("checksum", [])) for r in rows if ok(r) and r["mode"] == m})} for m in modes}
summary["identity"] = ident
summary["status"] = "passed" if status == 0 and rows and all(ok(r) for r in rows) and all(v == {"traces": 1, "checksums": 1} for v in ident.values()) else "rejected"
(group / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(f"overlay-vs-native status={summary['status']} group={group}")
for name, g in summary["groups"].items():
    print(f"  {name:16} valid={g['valid']}/{g['runs']} mount={g['mount_s']} active={g['active_s']} files/s={g['files_per_sec']} p50={g['p50_ms']} p95={g['p95_ms']} p99={g['p99_ms']}")
PY
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"${compose[@]}" up -d --wait redis rustfs >"$group/backend-start.log" 2>&1
python3 - "$endpoint" "$access" "$secret" <<'PY'
import sys, boto3, botocore.config
c = boto3.client("s3", endpoint_url=sys.argv[1], aws_access_key_id=sys.argv[2], aws_secret_access_key=sys.argv[3],
                 region_name="us-east-1", config=botocore.config.Config(s3={"addressing_style": "path"}))
for b in ("ovn-native", "ovn-packed"):
    c.create_bucket(Bucket=b)
PY
sha256sum "$BINARY" "$FIXTURE" >"$group/binary.sha256"
git -C "$ROOT" rev-parse HEAD >"$group/revision.txt"
printf 'files=%s\nfile_bytes=%s\nworkers=%s\nepochs=%s\nrepeats=%s\nmodes=%s\nscanner_seed=%s\nfuse_ttl_ms=0\ndirect_io=1\nkeep_cache=0\nread_cache=0\nrw_files=%s\nrw_file_bytes=%s\nrw_mixed_ops=%s\nrw_write_ratio=%s\nrw_fsync=1\n' \
    "$FILES" "$FILE_BYTES" "$WORKERS" "$EPOCHS" "$REPEATS" "$MODES" "$SEED" "$RW_FILES" "$RW_BYTES" "$RW_MIXED_OPS" "$RW_WRITE_RATIO" >"$group/profile.env"
sudo -n docker image inspect --format '{{json .RepoDigests}}' redis:7.2-alpine w23geq7ncegg6etqsl.xuanyuan.run/rustfs/rustfs:latest >"$group/image-digests.txt" 2>&1 || true

data_yaml() {  # bucket
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
common_yaml() {  # cache_root
    cat <<EOF
layout: { chunk_size: 67108864, block_size: 4194304 }
fuse: { workers: 16, max_background: 512, privileged: false }
cache:
  root: $1
  read_memory_bytes: 0
  read_ssd_bytes: 0
  prefetch_enabled: false
  range_background_prefetch: false
  compression: none
EOF
}

mount_and_wait() {  # config mnt log -> sets pid; prints mount seconds
    local config="$1" mnt="$2" log="$3" start
    start="$(date +%s%N)"
    BREWFS_CACHE_TTL_MS=0 BREWFS_FUSE_READ_DIRECT_IO=1 BREWFS_FUSE_KEEP_CACHE=0 RUST_LOG="${OVN_RUST_LOG:-warn,brewfs=info}" \
        "$BINARY" mount --config "$config" "$mnt" >"$log" 2>&1 &
    pid=$!
    for _ in $(seq 1 3000); do
        mountpoint -q "$mnt" && break
        kill -0 "$pid" 2>/dev/null || return 1
        sleep .1
    done
    mountpoint -q "$mnt" || return 1
    python3 -c "print(($(date +%s%N) - $start) / 1e9)"
}

# ---- setup: native corpus through a flat Redis+RustFS mount -------------
mkdir -p "$work/native-setup/mnt" "$work/native-setup/cache"
{ echo "mount_point: $work/native-setup/mnt"; data_yaml ovn-native; echo "meta:"; echo "  backend: redis"; echo "  redis:"; echo "    url: \"$redis_url/1\""; common_yaml "$work/native-setup/cache"; } >"$work/native.yaml"
sed "s#$work/native-setup/mnt#\$MNT#; s#$work/native-setup/cache#\$CACHE#" "$work/native.yaml" >"$group/native.yaml.template"
mount_and_wait "$work/native.yaml" "$work/native-setup/mnt" "$group/native-setup-mount.log" >/dev/null
setup_start="$(date +%s%N)"
# A fresh flat volume root is uid 0 / 0755; setup writes as root (allow_other
# mount). Only the unmeasured corpus write runs privileged.
sudo -n python3 "$ROOT/tools/perf/write_smallfiles_corpus.py" --root "$work/native-setup/mnt" --files-per-dir "$((FILES / 100))" \
    --min-size "$FILE_BYTES" --max-size "$FILE_BYTES" --workers "$WORKERS" >"$group/native-corpus-write.log" 2>&1
sync -f "$work/native-setup/mnt"
stop_mount "$work/native-setup/mnt"
python3 -c "print('{\"native_corpus_write_and_drain_s\": %.3f}' % (($(date +%s%N) - $setup_start) / 1e9))" >"$group/native-setup.json"

# ---- setup: packed corpus + workspace with packed lower -----------------
setup_start="$(date +%s%N)"
"$FIXTURE" --wire-version 5 --frame-policy size-only --inline-data off --metadata-codec zstd --data-codec zstd \
    --access-profile random-small-file --bucket ovn-packed --endpoint "$endpoint" --region us-east-1 \
    --force-path-style true --prefix ovn --manifest-output "$work/manifest-key.txt" --dir-levels 2 \
    --dirs-per-level 10 --files-per-dir "$((FILES / 100))" --small-file-size "$FILE_BYTES" \
    --small-file-min-size "$FILE_BYTES" --small-file-max-size "$FILE_BYTES" >"$group/packed-fixture.log" 2>&1
manifest="$(cat "$work/manifest-key.txt")"
data_yaml ovn-packed >"$work/packed-data.yaml"
ns="ovn-$ts"
"$BINARY" workspace --meta-backend redis --meta-url "$redis_url/2" --workspace-namespace "$ns" \
    init-packed-volume --manifest-key "$manifest" --data-config "$work/packed-data.yaml" >"$group/workspace-init.json" 2>"$group/workspace-init.log"
# The CLI logs to stdout before printing the record; parse from its first "{".
workspace="$(python3 -c '
import json, sys
lines = open(sys.argv[1], encoding="utf-8", errors="replace").read().splitlines()
start = next(i for i, line in enumerate(lines) if line == "{")
print(json.loads("\n".join(lines[start:]))["workspace_id"])
' "$group/workspace-init.json")"
python3 -c "print('{\"packed_fixture_and_workspace_s\": %.3f}' % (($(date +%s%N) - $setup_start) / 1e9))" >"$group/overlay-setup.json"
printf 'manifest=%s\nworkspace=%s\nnamespace=%s\n' "$manifest" "$workspace" "$ns" >>"$group/profile.env"

run_arm() {  # arm mode rep
    local arm="$1" mode="$2" rep="$3" dir="$work/$arm-$mode-r$rep" art="$group/runs/$mode-r$rep-$arm"
    mkdir -p "$dir/mnt" "$dir/cache" "$art"
    if [[ "$arm" == native ]]; then
        { echo "mount_point: $dir/mnt"; data_yaml ovn-native; echo "meta:"; echo "  backend: redis"; echo "  redis:"; echo "    url: \"$redis_url/1\""; common_yaml "$dir/cache"; } >"$dir/mount.yaml"
    else
        { echo "mount_point: $dir/mnt"; echo "volume_format: workspace"; echo "workspace: $workspace"; echo "workspace_namespace: $ns";
          data_yaml ovn-packed; echo "meta:"; echo "  backend: redis"; echo "  redis:"; echo "    url: \"$redis_url/2\""; common_yaml "$dir/cache"; } >"$dir/mount.yaml"
    fi
    sync; sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    # Metadata round trips per arm: Redis command counters around the run.
    "${compose[@]}" exec -T redis redis-cli -p "$redis_port" INFO commandstats >"$art/redis-cmdstats-before.txt" 2>&1 || true
    local rc=0 mount_s monitor=""
    # Optional diagnosis only: MONITOR perturbs latency, never use for numbers.
    if [[ "${OVN_REDIS_MONITOR:-0}" == 1 ]]; then
        "${compose[@]}" exec -T redis redis-cli -p "$redis_port" MONITOR >"$art/redis-monitor.txt" 2>&1 &
        monitor=$!
    fi
    mount_s="$(mount_and_wait "$dir/mount.yaml" "$dir/mnt" "$art/brewfs.log")" || rc=1
    local profiler=""
    # Optional diagnosis only: perf sampling perturbs timing, never use for numbers.
    if (( rc == 0 )) && [[ -n "${OVN_PERF_ARM:-}" && "$OVN_PERF_ARM" == "$arm" ]]; then
        local bpid; bpid="$(pgrep -f -- "mount --config $dir/mount.yaml" | head -1)"
        sudo -n perf record -F 499 --call-graph "${OVN_PERF_CALLGRAPH:-fp}" -p "$bpid" -o "$art/perf.data" >"$art/perf-record.log" 2>&1 &
        profiler=$!
    fi
    if (( rc == 0 )) && [[ "$mode" == rw ]]; then
        # Each step is its own row with its own commandstats; the page cache is
        # dropped between steps so readback/mixed read through FUSE.
        local step sub="rw-r$rep"
        # Unmeasured setup: roots are uid 0/0755 (like the corpus write), so
        # create the workload directory privileged and hand it to this user.
        sudo -n mkdir "$dir/mnt/$sub" && sudo -n chown "$(id -u):$(id -g)" "$dir/mnt/$sub" || rc=1
        for step in write readback mixed; do
            local sart="$group/runs/$step-r$rep-$arm"; mkdir -p "$sart"
            sync; sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches'
            "${compose[@]}" exec -T redis redis-cli -p "$redis_port" INFO commandstats >"$sart/redis-cmdstats-before.txt" 2>&1 || true
            local srcs=0
            timeout 1800 python3 "$ROOT/tools/perf/smallfiles_rw.py" --root "$dir/mnt" --subdir "$sub" --mode "$step" \
                --files "$RW_FILES" --file-bytes "$RW_BYTES" --workers "$WORKERS" --mixed-ops "$RW_MIXED_OPS" \
                --write-ratio "$RW_WRITE_RATIO" --seed "$SEED" --label "$arm" --json-output "$sart/summary.json" \
                >"$sart/scanner.log" 2>&1 || srcs=$?
            "${compose[@]}" exec -T redis redis-cli -p "$redis_port" INFO commandstats >"$sart/redis-cmdstats-after.txt" 2>&1 || true
            printf '%s\n' "$srcs" >"$sart/step-exit.txt"
            (( srcs == 0 )) || rc=$srcs
        done
        tr -d '\000' <"$dir/mnt/.stats" >"$art/fuse-stats.txt" 2>/dev/null || true
    elif (( rc == 0 )); then
        timeout 1800 python3 "$ROOT/tools/perf/smallfiles_scan.py" --root "$dir/mnt" --label "$arm" --mode "$mode" \
            --expected-files "$FILES" --min-size "$FILE_BYTES" --max-size "$FILE_BYTES" --dir-levels 2 --dirs-per-level 10 \
            --files-per-leaf "$((FILES / 100))" --workers "$WORKERS" --order shuffle --shuffle-seed "$SEED" --epochs "$EPOCHS" \
            --batch-size 256 --max-inflight-batches 2 --json-output "$art/summary.json" >"$art/scanner.log" 2>&1 || rc=$?
        tr -d '\000' <"$dir/mnt/.stats" >"$art/fuse-stats.txt" 2>/dev/null || true
    fi
    if [[ -n "$profiler" ]]; then sudo -n kill -INT "$profiler" 2>/dev/null || true; wait "$profiler" 2>/dev/null || true; fi
    local rc_mount=0
    [[ -n "${mount_s:-}" ]] || rc_mount=1
    stop_mount "$dir/mnt" || { rc=1; rc_mount=1; }
    if [[ -n "$monitor" ]]; then kill "$monitor" 2>/dev/null || true; wait "$monitor" 2>/dev/null || true; fi
    "${compose[@]}" exec -T redis redis-cli -p "$redis_port" INFO commandstats >"$art/redis-cmdstats-after.txt" 2>&1 || true
    local row_dirs=("$art:$mode")
    if [[ "$mode" == rw ]]; then
        row_dirs=()
        for step in write readback mixed; do row_dirs+=("$group/runs/$step-r$rep-$arm:$step"); done
        rmdir "$art" 2>/dev/null || true
    fi
    local entry
    for entry in "${row_dirs[@]}"; do
    # rw: a step's own exit, or the arm's mount/unmount failure (rc_mount).
    local step_rc="$rc"
    if [[ "$mode" == rw ]]; then
        step_rc="$rc_mount"
        [[ -f "${entry%%:*}/step-exit.txt" ]] && (( step_rc == 0 )) && step_rc="$(cat "${entry%%:*}/step-exit.txt")"
    fi
    python3 - "${entry%%:*}" "$arm" "${entry##*:}" "$rep" "$step_rc" "${mount_s:-0}" <<'PY'
import json, pathlib, sys
art = pathlib.Path(sys.argv[1]); arm, mode, rep, rc, mount_s = sys.argv[2], sys.argv[3], int(sys.argv[4]), int(sys.argv[5]), float(sys.argv[6])
art.mkdir(parents=True, exist_ok=True)
row = {"arm": arm, "mode": mode, "rep": rep, "exit": rc, "mount_s": mount_s}
p = art / "summary.json"
if p.exists():
    s = json.loads(p.read_text()); e = s["epochs"] if isinstance(s.get("epochs"), list) else [s]
    row.update(files_ok=all(x["files"] == x["expected_files"] and x["errors"] == 0 for x in e),
               trace_sha256=[x.get("trace_sha256") for x in e], checksum=[x.get("checksum") for x in e],
               active_s=sum(x["seconds"] for x in e), files_per_sec=e[-1]["files_per_sec"],
               p50_ms=e[-1].get("latency_p50_ms"), p95_ms=e[-1].get("latency_p95_ms"), p99_ms=e[-1].get("latency_p99_ms"))
(art / "row.json").write_text(json.dumps(row) + "\n")
print(f"{mode} r{rep} {arm}: exit={rc} files_ok={row.get('files_ok')} files/s={row.get('files_per_sec')} p99={row.get('p99_ms')}")
PY
    done
    return 0
}

for mode in $MODES; do
    for rep in $(seq 1 "$REPEATS"); do
        if (( rep % 2 == 1 )); then order=(native overlay); else order=(overlay native); fi
        for arm in "${order[@]}"; do run_arm "$arm" "$mode" "$rep"; done
    done
done
