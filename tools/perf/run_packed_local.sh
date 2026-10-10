#!/usr/bin/env bash
# Bounded real FUSE/RustFS validation. Generated artifacts are never acceptance
# evidence when the scanner, checksum, mount or cleanup fails.
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WIRE_VERSION="${PACKED_LOCAL_WIRE_VERSION:-5}"
COLD_CORPUS="${PACKED_LOCAL_COLD_CORPUS:-false}"
HARDLINK_CORPUS="${PACKED_LOCAL_HARDLINK_CORPUS:-false}"
FILES="${PACKED_LOCAL_FILES:-10000}"
FILE_BYTES="${PACKED_LOCAL_FILE_BYTES:-102400}"
METADATA_BYTES="${PACKED_LOCAL_METADATA_BYTES:-8388608}"
WORKERS="${PACKED_LOCAL_WORKERS:-16}"
MODE="${PACKED_LOCAL_MODE:-stat}"
EPOCHS="${PACKED_LOCAL_EPOCHS:-2}"
TIMEOUT_SECONDS="${PACKED_LOCAL_TIMEOUT_SECONDS:-240}"
SCANNER_SEED="${PACKED_LOCAL_SCANNER_SEED:-20261001}"
FRAME_POLICY="${PACKED_LOCAL_FRAME_POLICY:-size-only}"
INLINE_DATA="${PACKED_LOCAL_INLINE_DATA:-on}"
METADATA_CODEC="${PACKED_LOCAL_METADATA_CODEC:-zstd}"
DATA_CODEC="${PACKED_LOCAL_DATA_CODEC:-zstd}"
ACCESS_PROFILE="${PACKED_LOCAL_ACCESS_PROFILE:-random-small-file}"
P90_TRAINING_TRACE="${PACKED_LOCAL_P90_TRAINING_TRACE:-}"
BINARY="${PACKED_LOCAL_BINARY:-$ROOT/target/debug/brewfs}"
FIXTURE="${PACKED_LOCAL_FIXTURE:-$ROOT/target/debug/packed_v3_snapshot_fixture}"
COMPOSE="$ROOT/docker/compose-xfstests/docker-compose.juicefs-perf.yml"
for value in "$FILES" "$FILE_BYTES" "$METADATA_BYTES" "$WORKERS" "$EPOCHS" "$TIMEOUT_SECONDS" "$SCANNER_SEED"; do
    [[ "$value" =~ ^[0-9]+$ ]] || { printf 'Expected integer control parameter\n' >&2; exit 2; }
done
(( FILES >= 100 && FILES <= 10000 && FILES % 100 == 0 )) || { printf 'Use 100..10000 files divisible by 100\n' >&2; exit 2; }
(( FILE_BYTES > 0 && FILE_BYTES <= 67108864 && WORKERS > 0 && WORKERS <= 64 && EPOCHS > 0 && EPOCHS <= 3 && TIMEOUT_SECONDS > 0 && TIMEOUT_SECONDS <= 1800 )) || exit 2
case "$MODE" in stat|tree|full) ;; *) exit 2 ;; esac
case "$FRAME_POLICY" in size-only|static-256kib|static-1mib|static-4mib|p90-training) ;; *) exit 2 ;; esac
case "$INLINE_DATA" in on|off) ;; *) exit 2 ;; esac
case "$METADATA_CODEC" in raw|zstd) ;; *) exit 2 ;; esac
case "$DATA_CODEC" in raw|zstd) ;; *) exit 2 ;; esac
case "$ACCESS_PROFILE" in random-small-file|sequential-small-file|mixed) ;; *) exit 2 ;; esac
[[ "$WIRE_VERSION" == 5 ]] || { printf 'Use the current packed-v3 encoding 005\n' >&2; exit 2; }
case "$COLD_CORPUS" in true|false) ;; *) exit 2 ;; esac
case "$HARDLINK_CORPUS" in true|false) ;; *) exit 2 ;; esac
METADATA_PREFETCH=off
[[ -x "$BINARY" && -x "$FIXTURE" && -e /dev/fuse ]] || { printf 'Build brewfs and fixture with workspace-overlay; enable FUSE\n' >&2; exit 2; }
sudo -n true
# This Compose definition has fixed container names. Never stop someone else's run.
if docker ps -a --format '{{.Names}}' | grep -qx 'rustfs-juicefs-perf'; then
    printf 'RustFS test container already exists; refusing concurrent test\n' >&2; exit 2
fi
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
FIXTURE_PREFIX="${PACKED_LOCAL_FIXTURE_PREFIX:-local-validation-$RUN_ID}"
ARTIFACT="$ROOT/docker/compose-xfstests/artifacts/packed-local-$RUN_ID"
PROJECT="packed-local-$$"
WORK="$(mktemp -d)"
mkdir -p "$ARTIFACT" "$WORK/mnt" "$WORK/cache"
JOURNAL="$ARTIFACT/resource-journal.json"
PID=""
STARTED=false
MOUNT_READY=false
MOUNT_UNMOUNTED=false
python3 "$ROOT/tools/perf/packed_resource_journal.py" init --path "$JOURNAL" --run-id "$RUN_ID" --artifact "$ARTIFACT" --project "$PROJECT" --work "$WORK"
cleanup() {
    local status=$?
    local mount_present=false
    local process_alive=false
    local compose_present="$STARTED"
    local work_present=true
    trap - EXIT INT TERM
    # Cleanup is diagnostic and must finish every ownership check.
    set +e
    if mountpoint -q "$WORK/mnt"; then
        if timeout 20 fusermount3 -u "$WORK/mnt" >>"$ARTIFACT/cleanup.log" 2>&1; then
            if python3 "$ROOT/tools/perf/packed_resource_journal.py" event --path "$JOURNAL" --name mount_unmounted >>"$ARTIFACT/cleanup.log" 2>&1; then
                MOUNT_UNMOUNTED=true
            else
                status=1
            fi
        else
            status=1
        fi
    elif [[ "$MOUNT_READY" == true && "$MOUNT_UNMOUNTED" == false ]]; then
        if python3 "$ROOT/tools/perf/packed_resource_journal.py" event --path "$JOURNAL" --name mount_unmounted >>"$ARTIFACT/cleanup.log" 2>&1; then
            MOUNT_UNMOUNTED=true
        else
            status=1
        fi
    fi
    if [[ -n "$PID" ]]; then
        kill -TERM "$PID" 2>/dev/null || true
        for _ in $(seq 1 100); do kill -0 "$PID" 2>/dev/null || break; sleep .1; done
        if kill -0 "$PID" 2>/dev/null; then
            ps -o pid,stat,wchan:40,comm -p "$PID" >>"$ARTIFACT/cleanup.log" 2>&1 || true
            kill -KILL "$PID" 2>/dev/null || true
            status=1
        fi
        wait "$PID" 2>/dev/null || true
        if kill -0 "$PID" 2>/dev/null; then
            process_alive=true
        fi
    fi
    if "$STARTED"; then
        if docker compose -p "$PROJECT" -f "$COMPOSE" down -v --remove-orphans >>"$ARTIFACT/cleanup.log" 2>&1; then
            compose_present=false
            python3 "$ROOT/tools/perf/packed_resource_journal.py" event --path "$JOURNAL" --name compose_stopped >>"$ARTIFACT/cleanup.log" 2>&1 || status=1
        else
            status=1
        fi
    fi
    if mountpoint -q "$WORK/mnt"; then
        mount_present=true
    fi
    if [[ "$mount_present" == true || "$process_alive" == true ]]; then
        printf 'mount or daemon remains; preserving temporary directory\n' >>"$ARTIFACT/cleanup.log"
        status=1
    else
        if rm -r -- "$WORK"; then
            work_present=false
            python3 "$ROOT/tools/perf/packed_resource_journal.py" event --path "$JOURNAL" --name work_removed >>"$ARTIFACT/cleanup.log" 2>&1 || status=1
        else
            status=1
        fi
    fi
    if [[ -f "$JOURNAL" ]]; then
        JOURNAL_ARGS=(--path "$JOURNAL" --status "$status")
        [[ "$mount_present" == true ]] && JOURNAL_ARGS+=(--mount-present)
        [[ "$process_alive" == true ]] && JOURNAL_ARGS+=(--process-alive)
        [[ "$compose_present" == true ]] && JOURNAL_ARGS+=(--compose-present)
        [[ "$work_present" == true ]] && JOURNAL_ARGS+=(--work-present)
        if ! python3 "$ROOT/tools/perf/packed_resource_journal.py" finalize "${JOURNAL_ARGS[@]}" >>"$ARTIFACT/cleanup.log" 2>&1; then
            status=1
        fi
    fi
    if [[ -f "$ARTIFACT/run-manifest.json" ]]; then
        if ! python3 "$ROOT/tools/perf/packed_run_manifest.py" finalize --artifact "$ARTIFACT" --status "$status" >>"$ARTIFACT/manifest-validation.log" 2>&1; then
            status=1
        fi
    fi
    printf '%s\n' "$status" >"$ARTIFACT/exit-status.txt"
    printf 'artifact=%s status=%s\n' "$ARTIFACT" "$status"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
P90_TRACE_SHA=""
if [[ "$FRAME_POLICY" == p90-training ]]; then
    [[ -n "$P90_TRAINING_TRACE" && -f "$P90_TRAINING_TRACE" ]] || {
        printf 'p90-training requires PACKED_LOCAL_P90_TRAINING_TRACE\n' >&2
        exit 2
    }
    python3 "$ROOT/tools/perf/packed_p90_policy.py" \
        --training-trace "$P90_TRAINING_TRACE" \
        --output "$ARTIFACT/p90-policy.json"
    P90_TRACE_SHA="$(python3 - "$ARTIFACT/p90-policy.json" <<'PYP90'
import json
import pathlib
import sys

value = json.loads(pathlib.Path(sys.argv[1]).read_text())
print(value["trace_sha256"])
PYP90
)"
else
    [[ -z "$P90_TRAINING_TRACE" ]] || {
        printf 'PACKED_LOCAL_P90_TRAINING_TRACE requires p90-training\n' >&2
        exit 2
    }
fi
MANIFEST_ARGS=(init --artifact "$ARTIFACT" --run-id "$RUN_ID" --wire-version "$WIRE_VERSION" --files "$FILES" --file-bytes "$FILE_BYTES" --metadata-bytes "$METADATA_BYTES" --workers "$WORKERS" --epochs "$EPOCHS" --mode "$MODE" --scanner-seed "$SCANNER_SEED" --fixture-prefix "$FIXTURE_PREFIX" --frame-policy "$FRAME_POLICY" --inline-data "$INLINE_DATA" --metadata-codec "$METADATA_CODEC" --data-codec "$DATA_CODEC" --access-profile "$ACCESS_PROFILE")
[[ -n "$P90_TRACE_SHA" ]] && MANIFEST_ARGS+=(--p90-training-trace-sha256 "$P90_TRACE_SHA")
python3 "$ROOT/tools/perf/packed_run_manifest.py" "${MANIFEST_ARGS[@]}"
# Scope this to this local invocation. Do not change operator settings or cloud
# transport. Some SDK connectors route loopback ranges through an inherited proxy.
export HTTP_PROXY= HTTPS_PROXY= ALL_PROXY= http_proxy= https_proxy= all_proxy=
export NO_PROXY=localhost,127.0.0.1 no_proxy=localhost,127.0.0.1
export AWS_EC2_METADATA_DISABLED=true AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID="${RUSTFS_ACCESS_KEY:-rustfsadmin}"
export AWS_SECRET_ACCESS_KEY="${RUSTFS_SECRET_KEY:-rustfsadmin}"
# Fix local endpoint controls consistently for Compose and the mounted client.
export RUSTFS_HOST_BIND=127.0.0.1 RUSTFS_S3_HOST_PORT=19000 RUSTFS_CONSOLE_HOST_PORT=19001
export BREWFS_S3_BUCKET=brewfs-data
STARTED=true
docker compose -p "$PROJECT" -f "$COMPOSE" up -d rustfs >"$ARTIFACT/services.log" 2>&1
python3 "$ROOT/tools/perf/packed_resource_journal.py" event --path "$JOURNAL" --name compose_started >>"$ARTIFACT/services.log" 2>&1
docker compose -p "$PROJECT" -f "$COMPOSE" run --rm rustfs-init >>"$ARTIFACT/services.log" 2>&1
sha256sum "$BINARY" "$FIXTURE" >"$ARTIFACT/binary-sha256.txt"
git -C "$ROOT" rev-parse HEAD >"$ARTIFACT/revision.txt"
python3 - "$ROOT" "$ARTIFACT/dirty-diff-sha256.txt" <<'PYDIRTY'
import hashlib
import pathlib
import subprocess
import sys

root = pathlib.Path(sys.argv[1])
destination = pathlib.Path(sys.argv[2])
digest = hashlib.sha256()
tracked = subprocess.Popen(
    ["git", "-C", str(root), "diff", "HEAD", "--binary"],
    stdout=subprocess.PIPE,
)
assert tracked.stdout is not None
for block in iter(lambda: tracked.stdout.read(1024 * 1024), b""):
    digest.update(block)
if tracked.wait() != 0:
    raise RuntimeError("git diff failed while capturing dirty-tree provenance")
untracked = subprocess.check_output(
    ["git", "-C", str(root), "ls-files", "--others", "--exclude-standard", "-z"]
)
for raw_path in untracked.split(b"\0"):
    if not raw_path:
        continue
    relative = raw_path.decode()
    digest.update(b"\0UNTRACKED\0")
    digest.update(raw_path)
    digest.update(b"\0")
    with (root / relative).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
destination.write_text(f"{digest.hexdigest()}  dirty-tree\n")
PYDIRTY
python3 - "$ROOT" "$ARTIFACT/source-sha256.json" <<'PYSOURCES'
import pathlib,subprocess,hashlib,json,sys
root=pathlib.Path(sys.argv[1])
tracked=subprocess.check_output(['git','-C',str(root),'ls-files','-z','src','Cargo.toml','Cargo.lock','vendor/asyncfuse/src','vendor/asyncfuse/Cargo.toml'])
untracked=subprocess.check_output(['git','-C',str(root),'ls-files','--others','--exclude-standard','-z','src','vendor/asyncfuse/src','vendor/asyncfuse/Cargo.toml'])
paths=sorted(set(p.decode() for p in (tracked+untracked).split(b'\0') if p))
hashes={p:hashlib.sha256((root/p).read_bytes()).hexdigest() for p in paths if (root/p).is_file()}
pathlib.Path(sys.argv[2]).write_text(json.dumps(hashes,sort_keys=True,indent=2)+'\n')
PYSOURCES
python3 - "$ROOT" "$BINARY" "$ARTIFACT/dirty-diff-sha256.txt" "$ARTIFACT/toolchain.json" <<'PYTOOLCHAIN'
import json
import pathlib
import subprocess
import sys

root = pathlib.Path(sys.argv[1])
binary = pathlib.Path(sys.argv[2])
dirty_digest_path = pathlib.Path(sys.argv[3])
destination = pathlib.Path(sys.argv[4])

def output(*command):
    return subprocess.check_output(command, text=True).strip()

rustc_verbose = output("rustc", "-Vv")
host = next(
    (line.split(":", 1)[1].strip() for line in rustc_verbose.splitlines() if line.startswith("host:")),
    "",
)
revision = output("git", "-C", str(root), "rev-parse", "HEAD")
dirty_diff_sha256 = dirty_digest_path.read_text().split()[0]
profile = "release" if "/release/" in binary.as_posix() else "debug"
destination.write_text(
    json.dumps(
        {
            "rustc_verbose": rustc_verbose,
            "cargo_version": output("cargo", "-V"),
            "host": host,
            "binary_profile": profile,
            "revision": revision,
            "dirty_diff_sha256": dirty_diff_sha256,
        },
        sort_keys=True,
        indent=2,
    )
    + "\n"
)
PYTOOLCHAIN
FIXTURE_ARGS=(--wire-version 5 --frame-policy "$FRAME_POLICY" --inline-data "$INLINE_DATA" --metadata-codec "$METADATA_CODEC" --data-codec "$DATA_CODEC" --access-profile "$ACCESS_PROFILE")
[[ "$FRAME_POLICY" == p90-training ]] && FIXTURE_ARGS+=(--p90-policy "$ARTIFACT/p90-policy.json")
[[ "$COLD_CORPUS" == true ]] && FIXTURE_ARGS+=(--cold-corpus true)
[[ "$HARDLINK_CORPUS" == true ]] && FIXTURE_ARGS+=(--hardlink-corpus true)
"$FIXTURE" "${FIXTURE_ARGS[@]}" --bucket brewfs-data --endpoint http://127.0.0.1:19000 --region us-east-1 --force-path-style true --prefix "$FIXTURE_PREFIX" --manifest-output "$ARTIFACT/manifest-key.txt" --dir-levels 2 --dirs-per-level 10 --files-per-dir "$((FILES / 100))" --small-file-size "$FILE_BYTES" --small-file-min-size "$FILE_BYTES" --small-file-max-size "$FILE_BYTES" --access-profile "$ACCESS_PROFILE" >"$ARTIFACT/fixture.log" 2>&1
python3 - "$WORK" "$ARTIFACT/manifest-key.txt" <<'PYCONFIG'
import pathlib,sys
work=pathlib.Path(sys.argv[1]); key=pathlib.Path(sys.argv[2]).read_text().strip()
(work/'mount.yaml').write_text(f"""mount_point: {work}/mnt
volume_format: packed-metadata-v3
packed_manifest_key: {key}
data:
  backend: s3
  s3:
    bucket: brewfs-data
    endpoint: http://127.0.0.1:19000
    region: us-east-1
    force_path_style: true
    disable_payload_checksum: true
layout: {{ chunk_size: 67108864, block_size: 4194304 }}
fuse: {{ workers: 16, max_background: 512, privileged: false }}
cache:
  root: {work}/cache
  read_memory_bytes: 0
  read_ssd_bytes: 0
  prefetch_enabled: false
  range_background_prefetch: false
  compression: none
""")
PYCONFIG
printf 'files=%s
file_bytes=%s
metadata_bytes=%s
workers=%s
epochs=%s
mode=%s
order=shuffle
payload_memory=0
payload_ssd=0
window=0
decoded=0
metadata_prefetch=%s
wire_version=%s
frame_policy=%s
inline_data=%s
metadata_codec=%s
data_codec=%s
access_profile=%s
fuse_ttl_ms=0
direct_io=1
keep_cache=0
loopback_proxy=disabled\ncold_corpus=%s\nhardlink_corpus=%s\n' "$FILES" "$FILE_BYTES" "$METADATA_BYTES" "$WORKERS" "$EPOCHS" "$MODE" "$METADATA_PREFETCH" "$WIRE_VERSION" "$FRAME_POLICY" "$INLINE_DATA" "$METADATA_CODEC" "$DATA_CODEC" "$ACCESS_PROFILE" "$COLD_CORPUS" "$HARDLINK_CORPUS" >"$ARTIFACT/profile.env"
printf 'packed_version=v3\nscanner_seed=%s\nfixture_prefix=%s\nmanifest_schema=packed-v3-run-manifest-v1\n' "$SCANNER_SEED" "$FIXTURE_PREFIX" >>"$ARTIFACT/profile.env"
[[ -n "$P90_TRACE_SHA" ]] && printf 'p90_training_trace_sha256=%s\n' "$P90_TRACE_SHA" >>"$ARTIFACT/profile.env"
sync
sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches'
printf 'page_cache=dropped\n' >"$ARTIFACT/cache-proof.env"
START="$(date +%s%N)"
BREWFS_PACKED_METADATA_CACHE_BYTES="$METADATA_BYTES" BREWFS_PACKED_METADATA_PREFETCH="$METADATA_PREFETCH" BREWFS_PACKED_DECODED_FRAME_CACHE_BYTES=0 BREWFS_PACKED_FRAME_WINDOW_CACHE_BYTES=0 BREWFS_PACKED_FRAME_WINDOW_PREFETCH=false BREWFS_CACHE_TTL_MS=0 BREWFS_FUSE_READ_DIRECT_IO=1 BREWFS_FUSE_KEEP_CACHE=0 RUST_LOG=warn,brewfs=info "$BINARY" mount --config "$WORK/mount.yaml" "$WORK/mnt" >"$ARTIFACT/brewfs.log" 2>&1 &
PID=$!
for _ in $(seq 1 600); do
    mountpoint -q "$WORK/mnt" && break
    kill -0 "$PID" || { printf 'mount failed\n' >&2; exit 1; }
    sleep .1
done
mountpoint -q "$WORK/mnt"
READY="$(date +%s%N)"
if python3 "$ROOT/tools/perf/packed_resource_journal.py" event --path "$JOURNAL" --name mount_ready >>"$ARTIFACT/brewfs.log" 2>&1; then
    MOUNT_READY=true
else
    exit 1
fi
if [[ "$COLD_CORPUS" == true ]]; then
    python3 - "$WORK/mnt" "$ARTIFACT/cold-verification.json" <<'PYCOLD'
import os,pathlib,json,sys,errno
root=pathlib.Path(sys.argv[1])
target=os.readlink(os.fsencode(root/'.cold-link'))
assert target==b'raw-\xff-target'
value=os.getxattr(root/'.cold-file','user.brewfs.test')
import hashlib
pathlib.Path(sys.argv[2]).write_text(json.dumps(dict(readlink_bytes=target==b'raw-\xff-target',xattr_length=len(value),xattr_sha256=hashlib.sha256(value).hexdigest()))+'\n')
assert value==b'\x00\xffcold'
assert 'user.brewfs.test' in os.listxattr(root/'.cold-file')
assert (root/'.cold-file').read_bytes()==b'cold\n'
try:
    os.setxattr(root/'.cold-file','user.brewfs.test',b'forbidden')
except OSError as error:
    assert error.errno == errno.EROFS, error.errno
    assert os.getxattr(root/'.cold-file','user.brewfs.test')==b'\x00\xffcold'
else:
    raise AssertionError('readonly xattr mutation succeeded')
pathlib.Path(sys.argv[2]).write_text(json.dumps(dict(readlink_bytes=True,xattr=True,listxattr=True,readonly_mutation_rejected=True))+'\n')
PYCOLD
fi
if [[ "$HARDLINK_CORPUS" == true ]]; then
    python3 - "$WORK/mnt" "$ARTIFACT/hardlink-verification.json" <<'PYHARDLINK'
import pathlib,os,json,sys
root=pathlib.Path(sys.argv[1]);a=root/'.hardlink-a';b=root/'d000'/'.hardlink-b'
aa=a.stat();bb=b.stat()
assert aa.st_ino==bb.st_ino and aa.st_nlink==bb.st_nlink==2
assert a.read_bytes()==b.read_bytes()==b'hardlink\n'
pathlib.Path(sys.argv[2]).write_text(json.dumps(dict(same_inode=True,nlink=2,content=True))+'\n')
PYHARDLINK
fi
python3 - "$PID" "$ARTIFACT/daemon-memory-before.json" <<'PYMEM'
import pathlib,json,sys
p=pathlib.Path('/proc')/sys.argv[1]
fields={}
for line in (p/'smaps_rollup').read_text().splitlines():
    if line.startswith(('Rss:','Pss:')): fields[line.split(':')[0]+'_kib']=int(line.split()[1])
pathlib.Path(sys.argv[2]).write_text(json.dumps(fields)+'\n')
PYMEM
timeout --signal=TERM --kill-after=10 "$TIMEOUT_SECONDS" python3 "$ROOT/tools/perf/smallfiles_scan.py" --root "$WORK/mnt" --label packed-local --mode "$MODE" --expected-files "$FILES" --min-size "$FILE_BYTES" --max-size "$FILE_BYTES" --dir-levels 2 --dirs-per-level 10 --files-per-leaf "$((FILES / 100))" --workers "$WORKERS" --order shuffle --shuffle-seed "$SCANNER_SEED" --epochs "$EPOCHS" --batch-size 256 --max-inflight-batches 2 --json-output "$ARTIFACT/summary.json" >"$ARTIFACT/scanner.log" 2>&1
SCAN_END="$(date +%s%N)"
python3 - "$PID" "$ARTIFACT/daemon-memory-after.json" <<'PYMEM'
import pathlib,json,sys
p=pathlib.Path('/proc')/sys.argv[1]
fields={}
for line in (p/'smaps_rollup').read_text().splitlines():
    if line.startswith(('Rss:','Pss:')): fields[line.split(':')[0]+'_kib']=int(line.split()[1])
for line in (p/'status').read_text().splitlines():
    if line.startswith('VmHWM:'): fields['peak_rss_kib']=int(line.split()[1])
pathlib.Path(sys.argv[2]).write_text(json.dumps(fields)+'\n')
PYMEM
tr -d '\000' <"$WORK/mnt/.stats" >"$ARTIFACT/fuse-stats.txt"
kill -TERM "$PID"
for _ in $(seq 1 200); do kill -0 "$PID" 2>/dev/null || break; sleep .1; done
if kill -0 "$PID" 2>/dev/null; then printf 'teardown timeout\n' >&2; exit 1; fi
wait "$PID"
PID=""
DRAIN_END="$(date +%s%N)"
python3 - "$ARTIFACT" "$START" "$READY" "$SCAN_END" "$DRAIN_END" <<'PYTIMING'
import json,pathlib,sys
p=pathlib.Path(sys.argv[1]); a,b,c,d=map(int,sys.argv[2:])
s=json.loads((p/'summary.json').read_text())
epochs=s['epochs'] if isinstance(s.get('epochs'),list) else [s]
assert all(row['files']==row['expected_files'] and row['errors']==0 for row in epochs)
active=(c-b)/1e9; drain=(d-c)/1e9
out=dict(mount_seconds=(b-a)/1e9, active_seconds=active, drain_seconds=drain, total_seconds=(d-a)/1e9)
logical=sum(row.get('payload_bytes',0) for row in epochs)
out['active_bw_mib_s']=logical/1048576/active
out['effective_active_plus_drain_bw_mib_s']=logical/1048576/(active+drain)
out['effective_wall_bw_mib_s']=logical/1048576/((d-a)/1e9)
(p/'timing.json').write_text(json.dumps(out,indent=2)+'\n')
PYTIMING
