#!/usr/bin/env bash
#
# End-to-end S3 + Redis smoke test: mount BrewFS on an object store and clone
# real git repositories onto it.
#
# This is the only harness path that exercises the S3 data backend with a
# Redis metadata store under a real git workload. The workspace-overlay smoke
# in CI uses the local-fs backend, so object-store write, cache-eviction
# recovery, and commit-back paths are not covered there.
#
# What it proves, in order:
#   1. BrewFS mounts with data.backend=s3 and meta.backend=redis.
#   2. A buffered write round-trips (write -> stat size -> read back).
#   3. `git clone` succeeds and the cloned worktree is self-consistent
#      (git log, git status, git fsck).
#   4. After unmount, wiping the local block cache, and remounting, every file
#      still has the same checksum and git fsck is still clean. Step 4 is what
#      makes this an object-store test: the second mount cannot be served from
#      the local cache.
#   5. The bucket actually received objects (S3 list check from a sibling
#      container).
#
# Usage:
#   bash docker/compose-xfstests/run_s3_git_clone_smoke.sh
#   BREWFS_SMOKE_GIT_REPO=https://github.com/github/gitignore.git \
#     bash docker/compose-xfstests/run_s3_git_clone_smoke.sh
#
# Environment:
#   BREWFS_SMOKE_GIT_REPO        repo to clone (default octocat/Hello-World)
#   BREWFS_SMOKE_ARTIFACT_DIR    artifact directory (default
#                                docker/compose-xfstests/artifacts/s3-git-clone-smoke-<ts>)
#   BREWFS_SMOKE_KEEP=1          keep services and volumes after the run
#   BREWFS_SMOKE_BUILD=1         force an image rebuild before running
#   BREWFS_SMOKE_NETWORK         override the compose network name
#
# The harness always tears services and volumes down on exit (pass
# BREWFS_SMOKE_KEEP=1 to inspect them afterwards).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.redis-perf.yml"
COMPOSE_SERVICE="perf"
REDIS_CONTAINER="redis-brewfs-perf"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
info() { log "INFO  $*"; }
ok()   { log "OK    $*"; }
err()  { log "ERROR $*" >&2; }

git_repo="${BREWFS_SMOKE_GIT_REPO:-https://github.com/octocat/Hello-World.git}"
run_stamp="$(date +%s)-$$"
artifact_dir="${BREWFS_SMOKE_ARTIFACT_DIR:-$SCRIPT_DIR/artifacts/s3-git-clone-smoke-$run_stamp}"

mkdir -p "$artifact_dir"

compose() { docker compose -f "$COMPOSE_FILE" "$@"; }

teardown() {
    if [[ "${BREWFS_SMOKE_KEEP:-0}" == "1" ]]; then
        info "keeping compose services and volumes (BREWFS_SMOKE_KEEP=1)"
        return 0
    fi
    info "stopping compose services and removing volumes"
    compose down -v --remove-orphans >/dev/null 2>&1 || true
}

on_exit() {
    local status=$?
    teardown
    if (( status == 0 )); then
        ok "s3 git-clone smoke passed; artifacts: $artifact_dir"
    else
        err "s3 git-clone smoke FAILED (exit=$status); artifacts: $artifact_dir"
    fi
    exit "$status"
}
trap on_exit EXIT

if ! command -v docker >/dev/null 2>&1; then
    err "docker is required"
    exit 1
fi

info "git repo under test: $git_repo"
info "artifacts: $artifact_dir"

if [[ "${BREWFS_SMOKE_BUILD:-0}" == "1" ]]; then
    info "building $COMPOSE_SERVICE image"
    compose build "$COMPOSE_SERVICE"
fi

info "starting Redis and RustFS backends"
compose up -d redis rustfs

info "waiting for the S3 bucket to exist"
compose run --rm rustfs-init >/dev/null

network="${BREWFS_SMOKE_NETWORK:-}"
if [[ -z "$network" ]]; then
    network="$(docker inspect "$REDIS_CONTAINER" \
        --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}' 2>/dev/null || true)"
fi
if [[ -z "$network" ]]; then
    err "could not determine the compose network; set BREWFS_SMOKE_NETWORK"
    exit 1
fi
info "compose network: $network"

inner="$artifact_dir/git_clone_smoke_inner.sh"
cat >"$inner" <<'SMOKE_INNER'
#!/usr/bin/env bash
#
# Runs INSIDE the perf container, on the compose network, with the backends up.
set -uo pipefail

artifact_dir="${SMOKE_ARTIFACT_DIR:-/smoke-artifacts}"
log_file="$artifact_dir/brewfs.log"
mount_stdout="$artifact_dir/mount-stdout.log"
summary_file="$artifact_dir/summary.txt"
config_path="/run/brewfs/config.yaml"
mount_dir="/mnt/brewfs"
cache_root="/var/lib/brewfs/cache"
git_repo="${SMOKE_GIT_REPO:?SMOKE_GIT_REPO must be set}"

smoke_info() { printf '[s3-git-clone-smoke] %s\n' "$*"; }
smoke_fail() { printf '[s3-git-clone-smoke] ERROR %s\n' "$*" >&2; exit 1; }

mkdir -p "$artifact_dir"

# Guard the sharp edge that makes this smoke worthwhile: without usable S3
# credentials the write path accepts data and then fails to commit it, and the
# only signal is a WARN in the daemon log (or an EIO much later). Fail loudly
# and early instead.
: "${BREWFS_META_URL:?BREWFS_META_URL must be set (Redis URL)}"
: "${BREWFS_S3_ENDPOINT:?BREWFS_S3_ENDPOINT must be set}"
: "${BREWFS_S3_BUCKET:?BREWFS_S3_BUCKET must be set}"
: "${BREWFS_S3_REGION:?BREWFS_S3_REGION must be set}"
: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID must be set for the S3 backend}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY must be set for the S3 backend}"

if ! command -v git >/dev/null 2>&1; then
    smoke_info "installing git"
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq >/dev/null 2>&1 || smoke_fail "apt-get update failed"
    apt-get install -y -qq git >/dev/null 2>&1 || smoke_fail "apt-get install git failed"
fi
smoke_info "git version: $(git --version)"

mkdir -p "$(dirname "$config_path")" "$mount_dir" "$cache_root"

cat >"$config_path" <<YAML
mount_point: $mount_dir

data:
  backend: s3
  s3:
    bucket: $BREWFS_S3_BUCKET
    region: $BREWFS_S3_REGION
    part_size: ${BREWFS_S3_PART_SIZE:-16777216}
    max_concurrency: ${BREWFS_S3_MAX_CONCURRENCY:-8}
    force_path_style: ${BREWFS_S3_FORCE_PATH_STYLE:-true}
    disable_payload_checksum: ${BREWFS_S3_DISABLE_PAYLOAD_CHECKSUM:-true}
    endpoint: $BREWFS_S3_ENDPOINT

meta:
  backend: redis
  redis:
    url: "$BREWFS_META_URL"

cache:
  root: $cache_root
YAML

smoke_info "config:"
sed 's/^/    /' "$config_path"

brewfs_pid=""

mount_brewfs() {
    local label="$1" i
    export RUST_LOG="${RUST_LOG:-brewfs=warn}"
    export BREWFS_LOG_FILE="$log_file"
    /usr/local/bin/brewfs mount --privileged --config "$config_path" "$mount_dir" \
        >>"$mount_stdout" 2>&1 &
    brewfs_pid=$!
    for i in $(seq 1 40); do
        if findmnt -rn --target "$mount_dir" --output FSTYPE 2>/dev/null | grep -q fuse; then
            smoke_info "mounted ($label) after ${i}s"
            return 0
        fi
        if ! kill -0 "$brewfs_pid" 2>/dev/null; then
            break
        fi
        sleep 1
    done
    printf '%s\n' "--- mount stdout ---" >>"$summary_file"
    tail -20 "$mount_stdout" >>"$summary_file" 2>/dev/null || true
    printf '%s\n' "--- daemon log ---" >>"$summary_file"
    tail -40 "$log_file" >>"$summary_file" 2>/dev/null || true
    smoke_fail "mount ($label) did not come up"
}

unmount_brewfs() {
    local i
    cd /
    if ! umount "$mount_dir"; then
        smoke_fail "failed to unmount $mount_dir"
    fi
    for i in $(seq 1 10); do
        if ! findmnt -rn --mountpoint "$mount_dir" >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    if findmnt -rn --mountpoint "$mount_dir" >/dev/null 2>&1; then
        smoke_fail "$mount_dir is still mounted after unmount"
    fi
    if [[ -n "$brewfs_pid" ]]; then
        if kill -0 "$brewfs_pid" 2>/dev/null; then
            kill "$brewfs_pid" 2>/dev/null || smoke_fail "failed to stop brewfs daemon"
        fi
        wait "$brewfs_pid" 2>/dev/null || true
        brewfs_pid=""
    fi
}

checksum_tree() {
    local out="$1"
    find "$mount_dir" -type f -exec md5sum {} + 2>/dev/null | sort -k2 >"$out"
    wc -l <"$out"
}

repo_name="$(basename "$git_repo" .git)"

smoke_info "phase 1: mount"
mount_brewfs "phase-1"

smoke_info "phase 1: buffered write round-trip"
printf 'hello-brewfs\n' >"$mount_dir/hello.txt"
echo "write exit=$?" >>"$summary_file"
size="$(stat -c '%s' "$mount_dir/hello.txt")"
if [[ "$size" != "13" ]]; then
    smoke_fail "expected hello.txt to be 13 bytes after write, got '$size'"
fi
if [[ "$(cat "$mount_dir/hello.txt")" != "hello-brewfs" ]]; then
    smoke_fail "hello.txt content did not round-trip"
fi
smoke_info "write round-trip OK (13 bytes)"

smoke_info "phase 1: git clone"
clone_start="$(date +%s%N)"
# github.com TLS connections are occasionally cut mid-handshake from CI-style
# runners. Retry so this smoke measures filesystem correctness, not egress luck.
clone_ms=""
for attempt in 1 2 3; do
    if git clone --depth 1 "$git_repo" "$mount_dir/$repo_name" \
        >"$artifact_dir/git-clone.log" 2>&1; then
        clone_end="$(date +%s%N)"
        clone_ms=$(( (clone_end - clone_start) / 1000000 ))
        smoke_info "git clone OK (attempt ${attempt}, ${clone_ms} ms)"
        break
    fi
    if (( attempt == 3 )); then
        cat "$artifact_dir/git-clone.log" >&2
        smoke_fail "git clone failed after 3 attempts"
    fi
    cat "$artifact_dir/git-clone.log" >&2
    smoke_info "git clone attempt ${attempt} failed, retrying"
    rm -rf "$mount_dir/$repo_name"
    sleep 2
done
if [[ -z "$clone_ms" ]]; then
    smoke_fail "git clone never succeeded"
fi

(
    cd "$mount_dir/$repo_name"
    git log --oneline >"$artifact_dir/git-log.txt" 2>&1 || exit 1
    git status --short >"$artifact_dir/git-status.txt" 2>&1 || exit 1
    if [[ -s "$artifact_dir/git-status.txt" ]]; then
        cat "$artifact_dir/git-status.txt" >&2
        exit 1
    fi
    git fsck --no-progress >"$artifact_dir/git-fsck.txt" 2>&1 || exit 1
) || smoke_fail "cloned worktree is not self-consistent"
head -1 "$artifact_dir/git-log.txt"
smoke_info "git log / status / fsck OK"

smoke_info "phase 1: synthetic source tree"
mkdir -p "$mount_dir/tree/src" "$mount_dir/tree/docs"
for i in $(seq 1 200); do
    printf 'fn func_%s() { println!("hello %s"); }\n' "$i" "$i" \
        >"$mount_dir/tree/src/mod_$i.rs"
done
for i in $(seq 1 50); do
    echo "doc $i" >"$mount_dir/tree/docs/doc_$i.md"
done
sync

before_count="$(checksum_tree "$artifact_dir/before.md5")"
smoke_info "phase 1: $before_count files checksummed"
if [[ "$before_count" -lt 2 ]]; then
    smoke_fail "expected the clone plus synthetic tree, got $before_count files"
fi

smoke_info "phase 2: unmount, wipe the local block cache, remount"
unmount_brewfs
if ! find "${cache_root:?}" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +; then
    smoke_fail "failed to wipe local cache"
fi
cache_entry="$(find "$cache_root" -mindepth 1 -maxdepth 1 -print -quit)" \
    || smoke_fail "failed to verify local cache"
if [[ -n "$cache_entry" ]]; then
    smoke_fail "local cache is not empty after wipe: $cache_entry"
fi
mount_brewfs "phase-2"

after_count="$(checksum_tree "$artifact_dir/after.md5")"
if [[ "$after_count" != "$before_count" ]]; then
    smoke_fail "file count changed across remount: $before_count -> $after_count"
fi
if ! diff -u "$artifact_dir/before.md5" "$artifact_dir/after.md5" >"$artifact_dir/checksum.diff" 2>&1; then
    head -40 "$artifact_dir/checksum.diff" >&2
    smoke_fail "checksums differ after a cache-wiped remount"
fi
smoke_info "cache-wiped remount OK: $after_count files identical"

if ! (cd "$mount_dir/$repo_name" && git fsck --no-progress >"$artifact_dir/git-fsck-after-remount.txt" 2>&1); then
    cat "$artifact_dir/git-fsck-after-remount.txt" >&2
    smoke_fail "git fsck failed after the cache-wiped remount"
fi
smoke_info "git fsck after remount OK"

warning_count="$(grep -ciE 'WARN|ERROR' "$log_file" 2>/dev/null || true)"
warning_count="${warning_count:-0}"
if [[ "$warning_count" != "0" ]]; then
    grep -iE 'WARN|ERROR' "$log_file" | head -20 >&2
    smoke_fail "daemon log contains $warning_count WARN/ERROR lines"
fi
smoke_info "daemon log is clean"

unmount_brewfs

{
    echo "git_repo=$git_repo"
    echo "clone_ms=$clone_ms"
    echo "files_before_remount=$before_count"
    echo "files_after_remount=$after_count"
    echo "daemon_warn_or_error_lines=$warning_count"
    echo "status=pass"
} >"$summary_file"

smoke_info "inner smoke PASSED"
SMOKE_INNER

# The inner driver runs on Linux; a Windows checkout with CRLF would break it.
sed -i 's/\r$//' "$inner"
bash -n "$inner" || { err "inner driver failed bash -n"; exit 1; }

info "running the mount + git clone smoke inside $COMPOSE_SERVICE"
if ! compose run --rm \
    --entrypoint bash \
    -e BREWFS_DATA_BACKEND=s3 \
    -e BREWFS_META_BACKEND=redis \
    -e SMOKE_ARTIFACT_DIR=/smoke-artifacts \
    -e SMOKE_GIT_REPO="$git_repo" \
    -e RUST_LOG=brewfs=warn \
    -v "$inner:/opt/smoke/git_clone_smoke_inner.sh:ro" \
    -v "$artifact_dir:/smoke-artifacts" \
    "$COMPOSE_SERVICE" /opt/smoke/git_clone_smoke_inner.sh; then
    err "inner smoke failed; see $artifact_dir"
    exit 1
fi
ok "inner smoke passed"

info "verifying the bucket actually received objects"
object_count="$(docker run --rm --network "$network" \
    -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-rustfsadmin}" \
    -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-rustfsadmin}" \
    -e AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}" \
    -e AWS_EC2_METADATA_DISABLED=true \
    amazon/aws-cli:latest \
    s3api list-objects-v2 \
    --endpoint-url "${BREWFS_S3_ENDPOINT:-http://rustfs:9000}" \
    --bucket "${BREWFS_S3_BUCKET:-brewfs-data}" \
    --query 'length(Contents)' --output text 2>/dev/null || echo "0")"
object_count="${object_count%$'\r'}"
object_count="${object_count:-0}"
if [[ "$object_count" == "None" || "$object_count" == "0" ]]; then
    err "no objects found in the S3 bucket; the data backend was not exercised"
    exit 1
fi

{
    echo "s3_bucket_objects=$object_count"
    echo "artifact_dir=$artifact_dir"
} >>"$artifact_dir/summary.txt"

ok "bucket holds $object_count objects"
ok "summary:"
sed 's/^/    /' "$artifact_dir/summary.txt"
