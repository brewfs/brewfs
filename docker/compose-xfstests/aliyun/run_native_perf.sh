#!/usr/bin/env bash

set -Eeuo pipefail

MODE="${1:?usage: run_native_perf.sh brewfs|juicefs [runner args ...]}"
shift

WORK_ROOT="${BREWFS_PERF_WORK_ROOT:-/opt/brewfs-perf}"
SOURCE_ROOT="${BREWFS_PERF_SOURCE_ROOT:-$WORK_ROOT/source/brewfs}"
DATA_ROOT="${BREWFS_PERF_DATA_ROOT:-/mnt/brewfs-perf-data}"
REDIS_PORT="${REDIS_HOST_PORT:-16379}"
S3_PORT="${RUSTFS_S3_HOST_PORT:-19000}"
MANAGED_BACKEND="${BREWFS_MANAGED_BACKEND:-false}"
S3_ENDPOINT="${BREWFS_S3_ENDPOINT:-http://127.0.0.1:${S3_PORT}}"
S3_BUCKET="${BREWFS_S3_BUCKET:-brewfs-data}"
S3_REGION="${BREWFS_S3_REGION:-us-east-1}"
REDIS_URL="${BREWFS_META_URL:-${JFS_META_URL:-redis://127.0.0.1:${REDIS_PORT}/0}}"
S3_ACCESS_KEY="${BREWFS_S3_ACCESS_KEY_ID:-${AWS_ACCESS_KEY_ID:-rustfsadmin}}"
S3_SECRET_KEY="${BREWFS_S3_SECRET_ACCESS_KEY:-${AWS_SECRET_ACCESS_KEY:-rustfsadmin}}"

# Aliyun OSS rejects path-style object requests with SecondLevelDomainForbidden,
# while the local RustFS fixture needs path-style addressing. Derive both the
# reachability probe and the object clients' addressing style from the endpoint.
S3_ENDPOINT_HOST="${S3_ENDPOINT#*://}"
S3_ENDPOINT_HOST="${S3_ENDPOINT_HOST%%/*}"
# In-region ECS traffic must stay on the OSS internal endpoint: the public
# endpoint is capped by the instance internet bandwidth (20 Mbps here), which
# alone pushed object PUTs past the client timeout and failed whole fio scenes.
if [[ "$S3_ENDPOINT_HOST" == oss-*.aliyuncs.com && "$S3_ENDPOINT_HOST" != *-internal.aliyuncs.com ]]; then
    internal_host="${S3_ENDPOINT_HOST%.aliyuncs.com}-internal.aliyuncs.com"
    if curl -s -o /dev/null --max-time 10 "https://${internal_host}/" 2>/dev/null; then
        echo "[native-perf] using OSS internal endpoint: $internal_host"
        S3_ENDPOINT_HOST="$internal_host"
        S3_ENDPOINT="https://${S3_ENDPOINT_HOST}"
    else
        echo "[native-perf] OSS internal endpoint unreachable; keeping $S3_ENDPOINT_HOST"
    fi
fi
if [[ "$S3_ENDPOINT_HOST" == oss-*.aliyuncs.com ]]; then
    S3_VIRTUAL_BUCKET_URL="https://${S3_BUCKET}.${S3_ENDPOINT_HOST}"
    S3_FORCE_PATH_STYLE_DEFAULT=false
else
    S3_VIRTUAL_BUCKET_URL="${S3_ENDPOINT%/}/${S3_BUCKET}"
    S3_FORCE_PATH_STYLE_DEFAULT=true
fi
PERF_TOOLS_VALUE="${PERF_TOOLS:-fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw dirstress dirperf metaperf looptest}"
DATA_BACKEND_VALUE="${BREWFS_DATA_BACKEND:-s3}"
SERVICES_ROOT="$DATA_ROOT/services"
REDIS_DIR="$SERVICES_ROOT/redis"
RUSTFS_DIR="$SERVICES_ROOT/rustfs"
LOG_ROOT="$DATA_ROOT/service-logs"
log() { echo "[$(date '+%H:%M:%S')] $*"; }
die() { log "ERROR $*" >&2; exit 1; }
is_truthy() { case "${1:-}" in 1|true|TRUE|yes|YES|on|ON) return 0 ;; *) return 1 ;; esac; }

# Fail fast when a profile's memory demand cannot fit on the host. The Aliyun
# 2026-09-17 round on an 8 GiB ECS (ecs.u1-c1m2.xlarge) did not fail here: the
# writeback profile asks BrewFS for a 4 GiB read cache plus a 4 GiB write cache
# under a 12 GiB budget, and the harness adds a 4 GiB fio prefill on top. That
# box spent 45+ minutes paging, pinning the 80 GiB system disk at 170 MB/s with
# 55-70 ms read latency while the data disk sat completely idle, and never
# produced an artifact. The same matrix finishes in about 9 minutes on 16 GiB.
#
# The writeback profile now asks for 4 GiB of cache in memory (2 GiB read plus
# 2 GiB write, the JuiceFS-matched split), so an 8 GiB host would land exactly on
# the boundary of its 8 GiB demand. The fixed headroom below keeps that host
# refused: both aligned profiles need 10 GiB and are run on
# ecs.u1-c1m2.2xlarge (16 GiB).
require_memory_headroom() {
    local required_bytes="$1"
    local description="$2"
    if is_truthy "${BREWFS_PERF_ALLOW_UNDERSIZED:-false}"; then
        log "跳过内存预检（BREWFS_PERF_ALLOW_UNDERSIZED）：$description"
        return 0
    fi
    # The cache budgets and the fio working set are not the whole footprint: the
    # kernel, the page cache and the runner itself need room too. Measured on the
    # 2026-09-17 round, a host that only just covers the caches still parks at a
    # few hundred MiB free and spends the run in iowait. Require a fixed margin so
    # the check refuses an 8 GiB box under either the old 12 GiB shape or the
    # aligned 8 GiB shape.
    local headroom_bytes=2147483648
    # BREWFS_PERF_MEMINFO lets the check be exercised against a synthetic file
    # instead of requiring a real undersized machine.
    local meminfo="${BREWFS_PERF_MEMINFO:-/proc/meminfo}"
    local mem_total_kib
    mem_total_kib="$(awk '/^MemTotal:/ {print $2; exit}' "$meminfo" 2>/dev/null || true)"
    if [[ -z "$mem_total_kib" ]]; then
        log "WARN 无法读取 $meminfo，跳过内存预检"
        return 0
    fi
    local mem_total_bytes=$(( mem_total_kib * 1024 ))
    local need_bytes=$(( required_bytes + headroom_bytes ))
    if (( mem_total_bytes < need_bytes )); then
        die "拒绝启动：$description 再加 $(( headroom_bytes / 1024 / 1024 ))MiB 余量共需 $(( need_bytes / 1024 / 1024 ))MiB 内存，本机只有 $(( mem_total_bytes / 1024 / 1024 ))MiB；换更大的 ECS 规格，或用 BREWFS_PERF_ALLOW_UNDERSIZED=1 显式承担换页停顿。"
    fi
}

# The VM flow uploads a single BrewFS binary and reuses the harness from the
# prepared image. Install that binary so the shared runner can mount it.
install_brewfs_binary() {
    local candidate
    for candidate in \
        "${BREWFS_BINARY_PATH:-}" \
        "$SOURCE_ROOT/target/release/brewfs" \
        "$SOURCE_ROOT/target/docker/brewfs" \
        /usr/local/bin/brewfs; do
        [[ -n "$candidate" && -f "$candidate" ]] || continue
        if [[ "$candidate" != /usr/local/bin/brewfs ]]; then
            if [[ -w /usr/local/bin ]]; then
                install -m 0755 "$candidate" /usr/local/bin/brewfs
            else
                sudo install -m 0755 "$candidate" /usr/local/bin/brewfs
            fi
            log "brewfs binary installed from $candidate"
        fi
        log "brewfs binary sha256: $(sha256sum /usr/local/bin/brewfs | awk '{print $1}')"
        return 0
    done
    die "no BrewFS binary found; upload one to $SOURCE_ROOT/target/release/brewfs"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --s3) DATA_BACKEND_VALUE=s3; shift ;;
        --local-fs) DATA_BACKEND_VALUE=local-fs; shift ;;
        --tools)
            [[ $# -ge 2 ]] || die '--tools requires a value'
            PERF_TOOLS_VALUE="$2"
            shift 2
            ;;
        --read-throughput-profile)
            export BREWFS_FUSE_READ_DIRECT_IO="${BREWFS_FUSE_READ_DIRECT_IO:-1}"
            shift
            ;;
        --metadata-throughput-profile)
            export BREWFS_METADATA_OPEN_CACHE_TTL_MS="${BREWFS_METADATA_OPEN_CACHE_TTL_MS:-1000}"
            export BREWFS_METADATA_OPEN_CACHE_CAPACITY="${BREWFS_METADATA_OPEN_CACHE_CAPACITY:-65536}"
            export BREWFS_METADATA_ALLOW_WRITE_OPEN_CACHE="${BREWFS_METADATA_ALLOW_WRITE_OPEN_CACHE:-true}"
            export JFS_OPEN_CACHE="${JFS_OPEN_CACHE:-1s}"
            export JFS_OPEN_CACHE_LIMIT="${JFS_OPEN_CACHE_LIMIT:-65536}"
            export JFS_BACKUP_META="${JFS_BACKUP_META:-0}"
            export JFS_NO_USAGE_REPORT="${JFS_NO_USAGE_REPORT:-true}"
            shift
            ;;
        --cached-read-throughput-profile)
            export JFS_COMPRESS="${JFS_COMPRESS:-none}"
            export JFS_WRITEBACK="${JFS_WRITEBACK:-false}"
            export JFS_BUFFER_SIZE_MIB="${JFS_BUFFER_SIZE_MIB:-4096}"
            export JFS_CACHE_SIZE_MIB="${JFS_CACHE_SIZE_MIB:-8192}"
            export JFS_CACHE_LARGE_WRITE="${JFS_CACHE_LARGE_WRITE:-true}"
            export JFS_MAX_UPLOADS="${JFS_MAX_UPLOADS:-4}"
            export JFS_MAX_STAGE_WRITE="${JFS_MAX_STAGE_WRITE:-4}"
            export JFS_MAX_READAHEAD_MIB="${JFS_MAX_READAHEAD_MIB:-1024}"
            export JFS_PREFETCH="${JFS_PREFETCH:-4}"
            export JFS_OPEN_CACHE="${JFS_OPEN_CACHE:-1s}"
            export JFS_OPEN_CACHE_LIMIT="${JFS_OPEN_CACHE_LIMIT:-65536}"
            export JFS_CACHE_DIR="${JFS_CACHE_DIR:-$DATA_ROOT/cache/juicefs}"
            export PERF_FIO_PREFILL_DRAIN="${PERF_FIO_PREFILL_DRAIN:-true}"
            export PERF_FIO_PREFILL_REMOUNT="${PERF_FIO_PREFILL_REMOUNT:-true}"
            shift
            ;;
        --writeback-throughput-profile)
            if [[ "$MODE" == brewfs ]]; then
                export BREWFS_WRITEBACK_MODE="${BREWFS_WRITEBACK_MODE:-commit_before_upload}"
                export BREWFS_CACHE_ROOT="${BREWFS_CACHE_ROOT:-$DATA_ROOT/cache/brewfs}"
                # Match the cache budgets of the JuiceFS leg of this same
                # profile: --buffer-size 4096 (4 GiB in memory) and
                # --cache-size 8192 (8 GiB on disk). The previous 4 GiB + 4 GiB
                # in-memory split was twice JuiceFS's memory tier, so the two
                # legs of one comparison round were not configured alike.
                # Re-measured with this split (2026-09-17, aliyun/README.md 5.2)
                # the host looked the same -- 185 MiB free and 31% average
                # iowait against 175 MiB and 32% -- so the old numbers were not
                # being held back by these budgets. Keep the split for
                # comparability, not because it buys throughput:
                # test_native_perf_budget_parity.sh is what holds the two legs
                # equal from here on.
                export BREWFS_READ_MEMORY_BYTES="${BREWFS_READ_MEMORY_BYTES:-2147483648}"
                export BREWFS_WRITE_MEMORY_BYTES="${BREWFS_WRITE_MEMORY_BYTES:-2147483648}"
                export BREWFS_READ_SSD_BYTES="${BREWFS_READ_SSD_BYTES:-4294967296}"
                export BREWFS_WRITE_SSD_BYTES="${BREWFS_WRITE_SSD_BYTES:-4294967296}"
                export BREWFS_MEMORY_BUDGET_BYTES="${BREWFS_MEMORY_BUDGET_BYTES:-8589934592}"
                export BREWFS_S3_MAX_CONCURRENCY="${BREWFS_S3_MAX_CONCURRENCY:-16}"
                export BREWFS_WRITEBACK_UPLOAD_CONCURRENCY="${BREWFS_WRITEBACK_UPLOAD_CONCURRENCY:-6}"
                export BREWFS_UPLOAD_CONCURRENCY="${BREWFS_UPLOAD_CONCURRENCY:-32}"
                export BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES="${BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES:-2147483648}"
                export BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES="${BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES:-3221225472}"
                export BREWFS_WRITEBACK_PERSIST_SYNC="${BREWFS_WRITEBACK_PERSIST_SYNC:-false}"
                export BREWFS_CACHED_BLOCK_ASSEMBLER="${BREWFS_CACHED_BLOCK_ASSEMBLER:-true}"
                export BREWFS_POPULATE_WRITE_CACHE_AFTER_UPLOAD="${BREWFS_POPULATE_WRITE_CACHE_AFTER_UPLOAD:-true}"
                export BREWFS_PERSIST_WRITE_CACHE_AFTER_UPLOAD="${BREWFS_PERSIST_WRITE_CACHE_AFTER_UPLOAD:-true}"
                export BREWFS_COMPRESSION="${BREWFS_COMPRESSION:-none}"
                export BREWFS_VERIFY_CACHE_CHECKSUM="${BREWFS_VERIFY_CACHE_CHECKSUM:-full}"
                # Match the local compose profile. Linux kernels before the
                # bdi->io_pages FUSE fix split buffered large reads into 256 KiB
                # requests even after accepting FUSE_MAX_PAGES, while read-only
                # FOPEN_DIRECT_IO reaches the negotiated 1 MiB kernel cap.
                export BREWFS_FUSE_READ_DIRECT_IO="${BREWFS_FUSE_READ_DIRECT_IO:-1}"
                # Match the JuiceFS comparison leg's 1 GiB max readahead.
                export BREWFS_PREFETCH_MAX_BYTES="${BREWFS_PREFETCH_MAX_BYTES:-1073741824}"
                export BREWFS_FUSE_WORKERS="${BREWFS_FUSE_WORKERS:-16}"
                export BREWFS_FUSE_MAX_BACKGROUND="${BREWFS_FUSE_MAX_BACKGROUND:-512}"
                export PERF_FIO_PREFILL_DRAIN="${PERF_FIO_PREFILL_DRAIN:-true}"
                export PERF_FIO_PREFILL_REMOUNT="${PERF_FIO_PREFILL_REMOUNT:-true}"
                export PERF_FIO_POST_WRITE_DRAIN="${PERF_FIO_POST_WRITE_DRAIN:-true}"
                require_memory_headroom "$(( BREWFS_READ_MEMORY_BYTES + BREWFS_WRITE_MEMORY_BYTES + 4294967296 ))" \
                    "BrewFS ${BREWFS_READ_MEMORY_BYTES}B 读缓存 + ${BREWFS_WRITE_MEMORY_BYTES}B 写缓存 + 4GiB fio 预填充"
            else
                export JFS_COMPRESS="${JFS_COMPRESS:-none}"
                export JFS_WRITEBACK="${JFS_WRITEBACK:-true}"
                export JFS_BUFFER_SIZE_MIB="${JFS_BUFFER_SIZE_MIB:-4096}"
                export JFS_CACHE_SIZE_MIB="${JFS_CACHE_SIZE_MIB:-8192}"
                export JFS_CACHE_LARGE_WRITE="${JFS_CACHE_LARGE_WRITE:-true}"
                export JFS_MAX_UPLOADS="${JFS_MAX_UPLOADS:-4}"
                export JFS_MAX_STAGE_WRITE="${JFS_MAX_STAGE_WRITE:-4}"
                export JFS_MAX_DOWNLOADS="${JFS_MAX_DOWNLOADS:-8}"
                export JFS_OPEN_CACHE="${JFS_OPEN_CACHE:-1s}"
                export JFS_OPEN_CACHE_LIMIT="${JFS_OPEN_CACHE_LIMIT:-65536}"
                export JFS_BACKUP_META="${JFS_BACKUP_META:-0}"
                export JFS_NO_USAGE_REPORT="${JFS_NO_USAGE_REPORT:-true}"
                export JFS_CACHE_DIR="${JFS_CACHE_DIR:-$DATA_ROOT/cache/juicefs}"
                export PERF_FIO_PREFILL_DRAIN="${PERF_FIO_PREFILL_DRAIN:-true}"
                export PERF_FIO_PREFILL_REMOUNT="${PERF_FIO_PREFILL_REMOUNT:-true}"
                export PERF_FIO_COLD_READ_CLEAR_CACHE="${PERF_FIO_COLD_READ_CLEAR_CACHE:-false}"
                export PERF_FIO_POST_WRITE_DRAIN="${PERF_FIO_POST_WRITE_DRAIN:-true}"
                require_memory_headroom "$(( JFS_BUFFER_SIZE_MIB * 1048576 + 4294967296 ))" \
                    "JuiceFS ${JFS_BUFFER_SIZE_MIB}MiB 写回缓冲 + 4GiB fio 预填充"
            fi
            shift
            ;;
        *) shift ;;
    esac
done
export PERF_TOOLS="$PERF_TOOLS_VALUE"
export BREWFS_DATA_BACKEND="$DATA_BACKEND_VALUE"

mkdir -p "$REDIS_DIR" "$RUSTFS_DIR" "$LOG_ROOT" "$DATA_ROOT/artifacts" \
    "$DATA_ROOT/cache" "$DATA_ROOT/mnt"
ARTIFACT_ROOT="$SOURCE_ROOT/docker/compose-xfstests/artifacts"
rm -rf "$ARTIFACT_ROOT"
mkdir -p "$ARTIFACT_ROOT"

start_redis() {
    if redis-cli -h 127.0.0.1 -p "$REDIS_PORT" ping >/dev/null 2>&1; then
        return 0
    fi
    redis-server \
        --bind 127.0.0.1 \
        --port "$REDIS_PORT" \
        --dir "$REDIS_DIR" \
        --dbfilename dump.rdb \
        --appendonly yes \
        --pidfile "$REDIS_DIR/redis.pid" \
        --logfile "$LOG_ROOT/redis.log" \
        --daemonize yes
    for _ in $(seq 1 60); do
        redis-cli -h 127.0.0.1 -p "$REDIS_PORT" ping >/dev/null 2>&1 && return 0
        sleep 1
    done
    die "native Redis did not become ready"
}

start_rustfs() {
    command -v rustfs >/dev/null 2>&1 || die "native rustfs binary is missing from the VM image"
    command -v aws >/dev/null 2>&1 || die "aws CLI is missing from the VM image"

    if ! curl -fsS --max-time 2 "$S3_ENDPOINT" >/dev/null 2>&1; then
        RUSTFS_ACCESS_KEY="$S3_ACCESS_KEY" \
        RUSTFS_SECRET_KEY="$S3_SECRET_KEY" \
        RUSTFS_CONSOLE_ENABLE=false \
            nohup rustfs server --address ":$S3_PORT" "$RUSTFS_DIR" \
            >"$LOG_ROOT/rustfs.log" 2>&1 &
        echo $! >"$RUSTFS_DIR/rustfs.pid"
    fi

    export AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY"
    export AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY"
    export AWS_DEFAULT_REGION="$S3_REGION"
    export AWS_EC2_METADATA_DISABLED=true
    for _ in $(seq 1 90); do
        if aws --endpoint-url "$S3_ENDPOINT" --region "$S3_REGION" s3api list-buckets >/dev/null 2>&1; then
            aws --endpoint-url "$S3_ENDPOINT" --region "$S3_REGION" s3api create-bucket --bucket "$S3_BUCKET" >/dev/null 2>&1 || true
            return 0
        fi
        sleep 1
    done
    die "native RustFS did not become ready; see $LOG_ROOT/rustfs.log"
}

start_managed_backends() {
    command -v redis-cli >/dev/null 2>&1 || die "redis-cli is missing from the VM image"
    [[ -n "$REDIS_URL" ]] || die "BREWFS_META_URL/JFS_META_URL is required for managed Redis"

    export AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY"
    export AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY"
    export AWS_DEFAULT_REGION="$S3_REGION"
    export AWS_EC2_METADATA_DISABLED=true
    redis-cli --no-auth-warning -u "$REDIS_URL" ping >/dev/null 2>&1 || die "managed Redis/Tair is not reachable"

    # The local-fs data backend keeps objects on the instance, so managed OSS is
    # only required when the run actually stores data in the bucket.
    if [[ "$DATA_BACKEND_VALUE" == "s3" ]]; then
        command -v aws >/dev/null 2>&1 || die "aws CLI is missing from the VM image"
        [[ -n "$S3_ENDPOINT" && -n "$S3_BUCKET" ]] || die "managed OSS endpoint and bucket are required"
        if [[ "$S3_ENDPOINT_HOST" == oss-*.aliyuncs.com ]]; then
            # HEAD on the virtual-hosted bucket: 200/403 both prove the bucket
            # exists and is reachable; 404 means it is missing.
            oss_probe_status=$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 "${S3_VIRTUAL_BUCKET_URL}/" || echo 000)
            case "$oss_probe_status" in
                200|403) log "managed OSS bucket reachable: $S3_BUCKET (http $oss_probe_status)" ;;
                *) die "managed OSS bucket is not reachable: $S3_BUCKET (http $oss_probe_status)" ;;
            esac
        else
            aws --endpoint-url "$S3_ENDPOINT" --region "$S3_REGION" s3api head-bucket --bucket "$S3_BUCKET" >/dev/null 2>&1 \
                || die "managed OSS bucket is not reachable: $S3_BUCKET"
        fi
        log "using managed Aliyun Redis/Tair and OSS"
    else
        log "using managed Aliyun Redis/Tair with $DATA_BACKEND_VALUE data backend"
    fi
}

cleanup_services() {
    if is_truthy "$MANAGED_BACKEND"; then
        return 0
    fi
    if [[ -f "$RUSTFS_DIR/rustfs.pid" ]]; then
        kill "$(cat "$RUSTFS_DIR/rustfs.pid")" >/dev/null 2>&1 || true
    fi
    if [[ -f "$REDIS_DIR/redis.pid" ]]; then
        redis-cli -h 127.0.0.1 -p "$REDIS_PORT" shutdown nosave >/dev/null 2>&1 || true
    fi
}
trap cleanup_services EXIT INT TERM

if is_truthy "$MANAGED_BACKEND"; then
    start_managed_backends
else
    start_redis
    if [[ "${BREWFS_DATA_BACKEND:-s3}" == "s3" ]]; then
        start_rustfs
    fi
fi

export BREWFS_PERF_SOURCE_ROOT="$SOURCE_ROOT"
export BREWFS_HOME="${BREWFS_HOME:-$DATA_ROOT/cache/brewfs}"
export BREWFS_MOUNT_POINT="${BREWFS_MOUNT_POINT:-$DATA_ROOT/mnt/brewfs}"
export BREWFS_DATA_DIR="${BREWFS_DATA_DIR:-$DATA_ROOT/cache/brewfs-data}"
export BREWFS_META_BACKEND=redis
export BREWFS_META_URL="$REDIS_URL"
export BREWFS_S3_ENDPOINT="$S3_ENDPOINT"
export BREWFS_S3_BUCKET="$S3_BUCKET"
export BREWFS_S3_REGION="$S3_REGION"
export BREWFS_S3_FORCE_PATH_STYLE="${BREWFS_S3_FORCE_PATH_STYLE:-$S3_FORCE_PATH_STYLE_DEFAULT}"
export BREWFS_S3_ACCESS_KEY_ID="$S3_ACCESS_KEY"
export BREWFS_S3_SECRET_ACCESS_KEY="$S3_SECRET_KEY"
export BREWFS_ARTIFACT_ROOT="$ARTIFACT_ROOT"
export XFSTESTS_DIR="${XFSTESTS_DIR:-/opt/xfstests-dev}"
export AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY"
export AWS_DEFAULT_REGION="$S3_REGION"
export AWS_EC2_METADATA_DISABLED=true

case "$MODE" in
    brewfs)
        install_brewfs_binary
        exec bash "$SOURCE_ROOT/docker/compose-xfstests/run_perf_in_container.sh" "$@"
        ;;
    juicefs)
        export JFS_MOUNT_POINT="${JFS_MOUNT_POINT:-$DATA_ROOT/mnt/juicefs}"
        export JFS_META_URL="$REDIS_URL"
        export JFS_S3_ENDPOINT="$S3_ENDPOINT"
        export JFS_S3_BUCKET="$S3_BUCKET"
        export JFS_S3_REGION="$S3_REGION"
        export JFS_S3_BUCKET_URL="$S3_VIRTUAL_BUCKET_URL"
        export JFS_CACHE_DIR="${JFS_CACHE_DIR:-$DATA_ROOT/cache/juicefs}"
        exec bash "$SOURCE_ROOT/docker/compose-xfstests/run_juicefs_perf_in_container.sh" "$@"
        ;;
    *)
        die "unsupported native workload: $MODE"
        ;;
esac
