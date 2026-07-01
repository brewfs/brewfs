#!/usr/bin/env bash
# BrewFS performance profiling + flame graph generation
#
# Prerequisites: docker, cargo, fio, linux-perf, inferno, python3
#
# Usage:
#   ./run_perf.sh              # Full run: build, benchmark, flame graph
#   ./run_perf.sh --no-build   # Skip rebuild
#   ./run_perf.sh --quick      # Shorter benchmarks for quick iteration
#   ./run_perf.sh --no-cleanup # Leave containers and mount point after run
#   ./run_perf.sh --compress lz4  # Enable LZ4 compression
#   ./run_perf.sh --compress zstd # Enable Zstd compression
#
# Useful environment knobs:
#   PERF_FIO_WORKLOADS="randrw" PERF_FIO_DIRECT=1 ./run_perf.sh --quick --skip-oncpu --skip-offcpu
#   PERF_RECORD_FREQ=19          # Lower perf sample frequency to keep perf.data smaller.
#   PERF_EVENT=task-clock         # Software event used for on-CPU profiling.
#   PERF_MMAP_PAGES=8             # Smaller perf mmap buffer for space-constrained hosts.
#   KEEP_PERF_DATA=1             # Preserve perf.data; default removes it after flame/report generation.
#   PERF_WORK_BASE=/mnt/perf     # Base directory for mount/cache staging.
#   PERF_WARMUP_DRAIN=false      # Skip post-warmup writeback drain before measured fio.
#   PERF_POST_FIO_DRAIN=false    # Skip writeback drain between write-heavy fio workloads.
#
# For detailed libc frames, install matching system debuginfo first:
#   Ubuntu/Debian: apt-get install libc6-dbg
# Without it, libc frames may show up as [libc.so.6] or raw addresses.
#
# Results are saved to: tools/perf/results/<timestamp>/
#   ├── config.yaml       # Mount config used
#   ├── diagnostics/      # BrewFS .stats snapshots
#   ├── flame/            # Flame graphs (.svg) and folded stacks
#   ├── fio/              # Fio JSON outputs
#   ├── llm-report.txt    # LLM-readable analysis
#   └── report.md         # Markdown report

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Results directory: tools/perf/results/<timestamp>
RUN_TS="$(date +%Y%m%d-%H%M%S)"
RESULTS_BASE="$SCRIPT_DIR/results"
RUN_DIR="$RESULTS_BASE/$RUN_TS"

# Working directory (temp, cleaned up). Keep it under the repository target
# directory by default: commit-before-upload staging can need multiple GiB, and
# /tmp is often backed by a much smaller root partition in CI/dev hosts.
WORK_BASE="${PERF_WORK_BASE:-$PROJECT_DIR/target/perf-work}"
WORK_DIR="$WORK_BASE/$RUN_TS-$$"
CONFIG_PATH="$WORK_DIR/config.yaml"
MNT_DIR="$WORK_DIR/mnt"
DATA_DIR="$WORK_DIR/data"
CACHE_DIR="$WORK_DIR/cache"

REDIS_PORT="${REDIS_PORT:-16379}"
RUSTFS_S3_PORT="${RUSTFS_S3_PORT:-19000}"
S3_BUCKET="${S3_BUCKET:-brewfs-perf-data}"
RUSTFS_ACCESS_KEY="${RUSTFS_ACCESS_KEY:-rustfsadmin}"
RUSTFS_SECRET_KEY="${RUSTFS_SECRET_KEY:-rustfsadmin}"

BUILD=1
QUICK=0
CLEANUP=1
SKIP_ONCPU=0
SKIP_OFFCPU=0
COMPRESSION="${BREWFS_COMPRESSION:-none}"
VERIFY_CACHE_CHECKSUM="${BREWFS_VERIFY_CACHE_CHECKSUM:-full}"
WRITEBACK_MODE="${BREWFS_WRITEBACK_MODE:-commit_before_upload}"
WRITEBACK_PERSIST_SYNC="${BREWFS_WRITEBACK_PERSIST_SYNC:-false}"
READ_MEMORY_BYTES="${BREWFS_READ_MEMORY_BYTES:-4294967296}"
READ_SSD_BYTES="${BREWFS_READ_SSD_BYTES:-4294967296}"
WRITE_MEMORY_BYTES="${BREWFS_WRITE_MEMORY_BYTES:-4294967296}"
WRITE_SSD_BYTES="${BREWFS_WRITE_SSD_BYTES:-4294967296}"
MEMORY_BUDGET_BYTES="${BREWFS_MEMORY_BUDGET_BYTES:-12884901888}"
DIRTY_SLICE_TARGET_SIZE="${BREWFS_DIRTY_SLICE_TARGET_SIZE:-67108864}"
DIRTY_SLICE_MAX_AGE_MS="${BREWFS_DIRTY_SLICE_MAX_AGE_MS:-2000}"
FUSE_WORKERS="${BREWFS_FUSE_WORKERS:-6}"
RANGE_BACKGROUND_PREFETCH="${BREWFS_RANGE_BACKGROUND_PREFETCH:-true}"
UPLOAD_CONCURRENCY="${BREWFS_UPLOAD_CONCURRENCY:-32}"
WRITEBACK_UPLOAD_CONCURRENCY="${BREWFS_WRITEBACK_UPLOAD_CONCURRENCY:-4}"
WRITEBACK_RECENT_PENDING_SOFT_BYTES="${BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES:-1073741824}"
WRITEBACK_RECENT_PENDING_HARD_BYTES="${BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES:-2147483648}"
PERF_RECORD_FREQ="${PERF_RECORD_FREQ:-49}"
PERF_EVENT="${PERF_EVENT:-task-clock}"
PERF_MMAP_PAGES="${PERF_MMAP_PAGES:-8}"
PERF_FIO_WORKLOADS="${PERF_FIO_WORKLOADS:-seqwrite seqread randwrite randread randrw}"
PERF_FIO_DIRECT="${PERF_FIO_DIRECT:-0}"
PERF_WARMUP_DRAIN="${PERF_WARMUP_DRAIN:-true}"
PERF_WARMUP_DRAIN_TIMEOUT_SECS="${PERF_WARMUP_DRAIN_TIMEOUT_SECS:-180}"
PERF_WARMUP_DRAIN_INTERVAL_SECS="${PERF_WARMUP_DRAIN_INTERVAL_SECS:-2}"
PERF_WARMUP_DRAIN_PENDING_BYTES="${PERF_WARMUP_DRAIN_PENDING_BYTES:-0}"
PERF_POST_FIO_DRAIN="${PERF_POST_FIO_DRAIN:-true}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-build) BUILD=0; shift ;;
        --quick) QUICK=1; shift ;;
        --no-cleanup) CLEANUP=0; shift ;;
        --skip-oncpu) SKIP_ONCPU=1; shift ;;
        --skip-offcpu) SKIP_OFFCPU=1; shift ;;
        --compress|--compression)
            COMPRESSION="${2:-lz4}"; shift 2 ;;
        *) echo "Unknown: $1"; exit 1 ;;
    esac
done

RUNTIME=$([ "$QUICK" -eq 1 ] && echo 15 || echo 60)
SEQ_SIZE=$([ "$QUICK" -eq 1 ] && echo 256m || echo 1g)
RAND_SIZE=$([ "$QUICK" -eq 1 ] && echo 128m || echo 512m)

BOLD='\033[1m'
GREEN='\033[32m'
YELLOW='\033[33m'
RED='\033[31m'
NC='\033[0m'

info()  { echo -e "${GREEN}[perf]${NC} ${BOLD}$*${NC}"; }
warn()  { echo -e "${YELLOW}[perf]${NC} $*"; }
err()   { echo -e "${RED}[perf]${NC} $*"; }

BREWFS_PID=""

cleanup() {
    if [ "$CLEANUP" -eq 1 ]; then
        info "cleaning up..."
        fusermount3 -u "$MNT_DIR" 2>/dev/null || true
        # Kill brewfs by saved PID (avoid pkill)
        if [[ -n "$BREWFS_PID" ]] && kill -0 "$BREWFS_PID" 2>/dev/null; then
            kill "$BREWFS_PID" 2>/dev/null || true
            wait "$BREWFS_PID" 2>/dev/null || true
        fi
        sleep 1
        docker compose -f "$SCRIPT_DIR/docker-compose.yml" down -v 2>/dev/null || true
        rm -rf "$WORK_DIR"
    fi
}
trap cleanup EXIT

check_cmd() {
    command -v "$1" >/dev/null 2>&1 || { err "$1 is required but not found"; exit 1; }
}
check_cmd docker
check_cmd cargo
check_cmd fio
check_cmd perf
check_cmd inferno-flamegraph
check_cmd python3

# ---- Build ----
if [ "$BUILD" -eq 1 ]; then
    info "building brewfs with profiling + frame pointers..."
    cd "$PROJECT_DIR"
    # Use DWARF4 to avoid addr2line compatibility issues with large binaries.
    # Split debuginfo keeps the binary smaller and perf can still find symbols.
    RUSTFLAGS="-C force-frame-pointers=yes -C debuginfo=2 -C split-debuginfo=off" \
        CARGO_PROFILE_RELEASE_DEBUG=2 \
        cargo build --release -p brewfs --features profiling 2>&1 | grep -E "error|warning|Finished" || true
    BINARY="${CARGO_TARGET_DIR:-$PROJECT_DIR/target}/release/brewfs"
else
    BINARY="${CARGO_TARGET_DIR:-$PROJECT_DIR/target}/release/brewfs"
    [ -x "$BINARY" ] || { err "binary not found: $BINARY"; exit 1; }
fi
[ -x "$BINARY" ] || { err "binary not found: $BINARY"; exit 1; }

# Verify the binary exists and has debug info
if [ -x "$BINARY" ]; then
    if ! file "$BINARY" | grep -q "with debug_info"; then
        warn "binary lacks debug info — flame graphs may have unresolved symbols"
    fi
    perf buildid-cache --add "$BINARY" >/dev/null 2>&1 || true
fi

# ---- Setup ----
info "setting up environment..."
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR" "$MNT_DIR" "$DATA_DIR" "$CACHE_DIR"
mkdir -p "$RUN_DIR/flame" "$RUN_DIR/fio" "$RUN_DIR/diagnostics"

FLAME_DIR="$RUN_DIR/flame"
FIO_DIR="$RUN_DIR/fio"
DIAG_DIR="$RUN_DIR/diagnostics"

# ---- Start infrastructure ----
info "starting redis + rustfs..."
cd "$SCRIPT_DIR"
docker compose down -v 2>/dev/null || true
REDIS_PORT="$REDIS_PORT" \
    RUSTFS_S3_PORT="$RUSTFS_S3_PORT" \
    RUSTFS_ACCESS_KEY="$RUSTFS_ACCESS_KEY" \
    RUSTFS_SECRET_KEY="$RUSTFS_SECRET_KEY" \
    S3_BUCKET="$S3_BUCKET" \
    docker compose up -d --wait redis rustfs 2>&1 | tail -5

REDIS_PORT="$REDIS_PORT" \
    RUSTFS_S3_PORT="$RUSTFS_S3_PORT" \
    RUSTFS_ACCESS_KEY="$RUSTFS_ACCESS_KEY" \
    RUSTFS_SECRET_KEY="$RUSTFS_SECRET_KEY" \
    S3_BUCKET="$S3_BUCKET" \
    docker compose run --rm rustfs-init >/dev/null

docker compose ps --format 'table {{.Service}}\t{{.Status}}' 2>/dev/null || true
if command -v redis-cli >/dev/null 2>&1; then
    redis-cli -p "$REDIS_PORT" ping >/dev/null 2>&1 || { err "redis not reachable"; exit 1; }
else
    docker compose exec -T redis redis-cli ping >/dev/null 2>&1 || {
        err "redis not reachable"
        exit 1
    }
fi
info "redis: OK, rustfs: OK"

# ---- Generate config ----
cat > "$CONFIG_PATH" << YEOF
mount_point: $MNT_DIR
data:
  backend: s3
  s3:
    bucket: $S3_BUCKET
    endpoint: http://127.0.0.1:$RUSTFS_S3_PORT
    region: us-east-1
    force_path_style: true
    disable_payload_checksum: true
    part_size: 16777216
    max_concurrency: 16
meta:
  backend: redis
  redis:
    url: "redis://127.0.0.1:$REDIS_PORT/0"
  open_file_cache_ttl_ms: 1000
  open_file_cache_capacity: 65536
layout:
  chunk_size: 67108864
  block_size: 4194304
fuse:
  workers: $FUSE_WORKERS
  max_background: 512
cache:
  root: $CACHE_DIR
  read_memory_bytes: $READ_MEMORY_BYTES
  read_ssd_bytes: $READ_SSD_BYTES
  write_memory_bytes: $WRITE_MEMORY_BYTES
  write_ssd_bytes: $WRITE_SSD_BYTES
  memory_budget_bytes: $MEMORY_BUDGET_BYTES
  dirty_slice_target_size: $DIRTY_SLICE_TARGET_SIZE
  dirty_slice_max_age_ms: $DIRTY_SLICE_MAX_AGE_MS
  upload_concurrency: $UPLOAD_CONCURRENCY
  range_background_prefetch: $RANGE_BACKGROUND_PREFETCH
  compression: $COMPRESSION
  verify_cache_checksum: $VERIFY_CACHE_CHECKSUM
  writeback_persist_sync: $WRITEBACK_PERSIST_SYNC
  writeback_mode: $WRITEBACK_MODE
YEOF

# Save a copy of the config to results
cp "$CONFIG_PATH" "$RUN_DIR/config.yaml"
info "config: compression=$COMPRESSION verify_cache_checksum=$VERIFY_CACHE_CHECKSUM writeback_mode=$WRITEBACK_MODE upload_concurrency=$UPLOAD_CONCURRENCY dirty_slice_target_size=$DIRTY_SLICE_TARGET_SIZE writeback_upload_concurrency=$WRITEBACK_UPLOAD_CONCURRENCY pending_soft=$WRITEBACK_RECENT_PENDING_SOFT_BYTES pending_hard=$WRITEBACK_RECENT_PENDING_HARD_BYTES range_background_prefetch=$RANGE_BACKGROUND_PREFETCH"

# ---- Mount ----
info "mounting brewfs..."
fusermount3 -u "$MNT_DIR" 2>/dev/null || true
sleep 1

AWS_ACCESS_KEY_ID="$RUSTFS_ACCESS_KEY" \
    AWS_SECRET_ACCESS_KEY="$RUSTFS_SECRET_KEY" \
    AWS_DEFAULT_REGION=us-east-1 \
    AWS_EC2_METADATA_DISABLED=true \
    BREWFS_WRITEBACK_UPLOAD_CONCURRENCY="$WRITEBACK_UPLOAD_CONCURRENCY" \
    BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES="$WRITEBACK_RECENT_PENDING_SOFT_BYTES" \
    BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES="$WRITEBACK_RECENT_PENDING_HARD_BYTES" \
    RUST_LOG="${RUST_LOG:-error}" \
    "$BINARY" mount --privileged --config "$CONFIG_PATH" >"$RUN_DIR/brewfs-mount.log" 2>&1 &
BREWFS_PID=$!

for i in $(seq 1 15); do
    mount | grep -q " on $MNT_DIR " && break
    sleep 1
done
mount | grep -q " on $MNT_DIR " || { err "mount failed"; exit 1; }
info "mounted at $MNT_DIR"

truthy() {
    case "${1:-}" in
        1|true|TRUE|yes|YES|on|ON) return 0 ;;
        *) return 1 ;;
    esac
}

stats_snapshot() {
    local label="$1"
    local stats_path="$MNT_DIR/.stats"
    if [ -e "$stats_path" ]; then
        tr -d '\000' <"$stats_path" >"$DIAG_DIR/stats-${label}.txt" 2>/dev/null || true
    fi
}

stat_value() {
    local metric="$1"
    local stats_path="$MNT_DIR/.stats"
    local value
    [ -e "$stats_path" ] || { printf '0'; return 0; }
    value="$(tr -d '\000' <"$stats_path" 2>/dev/null | awk -v metric="$metric" '$1 == metric { print $2; found=1; exit } END { if (!found) print "0" }')"
    case "$value" in
        ''|*[!0-9]*) printf '0' ;;
        *) printf '%s' "$value" ;;
    esac
}

max_u64() {
    if [ "$1" -ge "$2" ]; then
        printf '%s' "$1"
    else
        printf '%s' "$2"
    fi
}

wait_for_writeback_drain() {
    local label="$1"
    local timeout="$2"
    local interval="$3"
    local threshold="$4"
    local start now elapsed pending dirty buffer_dirty drain_bytes

    start="$(date +%s)"
    while true; do
        pending="$(stat_value brewfs_writeback_recent_pending_upload_bytes)"
        dirty="$(stat_value brewfs_writeback_dirty_bytes)"
        buffer_dirty="$(stat_value brewfs_buffer_dirty_bytes)"
        drain_bytes="$(max_u64 "$pending" "$dirty")"
        drain_bytes="$(max_u64 "$drain_bytes" "$buffer_dirty")"
        now="$(date +%s)"
        elapsed="$((now - start))"

        if [ "$drain_bytes" -le "$threshold" ]; then
            info "writeback drained after $label: pending=$pending dirty=$dirty buffer_dirty=$buffer_dirty elapsed=${elapsed}s"
            stats_snapshot "${label}-drained"
            return 0
        fi

        if [ "$elapsed" -ge "$timeout" ]; then
            warn "writeback drain timed out after $label: pending=$pending dirty=$dirty buffer_dirty=$buffer_dirty elapsed=${elapsed}s"
            stats_snapshot "${label}-drain-timeout"
            return 1
        fi

        if [ $((elapsed % 10)) -eq 0 ]; then
            info "  waiting for writeback drain after $label: pending=$pending dirty=$dirty buffer_dirty=$buffer_dirty elapsed=${elapsed}s"
        fi
        sleep "$interval"
    done
}

# ---- Warmup ----
info "warming up filesystem..."
fio --name=warmup --directory="$MNT_DIR" --rw=write --bs=4m --size=256m \
    --numjobs=1 --ioengine=sync --direct=0 \
    --group_reporting --eta=never --output-format=terse 2>/dev/null || true
rm -rf "$MNT_DIR"/* 2>/dev/null || true
stats_snapshot "warmup-after-rm"
if truthy "$PERF_WARMUP_DRAIN"; then
    wait_for_writeback_drain "warmup" "$PERF_WARMUP_DRAIN_TIMEOUT_SECS" "$PERF_WARMUP_DRAIN_INTERVAL_SECS" "$PERF_WARMUP_DRAIN_PENDING_BYTES" || true
fi

# ---- Helper: run fio and print summary ----
run_fio() {
    local label="$1"; shift
    info "  fio $label..."
    local tmp_json="$FIO_DIR/fio-${label}.json.tmp"
    local tmp_err="$FIO_DIR/fio-${label}.err.tmp"
    local status=0
    fio "$@" --directory="$MNT_DIR" --runtime="$RUNTIME" --time_based \
        --group_reporting --eta=never --output-format=json >"$tmp_json" 2>"$tmp_err" \
        || status=$?
    python3 -c "
import json,sys
s=sys.stdin.read()
start=s.find('{')
if start < 0:
    raise SystemExit(0)
d=json.loads(s[start:])
for j in d.get('jobs',[]):
    if j.get('error', 0):
        print(f\"    error: fio job failed with code {j.get('error')}\")
    for op in ('read','write'):
        bw=j.get(op,{}).get('bw_bytes',0)
        if bw>0:
            iops=j[op]['iops']
            lat=j[op]['lat_ns']['mean']/1e6
            print(f'    {op}: {bw/1024/1024:.0f} MiB/s, iops={iops:.1f}, lat_avg={lat:.2f}ms')
" <"$tmp_json" 2>/dev/null || true
    if [ "$status" -ne 0 ]; then
        warn "  fio $label exited with status $status"
        if [ -s "$tmp_err" ]; then
            sed 's/^/    /' "$tmp_err" | tail -20
        fi
    fi
    stats_snapshot "fio-${label}-after"
    mv "$tmp_json" "$FIO_DIR/fio-${label}.json" 2>/dev/null || true
    mv "$tmp_err" "$FIO_DIR/fio-${label}.err" 2>/dev/null || true
    if truthy "$PERF_POST_FIO_DRAIN"; then
        case "$label" in
            seqwrite|randwrite|randrw)
                wait_for_writeback_drain "fio-$label" "$PERF_WARMUP_DRAIN_TIMEOUT_SECS" "$PERF_WARMUP_DRAIN_INTERVAL_SECS" "$PERF_WARMUP_DRAIN_PENDING_BYTES" || true
                ;;
        esac
    fi
}

run_fio_suite() {
    local workload
    for workload in $PERF_FIO_WORKLOADS; do
        case "$workload" in
            seqwrite)
                run_fio "seqwrite" --name=seqwrite --rw=write --bs=4m --size="$SEQ_SIZE" --numjobs=1 --ioengine=sync --direct="$PERF_FIO_DIRECT"
                ;;
            seqread)
                run_fio "seqread" --name=seqread --rw=read --bs=4m --size="$SEQ_SIZE" --numjobs=1 --ioengine=sync --direct="$PERF_FIO_DIRECT"
                ;;
            randwrite)
                run_fio "randwrite" --name=randwrite --rw=randwrite --bs=4m --size="$RAND_SIZE" --numjobs=4 --ioengine=sync --direct="$PERF_FIO_DIRECT"
                ;;
            randread)
                run_fio "randread" --name=randread --rw=randread --bs=4m --size="$RAND_SIZE" --numjobs=4 --ioengine=sync --direct="$PERF_FIO_DIRECT"
                ;;
            randrw)
                run_fio "randrw" --name=randrw --rw=randrw --rwmixread=70 --bs=4m --size="$RAND_SIZE" --numjobs=4 --ioengine=sync --direct="$PERF_FIO_DIRECT"
                ;;
            *)
                err "unknown PERF_FIO_WORKLOADS entry: $workload"
                exit 1
                ;;
        esac
    done
}

# ---- Helper: run perf script safely ----
# Handles addr2line issues with large debug binaries.
run_perf_script() {
    local perf_data="$1"
    local output="$2"

    # Use --symfs / to let perf find the binary at its original path
    if perf script -i "$perf_data" --symfs / > "$output" 2>/dev/null; then
        return 0
    fi

    # Fallback: try --no-inline (avoids addr2line "could not read first record")
    if perf script --no-inline -i "$perf_data" --symfs / > "$output" 2>/dev/null; then
        return 0
    fi

    # Last resort: only emit basic fields (no addr2line at all)
    warn "perf script failed to resolve symbols; using basic output"
    perf script -i "$perf_data" -F comm,pid,tid,cpu,time,event,ip,sym,dso 2>/dev/null > "$output" || true
}

find_libc_path() {
    local path
    for path in /usr/lib/x86_64-linux-gnu/libc.so.6 /lib/x86_64-linux-gnu/libc.so.6; do
        if [ -f "$path" ]; then
            echo "$path"
            return 0
        fi
    done
    if command -v ldconfig >/dev/null 2>&1; then
        ldconfig -p 2>/dev/null | awk '/libc\.so\.6/ { print $NF; exit }'
    fi
}

libc_debug_file() {
    local libc_path="$1"
    local build_id=""
    if [ -n "$libc_path" ] && command -v readelf >/dev/null 2>&1; then
        build_id="$(readelf -n "$libc_path" 2>/dev/null | awk '/Build ID:/ { print $3; exit }')"
    fi
    if [ -n "$build_id" ]; then
        echo "/usr/lib/debug/.build-id/${build_id:0:2}/${build_id:2}.debug"
    fi
}

check_libc_debuginfo() {
    local libc_path
    local debug_file
    libc_path="$(find_libc_path || true)"
    debug_file="$(libc_debug_file "$libc_path" || true)"

    if [ -n "$debug_file" ] && [ -f "$debug_file" ]; then
        info "  libc debuginfo: $debug_file"
    else
        warn "  libc debuginfo not found; libc frames may stay as [libc.so.6]/0xaddr"
        warn "  install libc6-dbg or set DEBUGINFOD_URLS=https://debuginfod.ubuntu.com before running perf"
    fi
}

generate_libc_report() {
    local perf_data="$1"
    local output="$2"
    local err_file="${output}.err"

    perf report -i "$perf_data" --stdio --dsos libc.so.6 \
        --sort dso,symbol --percent-limit 0.5 \
        > "$output" 2> "$err_file" || true

    if [ -s "$err_file" ]; then
        {
            echo ""
            echo "---- perf report stderr ----"
            cat "$err_file"
        } >> "$output"
    fi
    rm -f "$err_file"
}

# =========================================================================
# ON-CPU FLAME GRAPH
# =========================================================================
if [ "$SKIP_ONCPU" -eq 0 ]; then
    info "=== ON-CPU profiling (perf record -e $PERF_EVENT -F $PERF_RECORD_FREQ -m $PERF_MMAP_PAGES --call-graph fp) ==="

    # Use frame-pointer based call graphs (fp) — faster and more reliable
    # than dwarf for large binaries. Requires -C force-frame-pointers=yes at build.
    perf record -e "$PERF_EVENT" -F "$PERF_RECORD_FREQ" -m "$PERF_MMAP_PAGES" \
        --call-graph fp -p "$BREWFS_PID" -o "$FLAME_DIR/oncpu-perf.data" &
    PERF_ONCPU_PID=$!
    sleep 1

    run_fio_suite

    kill -INT "$PERF_ONCPU_PID" 2>/dev/null || true
    wait "$PERF_ONCPU_PID" 2>/dev/null || true

    if [ -f "$FLAME_DIR/oncpu-perf.data" ]; then
        info "generating on-CPU flame graph..."
        check_libc_debuginfo
        run_perf_script "$FLAME_DIR/oncpu-perf.data" "$FLAME_DIR/oncpu-raw.txt"

        inferno-collapse-perf --addrs < "$FLAME_DIR/oncpu-raw.txt" \
            > "$FLAME_DIR/oncpu.folded" 2>/dev/null || true

        grep "brewfs" "$FLAME_DIR/oncpu.folded" > "$FLAME_DIR/oncpu-brewfs.folded" 2>/dev/null || true

        if [ -s "$FLAME_DIR/oncpu-brewfs.folded" ]; then
            inferno-flamegraph "$FLAME_DIR/oncpu-brewfs.folded" \
                > "$FLAME_DIR/oncpu-flame.svg"
            info "  on-CPU flame graph: $FLAME_DIR/oncpu-flame.svg"

            # Hotspot analysis
            info "  analyzing hotspots..."
            python3 "$SCRIPT_DIR/analyze_flame.py" --hotspots "$FLAME_DIR/oncpu-brewfs.folded" 2>/dev/null || true
        else
            warn "  no brewfs samples captured — is the workload too short?"
        fi

        info "  generating libc symbol report..."
        generate_libc_report "$FLAME_DIR/oncpu-perf.data" "$FLAME_DIR/libc-report.txt"
        info "  libc report: $FLAME_DIR/libc-report.txt"
        if [[ "${KEEP_PERF_DATA:-0}" != "1" ]]; then
            rm -f "$FLAME_DIR/oncpu-perf.data"
        fi

        # Clean up intermediate file
        rm -f "$FLAME_DIR/oncpu-raw.txt"
    fi
elif [ "$SKIP_OFFCPU" -eq 1 ]; then
    info "=== fio benchmark (perf disabled) ==="
    run_fio_suite
fi

# =========================================================================
# OFF-CPU FLAME GRAPH
# =========================================================================
if [ "$SKIP_OFFCPU" -eq 0 ]; then
    info "=== OFF-CPU profiling (sched:sched_switch) ==="

    rm -rf "$MNT_DIR"/* 2>/dev/null || true

    # Use fp call graph for off-cpu too — dwarf on large binaries causes
    # "could not read first record" errors with addr2line.
    perf record -e 'sched:sched_switch' --call-graph fp -a -o "$FLAME_DIR/offcpu-perf.data" &
    PERF_OFFCPU_PID=$!
    sleep 1

    info "  fio seqwrite (off-CPU)..."
    fio --name=offcpu-seqwrite --directory="$MNT_DIR" --rw=write --bs=4m \
        --size="$SEQ_SIZE" --numjobs=1 --ioengine=sync --direct="$PERF_FIO_DIRECT" \
        --runtime="$RUNTIME" --time_based --group_reporting --eta=never \
        --output-format=terse 2>/dev/null || true

    info "  fio seqread (off-CPU)..."
    fio --name=offcpu-seqread --directory="$MNT_DIR" --rw=read --bs=4m \
        --size="$SEQ_SIZE" --numjobs=1 --ioengine=sync --direct="$PERF_FIO_DIRECT" \
        --runtime="$RUNTIME" --time_based --group_reporting --eta=never \
        --output-format=terse 2>/dev/null || true

    kill -INT "$PERF_OFFCPU_PID" 2>/dev/null || true
    wait "$PERF_OFFCPU_PID" 2>/dev/null || true

    if [ -f "$FLAME_DIR/offcpu-perf.data" ]; then
        info "generating off-CPU flame graph..."
        python3 "$SCRIPT_DIR/analyze_flame.py" --offcpu \
            "$FLAME_DIR/offcpu-perf.data" "$FLAME_DIR/offcpu-brewfs.folded" 2>/dev/null || true

        if [ -s "$FLAME_DIR/offcpu-brewfs.folded" ]; then
            inferno-flamegraph --title "BrewFS Off-CPU" \
                "$FLAME_DIR/offcpu-brewfs.folded" \
                > "$FLAME_DIR/offcpu-flame.svg"
            info "  off-CPU flame graph: $FLAME_DIR/offcpu-flame.svg"
        else
            warn "  no off-CPU brewfs samples captured"
        fi
        if [[ "${KEEP_PERF_DATA:-0}" != "1" ]]; then
            rm -f "$FLAME_DIR/offcpu-perf.data"
        fi
    fi
fi

# =========================================================================
# Crypto overhead analysis
# =========================================================================
if [ -f "$FLAME_DIR/oncpu-brewfs.folded" ]; then
    info "=== crypto overhead ==="
    python3 "$SCRIPT_DIR/analyze_flame.py" --crypto "$FLAME_DIR/oncpu-brewfs.folded" 2>/dev/null || true
fi

# =========================================================================
# LLM-readable report
# =========================================================================
info "=== LLM-readable report ==="
HOTSPOTS_ARG=""
if [ -f "$FLAME_DIR/oncpu-brewfs.folded" ]; then
    HOTSPOTS_ARG="--hotspots $FLAME_DIR/oncpu-brewfs.folded"
fi

# The analyze_perf.py expects a "results" dir with fio*.json — symlink from our fio dir
ln -sfn "$FIO_DIR" "$RUN_DIR/results"

python3 "$SCRIPT_DIR/analyze_perf.py" --llm --bottleneck $HOTSPOTS_ARG "$RUN_DIR" \
    > "$RUN_DIR/llm-report.txt" 2>/dev/null || true
info "  LLM report: $RUN_DIR/llm-report.txt"
cat "$RUN_DIR/llm-report.txt" 2>/dev/null || true

# Generate markdown report too
python3 "$SCRIPT_DIR/analyze_perf.py" --bottleneck $HOTSPOTS_ARG "$RUN_DIR" \
    -o "$RUN_DIR/report.md" 2>/dev/null || true

# =========================================================================
# Summary
# =========================================================================
info "=============================================="
info "results saved to: $RUN_DIR"
ls -lh "$FLAME_DIR"/*.svg 2>/dev/null || true
info "=============================================="
echo ""
echo "  open flame graphs:"
for svg in "$FLAME_DIR"/*.svg; do
    [ -f "$svg" ] || continue
    echo "    file://$svg"
done
echo ""
echo "  reports:"
echo "    $RUN_DIR/llm-report.txt"
echo "    $RUN_DIR/report.md"
echo ""
echo "  fio results:"
ls "$FIO_DIR"/*.json 2>/dev/null || true

# Create a 'latest' symlink for convenience
ln -sfn "$RUN_TS" "$RESULTS_BASE/latest"
info "symlink: $RESULTS_BASE/latest -> $RUN_TS"
