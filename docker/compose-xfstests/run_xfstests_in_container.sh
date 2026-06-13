#!/usr/bin/env bash

set -euo pipefail

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
info() { log "INFO  $*"; }
ok()   { log "OK    $*"; }
err()  { log "ERROR $*" >&2; }

config_path="${BREWFS_CONFIG_PATH:-/run/brewfs/config.yaml}"
mount_dir="${BREWFS_MOUNT_POINT:-/mnt/brewfs}"
data_backend="${BREWFS_DATA_BACKEND:-local-fs}"
data_dir="${BREWFS_DATA_DIR:-${BREWFS_HOME:-/var/lib/brewfs}/data}"
meta_backend="${BREWFS_META_BACKEND:-redis}"
meta_url="${BREWFS_META_URL:-}"
meta_etcd_urls="${BREWFS_META_ETCD_URLS:-http://etcd:2379}"
meta_tikv_pd_endpoints="${BREWFS_META_TIKV_PD_ENDPOINTS:-pd:2379}"
meta_tikv_namespace="${BREWFS_META_TIKV_NAMESPACE:-brewfs}"
sqlite_path="${BREWFS_SQLITE_PATH:-${BREWFS_HOME:-/var/lib/brewfs}/metadata.db}"
log_file="${BREWFS_LOG_FILE:-/artifacts/brewfs.log}"
xfstests_dir="${XFSTESTS_DIR:-/opt/xfstests-dev}"
artifact_root="${BREWFS_ARTIFACT_ROOT:-/artifacts}"
artifact_dir="${BREWFS_ARTIFACT_DIR:-}"

xfstests_cases="${XFSTESTS_CASES:-}"
xfstests_skip_cases="${XFSTESTS_SKIP_CASES:-0}"
xfstests_check_args="${XFSTESTS_CHECK_ARGS:-}"

require_non_negative_integer() {
    local option="$1"
    local value="${2:-}"
    if ! [[ "$value" =~ ^[0-9]+$ ]]; then
        err "$option 需要提供非负整数，当前值: ${value:-<empty>}"
        exit 1
    fi
}

build_full_run_check_args() {
    local -a all_cases=()
    local -a filtered_cases=()
    local -a discovery_args=(-n -fuse -E xfstests_slayer.exclude)
    local skip_count=0
    local i=0
    local list_output=""

    require_non_negative_integer "XFSTESTS_SKIP_CASES" "$xfstests_skip_cases"
    skip_count="$xfstests_skip_cases"
    if (( skip_count == 0 )); then
        err "build_full_run_check_args 仅用于 skip-cases > 0 的场景"
        exit 1
    fi

    info "解析默认全量测试序列，并跳过前 $skip_count 个用例" >&2
    list_output="$(
        cd "$xfstests_dir"
        export PATH="$xfstests_dir:$PATH"
        ./check "${discovery_args[@]}"
    )"

    mapfile -t all_cases < <(
        printf '%s\n' "$list_output" \
            | awk '/^[[:alnum:]_-]+\/[0-9]+([[:space:]]|$)/ && $2 != "[expunged]" { print $1 }'
    )

    if [[ "${#all_cases[@]}" -eq 0 ]]; then
        err "无法解析默认全量测试序列"
        exit 1
    fi

    if (( skip_count >= ${#all_cases[@]} )); then
        err "XFSTESTS_SKIP_CASES=$skip_count 超过默认全量用例数 (${#all_cases[@]})"
        exit 1
    fi

    for ((i = skip_count; i < ${#all_cases[@]}; i++)); do
        filtered_cases+=("${all_cases[i]}")
    done

    info "默认全量测试共 ${#all_cases[@]} 个，用例跳过后剩余 ${#filtered_cases[@]} 个" >&2
    printf '%s\n' "${filtered_cases[@]}"
}

write_config() {
    mkdir -p "$(dirname "$config_path")" "$mount_dir"
    if [[ "$data_backend" == "local-fs" ]]; then
        mkdir -p "$data_dir"
    fi

    {
        echo "mount_point: $mount_dir"
        echo
        case "$data_backend" in
            local-fs)
                cat <<EOF
data:
  backend: local-fs
  localfs:
    data_dir: ${data_dir}
EOF
                ;;
            s3)
                bucket="${BREWFS_S3_BUCKET:-brewfs-data}"
                region="${BREWFS_S3_REGION:-us-east-1}"
                endpoint="${BREWFS_S3_ENDPOINT:-http://rustfs:9000}"
                force_path="${BREWFS_S3_FORCE_PATH_STYLE:-true}"
                part_size="${BREWFS_S3_PART_SIZE:-16777216}"
                max_conc="${BREWFS_S3_MAX_CONCURRENCY:-8}"
                cat <<EOF
data:
  backend: s3
  s3:
    bucket: ${bucket}
    region: ${region}
    part_size: ${part_size}
    max_concurrency: ${max_conc}
    force_path_style: ${force_path}
    endpoint: ${endpoint}
EOF
                ;;
            *)
                err "不支持的 BREWFS_DATA_BACKEND: $data_backend"
                exit 1
                ;;
        esac
        echo

        case "$meta_backend" in
            sqlite)
                mkdir -p "$(dirname "$sqlite_path")"
                local url="${meta_url:-sqlite://${sqlite_path}?mode=rwc}"
                cat <<EOF
meta:
  backend: sqlx
  sqlx:
    url: "$url"
EOF
                ;;
            redis)
                if [[ -z "$meta_url" ]]; then
                    err "BREWFS_META_URL 不能为空 (redis)"
                    exit 1
                fi
                cat <<EOF
meta:
  backend: redis
  redis:
    url: "$meta_url"
EOF
                ;;
            etcd)
                cat <<EOF
meta:
  backend: etcd
  etcd:
    urls:
EOF
                local old_ifs="$IFS"
                IFS=','
                for url in $meta_etcd_urls; do
                    echo "      - \"${url}\""
                done
                IFS="$old_ifs"
                ;;
            tikv)
                cat <<EOF
meta:
  backend: tikv
  tikv:
    pd_endpoints:
EOF
                local old_ifs="$IFS"
                IFS=','
                for endpoint in $meta_tikv_pd_endpoints; do
                    echo "      - \"${endpoint}\""
                done
                IFS="$old_ifs"
                echo "    namespace: \"${meta_tikv_namespace}\""
                ;;
            *)
                err "不支持的 BREWFS_META_BACKEND: $meta_backend"
                exit 1
                ;;
        esac

        echo
        cat <<EOF
layout:
  chunk_size: ${BREWFS_CHUNK_SIZE:-67108864}
  block_size: ${BREWFS_BLOCK_SIZE:-4194304}
EOF
    } >"$config_path"
}

install_mount_helper() {
    local helper="/usr/sbin/mount.fuse.brewfs"
    # 把运行时确定的路径直接硬写进 helper，避免 mount 调用时环境变量被清除
    local baked_log_file="${log_file:-/artifacts/brewfs.log}"
    local baked_fuse_log_file="${fuse_log_file:-}"

    cat >"$helper" <<EOF
#!/usr/bin/env bash
set -euo pipefail

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:\$PATH"

src="\${1:-}"
target="\${2:-}"
shift 2 || true

config_path="\${BREWFS_CONFIG_PATH:-/run/brewfs/config.yaml}"
log_file="${baked_log_file}"

mkdir -p "\$target" "\$(dirname "\$log_file")"

is_brewfs_mounted() {
    findmnt -rn --target "\$target" --output FSTYPE 2>/dev/null | grep -Eq '^fuse(\\.|$)'
}

pre_wait_secs="\${BREWFS_PRE_MOUNT_WAIT_SECS:-10}"
pre_deadline=\$((SECONDS + pre_wait_secs))
while is_brewfs_mounted; do
    if (( SECONDS >= pre_deadline )); then
        echo "target \$target is still mounted before starting BrewFS after \${pre_wait_secs}s" >&2
        exit 1
    fi
    sleep 0.1
done

EOF

    append_env_export() {
        local name="$1"
        local default="${2-}"
        local value="${!name:-$default}"
        if [[ -n "$value" || $# -ge 2 ]]; then
            printf 'export %s=%q\n' "$name" "$value" >>"$helper"
        fi
    }
    append_env_export AWS_ACCESS_KEY_ID "rustfsadmin"
    append_env_export AWS_SECRET_ACCESS_KEY "rustfsadmin"
    append_env_export AWS_DEFAULT_REGION "us-east-1"
    append_env_export AWS_EC2_METADATA_DISABLED "true"
    append_env_export AWS_SESSION_TOKEN
    append_env_export BREWFS_MOUNT_WAIT_SECS "60"
    append_env_export BREWFS_PRE_MOUNT_WAIT_SECS "10"
    append_env_export BREWFS_MOUNT_READY_DELAY_SECS "1"
    append_env_export RUST_LOG

    if [[ -n "$baked_fuse_log_file" ]]; then
        cat >>"$helper" <<EOF
mkdir -p "\$(dirname "${baked_fuse_log_file}")"
BREWFS_FUSE_OP_LOG=1 BREWFS_FUSE_LOG_FILE="${baked_fuse_log_file}" \\
    /usr/local/bin/brewfs mount --privileged --config "\$config_path" "\$target" >>"\$log_file" 2>&1 &
brewfs_pid=\$!
EOF
    else
        cat >>"$helper" <<'EOF'
/usr/local/bin/brewfs mount --privileged --config "$config_path" "$target" >>"$log_file" 2>&1 &
brewfs_pid=$!
EOF
    fi

    cat >>"$helper" <<'EOF'
wait_secs="${BREWFS_MOUNT_WAIT_SECS:-60}"
deadline=$((SECONDS + wait_secs))
while (( SECONDS < deadline )); do
    if is_brewfs_mounted; then
        sleep "${BREWFS_MOUNT_READY_DELAY_SECS:-1}"
        exit 0
    fi
    if ! kill -0 "$brewfs_pid" 2>/dev/null; then
        status=0
        wait "$brewfs_pid" || status=$?
        if (( status == 0 )); then
            status=1
        fi
        echo "brewfs mount process exited before $target became a mountpoint (status=$status)" >&2
        exit "$status"
    fi
    sleep 0.1
done

echo "timed out after ${wait_secs}s waiting for BrewFS mount at $target" >&2
echo "brewfs_pid=$brewfs_pid running=$(kill -0 "$brewfs_pid" 2>/dev/null && echo yes || echo no)" >&2
echo "findmnt: $(findmnt -rn --target "$target" --output TARGET,FSTYPE,SOURCE 2>/dev/null || true)" >&2
ps -o pid,ppid,stat,etime,comm,args -p "$brewfs_pid" >&2 || true
exit 1
EOF
    chmod +x "$helper"
}

write_local_config() {
    cat >"$xfstests_dir/local.config" <<EOF
export TEST_DEV=brewfs
export TEST_DIR=$mount_dir
export FSTYP=fuse
export FUSE_SUBTYP=.brewfs
export DF_PROG="df -T -P -a"
EOF
}

prepare_results_dir() {
    mkdir -p "$artifact_dir/results" "$artifact_dir/diagnostics" "$xfstests_dir/results"
    touch "$artifact_dir/results/check.log" "$artifact_dir/check.console.log" >/dev/null 2>&1 || true
}

redis_diag_enabled() {
    [[ "$meta_backend" == "redis" ]] || return 1
    [[ -n "$meta_url" ]] || return 1
    command -v redis-cli >/dev/null 2>&1 || return 1
}

redis_diag_cli() {
    redis-cli -u "$meta_url" "$@"
}

redis_diag_before_xfstests() {
    redis_diag_enabled || return 0
    {
        echo "# Redis diagnostic reset before xfstests"
        date -Iseconds
        redis_diag_cli CONFIG SET latency-monitor-threshold "${XFSTESTS_REDIS_LATENCY_THRESHOLD_MS:-1}" || true
        redis_diag_cli CONFIG RESETSTAT || true
        redis_diag_cli SLOWLOG RESET || true
        redis_diag_cli LATENCY RESET || true
    } >"$artifact_dir/diagnostics/redis-xfstests-before.txt" 2>&1 || true
}

redis_diag_after_xfstests() {
    redis_diag_enabled || return 0
    {
        echo "# Redis diagnostics after xfstests"
        date -Iseconds
        echo
        echo "## INFO commandstats"
        redis_diag_cli INFO commandstats || true
        echo
        echo "## INFO stats"
        redis_diag_cli INFO stats || true
        echo
        echo "## SLOWLOG GET 20"
        redis_diag_cli SLOWLOG GET 20 || true
        echo
        echo "## LATENCY LATEST"
        redis_diag_cli LATENCY LATEST || true
        echo
        echo "## LATENCY DOCTOR"
        redis_diag_cli LATENCY DOCTOR || true
    } >"$artifact_dir/diagnostics/redis-xfstests-after.txt" 2>&1 || true
}

stats_snapshot_after_xfstests() {
    local stats_path="$mount_dir/.stats"
    {
        date -Iseconds
        echo
        if [[ -e "$stats_path" ]]; then
            tr -d '\000' <"$stats_path"
        else
            echo "missing $stats_path"
        fi
    } >"$artifact_dir/diagnostics/stats-xfstests-after.txt" 2>&1 || true
}

copy_artifacts() {
    mkdir -p "$artifact_dir"
    if [[ -f "$log_file" && "$log_file" != "$artifact_dir/brewfs.log" ]]; then
        cp -f "$log_file" "$artifact_dir/brewfs.log" || true
    fi
    if [[ -n "${BREWFS_FUSE_LOG_FILE:-}" && -f "${BREWFS_FUSE_LOG_FILE}" && "${BREWFS_FUSE_LOG_FILE}" != "$artifact_dir/brewfs_fuse_ops.log" ]]; then
        cp -f "${BREWFS_FUSE_LOG_FILE}" "$artifact_dir/brewfs_fuse_ops.log" || true
    fi
    if [[ -f "$config_path" ]]; then
        cp -f "$config_path" "$artifact_dir/backend.yml" || true
    fi
    if [[ -f "$xfstests_dir/local.config" ]]; then
        cp -f "$xfstests_dir/local.config" "$artifact_dir/local.config" || true
    fi
    if [[ -d "$xfstests_dir/results" ]]; then
        mkdir -p "$artifact_dir/results"
        cp -a "$xfstests_dir/results/." "$artifact_dir/results/" 2>/dev/null || true
    fi

    chmod -R a+rwX "$artifact_dir" >/dev/null 2>&1 || true
}

cleanup() {
    while findmnt -rn --target "$mount_dir" --output FSTYPE 2>/dev/null | grep -Eq '^fuse(\.|$)'; do
        fusermount3 -u "$mount_dir" >/dev/null 2>&1 \
            || umount -f "$mount_dir" >/dev/null 2>&1 \
            || umount -l "$mount_dir" >/dev/null 2>&1 \
            || sleep 1
    done
    pkill -f "/usr/local/bin/brewfs mount" >/dev/null 2>&1 || true
}

on_exit() {
    local status=$?
    copy_artifacts || true
    if [[ -x /usr/local/bin/xfstests_report.sh ]]; then
        bash /usr/local/bin/xfstests_report.sh "$artifact_dir" --no-tar >/dev/null 2>&1 || true
    fi
    cleanup || true
    trap - EXIT
    exit "$status"
}

run_xfstests() {
    local -a check_args=()
    local -a selected_cases=()
    require_non_negative_integer "XFSTESTS_SKIP_CASES" "$xfstests_skip_cases"

    if [[ "$xfstests_skip_cases" != "0" && -n "$xfstests_cases" ]]; then
        err "XFSTESTS_SKIP_CASES 不能与 XFSTESTS_CASES 同时使用"
        exit 1
    fi

    if [[ "$xfstests_skip_cases" != "0" && -n "$xfstests_check_args" ]]; then
        err "XFSTESTS_SKIP_CASES 不能与 XFSTESTS_CHECK_ARGS 同时使用"
        exit 1
    fi

    if [[ -n "$xfstests_check_args" ]]; then
        read -r -a check_args <<<"$xfstests_check_args"
    elif [[ -n "$xfstests_cases" ]]; then
        read -r -a selected_cases <<<"$xfstests_cases"
        check_args=(-fuse -E xfstests_slayer.exclude "${selected_cases[@]}")
    elif [[ "$xfstests_skip_cases" != "0" ]]; then
        mapfile -t selected_cases < <(build_full_run_check_args)
        check_args=(-fuse -E xfstests_slayer.exclude --exact-order "${selected_cases[@]}")
    else
        check_args=(-fuse -E xfstests_slayer.exclude)
    fi

    (
        cd "$xfstests_dir"
        export PATH="$xfstests_dir:$PATH"
        ./check "${check_args[@]}" 2>&1 | tee -a "$artifact_dir/check.console.log" "$artifact_dir/results/check.log"
        exit "${PIPESTATUS[0]}"
    )
}

main() {
    local normalized_fuse_op_log

    if [[ -z "$artifact_dir" ]]; then
        ts="$(date +%s)-$RANDOM"
        artifact_dir="${artifact_root%/}/run-${ts}"
    fi
    mkdir -p "$artifact_dir"
    chmod a+rwx "$artifact_dir" >/dev/null 2>&1 || true
    log_file="$artifact_dir/brewfs.log"
    export BREWFS_LOG_FILE="$log_file"
    normalized_fuse_op_log="${BREWFS_FUSE_OP_LOG:-0}"
    normalized_fuse_op_log="${normalized_fuse_op_log,,}"
    if [[ "$normalized_fuse_op_log" =~ ^(1|true|yes|on)$ ]]; then
        fuse_log_file="$artifact_dir/brewfs_fuse_ops.log"
        export BREWFS_FUSE_LOG_FILE="$fuse_log_file"
    else
        fuse_log_file=""
        unset BREWFS_FUSE_LOG_FILE || true
    fi

    trap on_exit EXIT INT TERM

    info "写入 BrewFS 配置: $config_path"
    write_config

    info "安装 mount helper: /usr/sbin/mount.fuse.brewfs"
    install_mount_helper

    info "写入 xfstests local.config: $xfstests_dir/local.config"
    write_local_config

    info "将 xfstests results/ 指向产物目录（便于实时观察 check.log）"
    prepare_results_dir

    info "运行 xfstests (FUSE): dir=$xfstests_dir mount=$mount_dir"
    redis_diag_before_xfstests
    set +e
    run_xfstests
    status=$?
    set -e
    stats_snapshot_after_xfstests
    redis_diag_after_xfstests

    if [[ -f "$artifact_dir/check.console.log" ]]; then
        cp -f "$artifact_dir/check.console.log" "$artifact_dir/xfstests-script.log" >/dev/null 2>&1 || true
        mkdir -p "$artifact_dir/results"
        cp -f "$artifact_dir/check.console.log" "$artifact_dir/results/check.out" >/dev/null 2>&1 || true
    fi

    copy_artifacts || true
    if [[ -x /usr/local/bin/xfstests_report.sh ]]; then
        bash /usr/local/bin/xfstests_report.sh "$artifact_dir" --no-tar >/dev/null 2>&1 || true
    fi

    if [[ "$status" -eq 0 ]]; then
        ok "xfstests PASS"
    else
        err "xfstests FAIL (exit=$status)"
    fi
    ok "artifacts: $artifact_dir"
    exit "$status"
}

main "$@"
