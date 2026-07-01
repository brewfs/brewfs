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
artifact_root="${BREWFS_ARTIFACT_ROOT:-/artifacts}"
artifact_dir="${BREWFS_ARTIFACT_DIR:-}"
pjdfstest_dir="${PJDFSTEST_DIR:-/opt/pjdfstest}"
pjdfstest_tests="${PJDFSTEST_TESTS:-}"
pjdfstest_prove_args="${PJDFSTEST_PROVE_ARGS:-}"
fuse_log_file=""

require_tool() {
    local tool="$1"
    if ! command -v "$tool" >/dev/null 2>&1; then
        err "required tool not found: $tool"
        exit 1
    fi
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
                err "unsupported BREWFS_DATA_BACKEND: $data_backend"
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
                    err "BREWFS_META_URL must not be empty (redis)"
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
                err "unsupported BREWFS_META_BACKEND: $meta_backend"
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
    append_env_export BREWFS_CONFIG_PATH "$config_path"
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

prepare_artifacts() {
    mkdir -p "$artifact_dir/results" "$artifact_dir/diagnostics"
    touch "$artifact_dir/pjdfstest.console.log" "$artifact_dir/results/pjdfstest.log" >/dev/null 2>&1 || true
}

mount_brewfs() {
    info "mount BrewFS: $mount_dir"
    mkdir -p "$mount_dir"
    mount -t fuse.brewfs brewfs "$mount_dir"
    ok "BrewFS mounted at $mount_dir"
}

resolve_test_paths() {
    local -a paths=()
    local -a selected_tests=()
    local test=""

    if [[ ! -d "$pjdfstest_dir/tests" ]]; then
        err "pjdfstest tests directory not found: $pjdfstest_dir/tests"
        exit 1
    fi

    if [[ -z "$pjdfstest_tests" ]]; then
        printf '%s\n' "$pjdfstest_dir/tests"
        return 0
    fi

    read -r -a selected_tests <<<"$pjdfstest_tests"
    for test in "${selected_tests[@]}"; do
        if [[ -e "$pjdfstest_dir/tests/$test" ]]; then
            paths+=("$pjdfstest_dir/tests/$test")
        elif [[ -e "$pjdfstest_dir/$test" ]]; then
            paths+=("$pjdfstest_dir/$test")
        elif [[ -e "$test" ]]; then
            paths+=("$test")
        else
            err "pjdfstest test not found: $test"
            err "expected one of: $pjdfstest_dir/tests/$test, $pjdfstest_dir/$test, $test"
            exit 1
        fi
    done

    printf '%s\n' "${paths[@]}"
}

run_pjdfstest() {
    local -a prove_args=(-r)
    local -a extra_args=()
    local -a test_paths=()

    if [[ -n "$pjdfstest_prove_args" ]]; then
        read -r -a extra_args <<<"$pjdfstest_prove_args"
    fi

    mapfile -t test_paths < <(resolve_test_paths)

    (
        cd "$mount_dir"
        export PATH="$pjdfstest_dir:$PATH"
        prove "${prove_args[@]}" "${extra_args[@]}" "${test_paths[@]}" 2>&1 \
            | tee -a "$artifact_dir/pjdfstest.console.log" "$artifact_dir/results/pjdfstest.log"
        exit "${PIPESTATUS[0]}"
    )
}

stats_snapshot_after_pjdfstest() {
    local stats_path="$mount_dir/.stats"
    {
        date -Iseconds
        echo
        if [[ -e "$stats_path" ]]; then
            tr -d '\000' <"$stats_path"
        else
            echo "missing $stats_path"
        fi
    } >"$artifact_dir/diagnostics/stats-pjdfstest-after.txt" 2>&1 || true
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
    cleanup || true
    trap - EXIT
    exit "$status"
}

main() {
    local normalized_fuse_op_log
    local status

    require_tool findmnt
    require_tool fusermount3
    require_tool mount
    require_tool prove

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

    info "write BrewFS config: $config_path"
    write_config

    info "install mount helper: /usr/sbin/mount.fuse.brewfs"
    install_mount_helper

    info "prepare artifacts: $artifact_dir"
    prepare_artifacts

    mount_brewfs

    info "run pjdfstest: dir=$pjdfstest_dir mount=$mount_dir tests=${pjdfstest_tests:-<all>}"
    set +e
    run_pjdfstest
    status=$?
    set -e
    stats_snapshot_after_pjdfstest
    copy_artifacts || true

    if [[ "$status" -eq 0 ]]; then
        ok "pjdfstest PASS"
    else
        err "pjdfstest FAIL (exit=$status)"
    fi
    ok "artifacts: $artifact_dir"
    exit "$status"
}

main "$@"
