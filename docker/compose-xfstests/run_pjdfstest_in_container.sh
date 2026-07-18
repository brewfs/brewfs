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
pjdfstest_extra_args="${PJDFSTEST_EXTRA_ARGS:-}"
pjdfstest_skip_patterns="${PJDFSTEST_SKIP_PATTERNS:-}"
pjdfstest_skip_patterns_file="${PJDFSTEST_SKIP_PATTERNS_FILE:-}"
pjdfstest_default_skip_file="${PJDFSTEST_DEFAULT_SKIP_FILE:-/usr/local/share/brewfs/pjdfstest_skip_tests.txt}"
tmp_dir=""

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
                cat <<YAML
data:
  backend: local-fs
  localfs:
    data_dir: ${data_dir}
YAML
                ;;
            s3)
                bucket="${BREWFS_S3_BUCKET:-brewfs-data}"
                region="${BREWFS_S3_REGION:-us-east-1}"
                endpoint="${BREWFS_S3_ENDPOINT:-http://rustfs:9000}"
                force_path="${BREWFS_S3_FORCE_PATH_STYLE:-true}"
                part_size="${BREWFS_S3_PART_SIZE:-16777216}"
                max_conc="${BREWFS_S3_MAX_CONCURRENCY:-8}"
                cat <<YAML
data:
  backend: s3
  s3:
    bucket: ${bucket}
    region: ${region}
    part_size: ${part_size}
    max_concurrency: ${max_conc}
    force_path_style: ${force_path}
    endpoint: ${endpoint}
YAML
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
                cat <<YAML
meta:
  backend: sqlx
  sqlx:
    url: "$url"
YAML
                ;;
            redis)
                if [[ -z "$meta_url" ]]; then
                    err "BREWFS_META_URL must not be empty (redis)"
                    exit 1
                fi
                cat <<YAML
meta:
  backend: redis
  redis:
    url: "$meta_url"
YAML
                ;;
            etcd)
                cat <<YAML
meta:
  backend: etcd
  etcd:
    urls:
YAML
                local old_ifs="$IFS"
                IFS=','
                for url in $meta_etcd_urls; do
                    echo "      - \"${url}\""
                done
                IFS="$old_ifs"
                ;;
            tikv)
                cat <<YAML
meta:
  backend: tikv
  tikv:
    pd_endpoints:
YAML
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
        cat <<YAML
layout:
  chunk_size: ${BREWFS_CHUNK_SIZE:-67108864}
  block_size: ${BREWFS_BLOCK_SIZE:-4194304}
YAML
        if [[ -n "${BREWFS_CACHE_ROOT:-}" \
            || -n "${BREWFS_READ_MEMORY_BYTES:-}" \
            || -n "${BREWFS_READ_SSD_BYTES:-}" \
            || -n "${BREWFS_WRITE_MEMORY_BYTES:-}" \
            || -n "${BREWFS_WRITE_SSD_BYTES:-}" \
            || -n "${BREWFS_DIRTY_SLICE_TARGET_SIZE:-}" \
            || -n "${BREWFS_DIRTY_SLICE_MAX_AGE_MS:-}" \
            || -n "${BREWFS_UPLOAD_CONCURRENCY:-}" \
            || -n "${BREWFS_POPULATE_WRITE_CACHE_AFTER_UPLOAD:-}" \
            || -n "${BREWFS_PERSIST_WRITE_CACHE_AFTER_UPLOAD:-}" \
            || -n "${BREWFS_WRITEBACK_MODE:-}" \
            || -n "${BREWFS_WRITEBACK_PERSIST_SYNC:-}" \
            || -n "${BREWFS_WRITEBACK_REQUIRE_STAGE_BEFORE_COMMIT:-}" \
            || -n "${BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES:-}" \
            || -n "${BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES:-}" \
            || -n "${BREWFS_MEMORY_BUDGET_BYTES:-}" \
            || -n "${BREWFS_VERIFY_CACHE_CHECKSUM:-}" ]]; then
            echo
            echo "cache:"
            [[ -n "${BREWFS_CACHE_ROOT:-}" ]] && echo "  root: ${BREWFS_CACHE_ROOT}"
            [[ -n "${BREWFS_READ_MEMORY_BYTES:-}" ]] && echo "  read_memory_bytes: ${BREWFS_READ_MEMORY_BYTES}"
            [[ -n "${BREWFS_READ_SSD_BYTES:-}" ]] && echo "  read_ssd_bytes: ${BREWFS_READ_SSD_BYTES}"
            [[ -n "${BREWFS_WRITE_MEMORY_BYTES:-}" ]] && echo "  write_memory_bytes: ${BREWFS_WRITE_MEMORY_BYTES}"
            [[ -n "${BREWFS_WRITE_SSD_BYTES:-}" ]] && echo "  write_ssd_bytes: ${BREWFS_WRITE_SSD_BYTES}"
            [[ -n "${BREWFS_DIRTY_SLICE_TARGET_SIZE:-}" ]] && echo "  dirty_slice_target_size: ${BREWFS_DIRTY_SLICE_TARGET_SIZE}"
            [[ -n "${BREWFS_DIRTY_SLICE_MAX_AGE_MS:-}" ]] && echo "  dirty_slice_max_age_ms: ${BREWFS_DIRTY_SLICE_MAX_AGE_MS}"
            [[ -n "${BREWFS_UPLOAD_CONCURRENCY:-}" ]] && echo "  upload_concurrency: ${BREWFS_UPLOAD_CONCURRENCY}"
            [[ -n "${BREWFS_POPULATE_WRITE_CACHE_AFTER_UPLOAD:-}" ]] && echo "  populate_write_cache_after_upload: ${BREWFS_POPULATE_WRITE_CACHE_AFTER_UPLOAD}"
            [[ -n "${BREWFS_PERSIST_WRITE_CACHE_AFTER_UPLOAD:-}" ]] && echo "  persist_write_cache_after_upload: ${BREWFS_PERSIST_WRITE_CACHE_AFTER_UPLOAD}"
            [[ -n "${BREWFS_WRITEBACK_MODE:-}" ]] && echo "  writeback_mode: ${BREWFS_WRITEBACK_MODE}"
            [[ -n "${BREWFS_WRITEBACK_PERSIST_SYNC:-}" ]] && echo "  writeback_persist_sync: ${BREWFS_WRITEBACK_PERSIST_SYNC}"
            [[ -n "${BREWFS_WRITEBACK_REQUIRE_STAGE_BEFORE_COMMIT:-}" ]] && echo "  writeback_require_stage_before_commit: ${BREWFS_WRITEBACK_REQUIRE_STAGE_BEFORE_COMMIT}"
            [[ -n "${BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES:-}" ]] && echo "  writeback_recent_pending_soft_bytes: ${BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES}"
            [[ -n "${BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES:-}" ]] && echo "  writeback_recent_pending_hard_bytes: ${BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES}"
            [[ -n "${BREWFS_MEMORY_BUDGET_BYTES:-}" ]] && echo "  memory_budget_bytes: ${BREWFS_MEMORY_BUDGET_BYTES}"
            [[ -n "${BREWFS_VERIFY_CACHE_CHECKSUM:-}" ]] && echo "  verify_cache_checksum: ${BREWFS_VERIFY_CACHE_CHECKSUM}"
        fi
    } >"$config_path"
    return 0
}

install_mount_helper() {
    local helper="/usr/sbin/mount.fuse.brewfs"
    local baked_log_file="${log_file:-/artifacts/brewfs.log}"
    local baked_fuse_log_file="${fuse_log_file:-}"

    cat >"$helper" <<SCRIPTEOF
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

SCRIPTEOF

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
    append_env_export BREWFS_CACHE_TTL_MS "0"
    append_env_export RUST_LOG

    if [[ -n "$baked_fuse_log_file" ]]; then
        cat >>"$helper" <<SCRIPTEOF
mkdir -p "\$(dirname "${baked_fuse_log_file}")"
BREWFS_FUSE_OP_LOG=1 BREWFS_FUSE_LOG_FILE="${baked_fuse_log_file}" \\
    /usr/local/bin/brewfs mount --privileged --config "\$config_path" "\$target" >>"\$log_file" 2>&1 &
brewfs_pid=\$!
SCRIPTEOF
    else
        cat >>"$helper" <<'SCRIPTEOF'
/usr/local/bin/brewfs mount --privileged --config "$config_path" "$target" >>"$log_file" 2>&1 &
brewfs_pid=$!
SCRIPTEOF
    fi

    cat >>"$helper" <<'SCRIPTEOF'
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
SCRIPTEOF
    chmod +x "$helper"
}

mount_brewfs() {
    info "mount BrewFS: $mount_dir"
    mkdir -p "$mount_dir"

    local max_wait="${BREWFS_MOUNT_WAIT_SECS:-60}"
    mount -t fuse.brewfs brewfs "$mount_dir"

    local waited=0
    while [[ $waited -lt $max_wait ]]; do
        if mount | grep -q " on $mount_dir type fuse"; then
            ok "BrewFS mounted at $mount_dir"
            return 0
        fi
        sleep 0.5
        waited=$((waited + 1))
    done
    err "BrewFS mount did not appear at $mount_dir after ${max_wait}s"
    return 1
}

prepare_results_dir() {
    mkdir -p "$artifact_dir/results" "$artifact_dir/output"
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

trim_skip_file() {
    local source="$1"
    local dest="$2"
    awk '
        /^[[:space:]]*(#|$)/ { next }
        { print }
    ' "$source" >>"$dest"
}

prepare_test_list() {
    local raw_skipfile="$tmp_dir/skip-patterns.raw"
    local skipfile="$tmp_dir/skip-patterns.txt"
    local all_tests="$tmp_dir/tests.all"
    local selected_tests="$tmp_dir/tests.selected"
    local skip_source

    : >"$raw_skipfile"
    if [[ -f "$pjdfstest_default_skip_file" ]]; then
        trim_skip_file "$pjdfstest_default_skip_file" "$raw_skipfile"
    fi
    if [[ -n "$pjdfstest_skip_patterns_file" ]]; then
        if [[ ! -f "$pjdfstest_skip_patterns_file" ]]; then
            err "custom pjdfstest skip file not found: $pjdfstest_skip_patterns_file"
            exit 1
        fi
        trim_skip_file "$pjdfstest_skip_patterns_file" "$raw_skipfile"
    fi
    if [[ -n "$pjdfstest_skip_patterns" ]]; then
        for skip_source in $pjdfstest_skip_patterns; do
            printf '%s\n' "$skip_source" >>"$raw_skipfile"
        done
    fi
    sort -u "$raw_skipfile" >"$skipfile"

    cd "$pjdfstest_dir"
    find tests -type f -name '*.t' | sort >"$all_tests"
    if [[ -s "$skipfile" ]]; then
        grep -Ev -f "$skipfile" "$all_tests" >"$selected_tests"
    else
        cp "$all_tests" "$selected_tests"
    fi

    cp -f "$all_tests" "$artifact_dir/all-tests.txt"
    cp -f "$selected_tests" "$artifact_dir/selected-tests.txt"
    cp -f "$skipfile" "$artifact_dir/skip-patterns.txt"
    printf '%s' "$selected_tests"
}

write_report() {
    local status="$1"
    local selected_count="$2"
    local failed_count="$3"
    local skipped_count="$4"

    {
        echo "artifact_dir: $artifact_dir"
        echo "generated_at: $(date --iso-8601=seconds)"
        echo "status: $status"
        echo "selected_count: $selected_count"
        echo "failed_count: $failed_count"
        echo "skipped_count: $skipped_count"
    } >"$artifact_dir/summary.txt"

    {
        echo "# BrewFS pjdfstest report"
        echo
        echo "- artifact_dir: $artifact_dir"
        echo "- generated_at: $(date --iso-8601=seconds)"
        echo "- status: $status"
        echo "- selected_count: $selected_count"
        echo "- failed_count: $failed_count"
        echo "- skipped_count: $skipped_count"
        echo
        echo "## key files"
        echo
        echo "- pjdfstest.console.log"
        echo "- selected-tests.txt"
        echo "- skip-patterns.txt"
        echo "- failed-tests.txt"
        echo "- brewfs.log"
        echo "- backend.yml"
    } >"$artifact_dir/report.md"
}

run_pjdfstest() {
    local selected_tests_file
    local test_workdir="$mount_dir/.pjdfstest-work"
    local selected_tests_abs="$tmp_dir/tests.selected.abs"
    local selected_count
    local all_count
    local skipped_count
    local failed_count=0
    local -a prove_args=(-v)

    tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/brewfs-pjdfstest.XXXXXX")"
    selected_tests_file="$(prepare_test_list)"
    selected_count="$(wc -l <"$selected_tests_file" | tr -d '[:space:]')"
    all_count="$(wc -l <"$artifact_dir/all-tests.txt" | tr -d '[:space:]')"
    skipped_count=$((all_count - selected_count))

    if [[ -n "$pjdfstest_extra_args" ]]; then
        read -r -a extra <<<"$pjdfstest_extra_args"
        prove_args+=("${extra[@]}")
    fi

    mkdir -p "$test_workdir"
    sed "s#^#$pjdfstest_dir/#" "$selected_tests_file" >"$selected_tests_abs"
    info "pjdfstest selected tests: $selected_count, skipped: $skipped_count"
    info "pjdfstest workdir: $test_workdir"
    info "pjdfstest args: ${prove_args[*]}"

    export PATH="$pjdfstest_dir:$PATH"
    cd "$test_workdir"

    set +e
    xargs -r prove "${prove_args[@]}" <"$selected_tests_abs" 2>&1 | tee "$artifact_dir/pjdfstest.console.log"
    status="${PIPESTATUS[0]}"
    set -e

    # Ignore prove's successful summary rows such as "Failed: 0".  They can
    # accompany TODO-passed diagnostics and are not failed pjdfstest cases.
    grep -E '^[^[:space:]].*\.t .*Failed: [1-9][0-9]*|Result: FAIL|Failed [1-9][0-9]*/[0-9]+ subtests' "$artifact_dir/pjdfstest.console.log" \
        >"$artifact_dir/failed-tests.txt" 2>/dev/null || true
    failed_count="$(wc -l <"$artifact_dir/failed-tests.txt" | tr -d '[:space:]')"

    if [[ "$status" -eq 0 ]]; then
        write_report "PASS" "$selected_count" "$failed_count" "$skipped_count"
        ok "pjdfstest PASS"
    else
        write_report "FAIL" "$selected_count" "$failed_count" "$skipped_count"
        err "pjdfstest FAIL (exit=$status)"
    fi
    ok "artifacts: $artifact_dir"
    return "$status"
}

cleanup() {
    if [[ -n "$tmp_dir" && -d "$tmp_dir" ]]; then
        rm -rf "$tmp_dir" >/dev/null 2>&1 || true
    fi
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

    if [[ -z "$artifact_dir" ]]; then
        ts="$(date +%s)-$RANDOM"
        artifact_dir="${artifact_root%/}/pjdfstest-run-${ts}"
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

    info "prepare results dir"
    prepare_results_dir

    mount_brewfs

    set +e
    run_pjdfstest
    status=$?
    set -e

    copy_artifacts || true
    exit "$status"
}

main "$@"
