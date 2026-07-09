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
sqlite_path="${BREWFS_SQLITE_PATH:-${BREWFS_HOME:-/var/lib/brewfs}/metadata.db}"
log_file="${BREWFS_LOG_FILE:-/artifacts/brewfs.log}"
ltp_dir="${LTP_DIR:-/opt/ltp}"
artifact_root="${BREWFS_ARTIFACT_ROOT:-/artifacts}"
artifact_dir="${BREWFS_ARTIFACT_DIR:-}"

ltp_scenarios="${LTP_SCENARIOS:-fs}"
ltp_extra_args="${LTP_EXTRA_ARGS:-}"
ltp_skip_files="${LTP_SKIP_FILES:-}"
ltp_skip_tests="${LTP_SKIP_TESTS:-}"
ltp_skip_tests_file="${LTP_SKIP_TESTS_FILE:-}"
ltp_default_skip_tests_file="${LTP_DEFAULT_SKIP_TESTS_FILE:-/usr/local/share/brewfs/ltp_skip_tests.txt}"
ltp_tmp_dir=""

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
    } >"$config_path"
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

prepare_results_dir() {
    mkdir -p "$artifact_dir/results" "$artifact_dir/output"
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

    if [[ -d "$ltp_dir/results" ]]; then
        cp -a "$ltp_dir/results/." "$artifact_dir/results/" 2>/dev/null || true
    fi
    if [[ -d "$ltp_dir/output" ]]; then
        cp -a "$ltp_dir/output/." "$artifact_dir/output/" 2>/dev/null || true
    fi

    chmod -R a+rwX "$artifact_dir" >/dev/null 2>&1 || true
}

trim_whitespace() {
    local value="$1"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    printf '%s' "$value"
}

resolve_ltp_cmdfile() {
    local cmdfile="$1"

    if [[ -f "$cmdfile" ]]; then
        realpath "$cmdfile"
        return 0
    fi

    if [[ -f "$ltp_dir/runtest/$cmdfile" ]]; then
        realpath "$ltp_dir/runtest/$cmdfile"
        return 0
    fi

    err "unable to resolve LTP command file: $cmdfile"
    exit 1
}

prepare_ltp_skiplist() {
    local merged_skipfile="$ltp_tmp_dir/skip-tests.raw"
    local normalized_skipfile="$ltp_tmp_dir/skip-tests.txt"
    local skip_source

    : >"$merged_skipfile"

    if [[ -f "$ltp_default_skip_tests_file" ]]; then
        cat "$ltp_default_skip_tests_file" >>"$merged_skipfile"
        printf '\n' >>"$merged_skipfile"
    fi

    if [[ -n "$ltp_skip_tests_file" ]]; then
        if [[ ! -f "$ltp_skip_tests_file" ]]; then
            err "custom LTP skip file not found: $ltp_skip_tests_file"
            exit 1
        fi
        cat "$ltp_skip_tests_file" >>"$merged_skipfile"
        printf '\n' >>"$merged_skipfile"
    fi

    if [[ -n "$ltp_skip_tests" ]]; then
        for skip_source in $ltp_skip_tests; do
            printf '%s\n' "$skip_source" >>"$merged_skipfile"
        done
    fi

    awk '
        /^[[:space:]]*(#|$)/ { next }
        { print $1 }
    ' "$merged_skipfile" | sort -u >"$normalized_skipfile"

    printf '%s' "$normalized_skipfile"
}

prepare_ltp_cmdfiles() {
    local skipfile="$1"
    local scenario_list="$2"
    local scenario
    local source_cmdfile
    local filtered_cmdfile
    local skipped_count
    local -a selected_scenarios=()
    local -a filtered_cmdfiles=()

    IFS=',' read -r -a selected_scenarios <<<"$scenario_list"

    for scenario in "${selected_scenarios[@]}"; do
        scenario="$(trim_whitespace "$scenario")"
        [[ -z "$scenario" ]] && continue

        source_cmdfile="$(resolve_ltp_cmdfile "$scenario")"
        filtered_cmdfile="$ltp_tmp_dir/$(basename "$source_cmdfile").filtered"

        awk -v skipfile="$skipfile" '
            BEGIN {
                while ((getline line < skipfile) > 0) {
                    if (line ~ /^[[:space:]]*(#|$)/) {
                        continue
                    }
                    split(line, fields)
                    skip[fields[1]] = 1
                }
                close(skipfile)
            }
            /^[[:space:]]*#/ || NF == 0 {
                print
                next
            }
            !($1 in skip) {
                print
            }
        ' "$source_cmdfile" >"$filtered_cmdfile"

        filtered_cmdfiles+=("$filtered_cmdfile")
    done

    if [[ "${#filtered_cmdfiles[@]}" -eq 0 ]]; then
        err "no LTP command files selected"
        exit 1
    fi

    skipped_count="$(wc -l <"$skipfile" | tr -d '[:space:]')"
    info "prepared ${#filtered_cmdfiles[@]} LTP command file(s); skip list entries: ${skipped_count:-0}" >&2

    IFS=',' printf '%s' "${filtered_cmdfiles[*]}"
}

cleanup() {
    if [[ -n "$ltp_tmp_dir" && -d "$ltp_tmp_dir" ]]; then
        rm -rf "$ltp_tmp_dir" >/dev/null 2>&1 || true
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
    if [[ -x /usr/local/bin/ltp_report.sh ]]; then
        bash /usr/local/bin/ltp_report.sh "$artifact_dir" --no-tar >/dev/null 2>&1 || true
    fi
    cleanup || true
    trap - EXIT
    exit "$status"
}

run_ltp() {
    local skipfile
    local filtered_cmdfiles
    local -a ltp_args

    if [[ -n "$ltp_skip_files" ]]; then
        info "LTP_SKIP_FILES is deprecated and ignored; use LTP_SKIP_TESTS or LTP_SKIP_TESTS_FILE"
    fi

    ltp_tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/brewfs-ltp.XXXXXX")"
    skipfile="$(prepare_ltp_skiplist)"
    filtered_cmdfiles="$(prepare_ltp_cmdfiles "$skipfile" "$ltp_scenarios")"
    ltp_args=(-f "$filtered_cmdfiles" -d "$mount_dir" -q)

    if [[ -n "$ltp_extra_args" ]]; then
        read -r -a extra <<<"$ltp_extra_args"
        ltp_args+=("${extra[@]}")
    fi

    info "LTP scenarios: $ltp_scenarios, mount: $mount_dir"
    info "LTP args: ${ltp_args[*]}"

    export LTP_DEV="$mount_dir"
    export LTP_DEV_FS_TYPE="fuse"

    cd "$ltp_dir"
    set +e
    ./runltp "${ltp_args[@]}" 2>&1 | tee -a "$artifact_dir/ltp.console.log"
    status="${PIPESTATUS[0]}"
    set -e

    if [[ -f "$artifact_dir/ltp.console.log" ]]; then
        cp -f "$artifact_dir/ltp.console.log" "$artifact_dir/results/ltp.console.log" >/dev/null 2>&1 || true
    fi

    return "$status"
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

    info "write BrewFS config: $config_path"
    write_config

    info "install mount helper: /usr/sbin/mount.fuse.brewfs"
    install_mount_helper

    info "prepare results dir"
    prepare_results_dir

    mount_brewfs

    info "run LTP ($ltp_scenarios): mount=$mount_dir"
    set +e
    run_ltp
    status=$?
    set -e

    copy_artifacts || true
    if [[ -x /usr/local/bin/ltp_report.sh ]]; then
        bash /usr/local/bin/ltp_report.sh "$artifact_dir" --no-tar >/dev/null 2>&1 || true
    fi

    if [[ "$status" -eq 0 ]]; then
        ok "LTP PASS"
    else
        err "LTP FAIL (exit=$status)"
    fi
    ok "artifacts: $artifact_dir"
    exit "$status"
}

main "$@"
