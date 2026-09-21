#!/usr/bin/env bash
# Prepare an Ubuntu 24.04 ECS instance as a native BrewFS/JuiceFS performance VM.
#
# Aliyun OSS and Redis/Tair are managed services in the supported flow, so the
# prepared VM contains no Docker, no Redis server and no RustFS binary. Once
# this script reaches READY, a run only needs a BrewFS binary dropped at
# $SOURCE/target/release/brewfs: the native runner installs it into
# /usr/local/bin/brewfs before the harness starts.
#
# Usage: prepare_native_perf_vm.sh [payload-dir]
#
# The payload directory (default /opt/brewfs-perf-image-prep) supplies the
# harness revisions overlaid on the cloned BrewFS checkout:
#   harness/run_perf_in_container.sh
#   harness/run_juicefs_perf_in_container.sh
#   harness/perf_metadata_fallback.py
#   harness/perf_manifest.py
#   harness/run_native_perf.sh
#
# The script is idempotent and is expected to run detached from the cloud
# assistant, because installing packages can restart that agent.
set -Eeuo pipefail

export DEBIAN_FRONTEND=noninteractive

PAYLOAD_DIR="${1:-/opt/brewfs-perf-image-prep}"

# Optional environment file shipped with the payload (dependency URLs and
# version overrides). It is sourced before the defaults below are applied.
# shellcheck source=/dev/null
[[ -f "$PAYLOAD_DIR/setup/prepare.env" ]] && . "$PAYLOAD_DIR/setup/prepare.env"

ROOT="${BREWFS_PERF_ROOT:-/opt/brewfs-perf}"
SOURCE="$ROOT/source/brewfs"
NATIVE_DIR="$ROOT/native"
REPO="${BREWFS_PERF_REPO:-https://github.com/brewfs/brewfs.git}"
REF="${BREWFS_PERF_REF:-main}"
JUICEFS_VERSION="${JUICEFS_VERSION:-1.4.1}"
STATUS_FILE="$ROOT/.prepare-status"

step() { printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*"; }
fail() {
    step "ERROR $*"
    mkdir -p "$ROOT"
    printf 'FAILED %s\n' "$*" >"$STATUS_FILE"
    exit 1
}

# Without a trap an unexpected non-zero exit (killed download, failed apt
# transaction) would leave the status file saying RUNNING forever.
on_error() {
    local rc=$?
    if [[ $rc -ne 0 ]]; then
        fail "unexpected failure at line ${1:-?} (rc=$rc)"
    fi
}
trap 'on_error "$LINENO"' ERR

harness_sha() { sha256sum "$1" | awk '{print $1}'; }

[[ "$(id -u)" -eq 0 ]] || fail 'run this script as root'
mkdir -p "$ROOT"
printf 'RUNNING\n' >"$STATUS_FILE"

step 'repairing any interrupted dpkg transaction'
dpkg --configure -a || true
apt-get -f install -y -qq || true
rm -f /var/lib/dpkg/lock-frontend /var/lib/dpkg/lock /var/cache/apt/archives/lock

step 'installing VM dependencies'
apt-get update -qq
apt-get install --no-upgrade -y -qq \
    git curl zip jq ca-certificates unzip tar gzip xz-utils \
    bash build-essential protobuf-compiler util-linux e2fsprogs fuse3 libfuse3-3 \
    xfsprogs fio stress-ng redis-tools \
    acl attr bc dbench dump gawk liburing2 libuuid1 lvm2 make perl psmisc \
    python3 quota sed strace sudo uuid-runtime xfsdump exfatprogs f2fs-tools udftools \
    procps

step 'enabling unprivileged FUSE mounts'
mkdir -p /etc/fuse /etc/modules-load.d
grep -q '^user_allow_other$' /etc/fuse.conf 2>/dev/null || echo 'user_allow_other' >>/etc/fuse.conf
echo fuse >/etc/modules-load.d/brewfs-perf-fuse.conf
modprobe fuse || true

step 'installing AWS CLI v2'
if command -v aws >/dev/null 2>&1 && aws --version 2>&1 | grep -q 'aws-cli/2'; then
    echo "  $(aws --version 2>&1 | head -n 1) already installed"
else
    rm -rf /tmp/aws /tmp/awscliv2.zip
    curl --fail --location --retry 5 --retry-all-errors \
        https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip --output /tmp/awscliv2.zip
    unzip -q /tmp/awscliv2.zip -d /tmp
    /tmp/aws/install --bin-dir /usr/local/bin --install-dir /usr/local/aws-cli --update
fi
command -v aws >/dev/null 2>&1 || fail 'AWS CLI installation failed'
rm -rf /tmp/aws /tmp/awscliv2.zip

step "installing JuiceFS $JUICEFS_VERSION"
if [[ -x /usr/local/bin/juicefs ]] && \
    /usr/local/bin/juicefs version 2>/dev/null | head -n 1 | grep -q "$JUICEFS_VERSION"; then
    echo "  $(/usr/local/bin/juicefs version 2>/dev/null | head -n 1) already installed"
else
    # The GitHub release CDN is frequently rate limited from cn-hangzhou (tens
    # of KB/s). JuiceFS publishes the same artifact on its own CDN, so try that
    # first and keep proxy mirrors as fallbacks.
    juicefs_urls="${JUICEFS_DOWNLOAD_URLS:-}"
    if [[ -z "$juicefs_urls" ]]; then
        juicefs_asset="juicefs-${JUICEFS_VERSION}-linux-amd64.tar.gz"
        juicefs_urls="https://d.juicefs.com/juicefs/releases/download/v${JUICEFS_VERSION}/${juicefs_asset}"
        juicefs_urls+=" https://gh-proxy.com/https://github.com/juicedata/juicefs/releases/download/v${JUICEFS_VERSION}/${juicefs_asset}"
        juicefs_urls+=" https://ghfast.top/https://github.com/juicedata/juicefs/releases/download/v${JUICEFS_VERSION}/${juicefs_asset}"
    fi
    juicefs_downloaded=0
    for url in $juicefs_urls; do
        echo "  downloading $url"
        rm -f /tmp/juicefs.tar.gz
        if curl --fail --location --retry 2 --retry-all-errors --connect-timeout 10 --max-time 900 \
            "$url" --output /tmp/juicefs.tar.gz && tar -tzf /tmp/juicefs.tar.gz >/dev/null 2>&1; then
            juicefs_downloaded=1
            break
        fi
        echo "  download failed or archive is invalid, trying the next mirror"
    done
    [[ "$juicefs_downloaded" == 1 ]] || fail 'unable to download the JuiceFS release archive'
    tar -xzf /tmp/juicefs.tar.gz -C /tmp
    install -m 0755 /tmp/juicefs /usr/local/bin/juicefs
fi
rm -f /tmp/juicefs.tar.gz /tmp/juicefs
/usr/local/bin/juicefs version | head -n 1

step "cloning BrewFS ($REF)"
git config --global http.version HTTP/1.1
git config --global http.lowSpeedLimit 0
git config --global http.lowSpeedTime 300
clone_urls="${BREWFS_PERF_CLONE_URLS:-}"
if [[ -z "$clone_urls" ]]; then
    clone_urls="$REPO"
    if [[ "$REPO" == https://github.com/* ]]; then
        clone_urls+=" https://gh-proxy.com/${REPO}"
        clone_urls+=" https://ghfast.top/${REPO}"
    fi
fi
cloned=0
for clone_url in $clone_urls; do
    for attempt in $(seq 1 3); do
        echo "  clone attempt ${attempt}/3 from $clone_url"
        rm -rf "$SOURCE"
        mkdir -p "$(dirname "$SOURCE")"
        if git clone --depth=1 --branch "$REF" "$clone_url" "$SOURCE"; then
            cloned=1
            break 2
        fi
        sleep $((attempt * 10))
    done
done
[[ "$cloned" == 1 ]] || fail "unable to clone $REPO ($REF)"
git config --global --add safe.directory "$SOURCE"
BASE_COMMIT="$(git -C "$SOURCE" rev-parse HEAD)"

if [[ "${BREWFS_PERF_INSTALL_IO_PAGES_KERNEL:-0}" == 1 ]]; then
    kernel_installer="$SOURCE/docker/compose-xfstests/aliyun/install_fuse_io_pages_kernel.sh"
    [[ -x "$kernel_installer" ]] || fail "FUSE io_pages installer is missing: $kernel_installer"
    "$kernel_installer"
fi

step 'extracting prebuilt xfstests'
XFSTESTS_ARCHIVE="$SOURCE/tests/scripts/xfstests-prebuilt/xfstests-prebuilt.tar.gz"
if [[ -n "${XFSTESTS_ARCHIVE_URL:-}" ]]; then
    echo "  fetching prebuilt archive from the supplied URL"
    rm -f /tmp/xfstests-prebuilt.tar.gz
    if curl --fail --location --retry 3 --retry-all-errors --connect-timeout 10 --max-time 900 \
        "$XFSTESTS_ARCHIVE_URL" --output /tmp/xfstests-prebuilt.tar.gz; then
        install -m 0644 /tmp/xfstests-prebuilt.tar.gz "$XFSTESTS_ARCHIVE"
    else
        echo "  archive URL download failed" >&2
    fi
    rm -f /tmp/xfstests-prebuilt.tar.gz
fi
# The archive is stored in Git LFS; a plain clone leaves a pointer behind.
if [[ -e "$XFSTESTS_ARCHIVE" ]] && ! gzip -t "$XFSTESTS_ARCHIVE" >/dev/null 2>&1; then
    if command -v git-lfs >/dev/null 2>&1; then
        echo "  archive is a Git LFS pointer; running git lfs pull"
        git -C "$SOURCE" lfs pull --include='tests/scripts/xfstests-prebuilt/xfstests-prebuilt.tar.gz' || true
    fi
fi
[[ -s "$XFSTESTS_ARCHIVE" ]] || fail "missing prebuilt xfstests archive: $XFSTESTS_ARCHIVE"
gzip -t "$XFSTESTS_ARCHIVE" >/dev/null 2>&1 || fail "prebuilt xfstests archive is not a gzip file (Git LFS pointer?): $XFSTESTS_ARCHIVE"
rm -rf /opt/xfstests-dev
tar -xzf "$XFSTESTS_ARCHIVE" -C /opt --transform 's|^xfstests|xfstests-dev|'
chmod +x /opt/xfstests-dev/check /opt/xfstests-dev/src/* 2>/dev/null || true
[[ -x /opt/xfstests-dev/check ]] || fail 'xfstests check binary is missing'

step 'overlaying native harness revisions'
[[ -d "$PAYLOAD_DIR/harness" ]] || fail "payload harness directory is missing: $PAYLOAD_DIR/harness"
install -m 0755 "$PAYLOAD_DIR/harness/run_perf_in_container.sh" \
    "$SOURCE/docker/compose-xfstests/run_perf_in_container.sh"
install -m 0755 "$PAYLOAD_DIR/harness/run_juicefs_perf_in_container.sh" \
    "$SOURCE/docker/compose-xfstests/run_juicefs_perf_in_container.sh"
install -m 0755 "$PAYLOAD_DIR/harness/perf_metadata_fallback.py" \
    "$SOURCE/docker/compose-xfstests/perf_metadata_fallback.py"
mkdir -p "$SOURCE/tools/perf"
install -m 0755 "$PAYLOAD_DIR/harness/perf_manifest.py" \
    "$SOURCE/tools/perf/perf_manifest.py"
mkdir -p "$NATIVE_DIR"
install -m 0755 "$PAYLOAD_DIR/harness/run_native_perf.sh" "$NATIVE_DIR/run_native_perf.sh"
install -m 0755 "$PAYLOAD_DIR/harness/perf_metadata_fallback.py" \
    /usr/local/bin/perf_metadata_fallback.py
install -m 0755 "$PAYLOAD_DIR/harness/perf_manifest.py" \
    /usr/local/bin/perf_manifest.py

git -C "$SOURCE" add -A
if ! git -C "$SOURCE" diff --cached --quiet; then
    git -C "$SOURCE" -c user.name='BrewFS Perf Image' -c user.email='perf-image@localhost' \
        commit -q -m 'chore(perf): overlay native harness revisions for the performance VM'
fi
OVERLAY_COMMIT="$(git -C "$SOURCE" rev-parse HEAD)"

step 'writing image manifest'
mkdir -p "$ROOT/artifacts" "$ROOT/cache" "$ROOT/bin"
printf '%s\n' "$OVERLAY_COMMIT" >"$ROOT/image-source-commit"
cat >"$ROOT/image-manifest.json" <<EOF
{
  "repository": "$REPO",
  "ref": "$REF",
  "baseCommit": "$BASE_COMMIT",
  "overlayCommit": "$OVERLAY_COMMIT",
  "juiceFsVersion": "$JUICEFS_VERSION",
  "preparedAt": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "managedBackends": "aliyun-oss + aliyun-redis-tair",
  "harnessOverlaySha256": {
    "run_perf_in_container.sh": "$(harness_sha "$SOURCE/docker/compose-xfstests/run_perf_in_container.sh")",
    "run_juicefs_perf_in_container.sh": "$(harness_sha "$SOURCE/docker/compose-xfstests/run_juicefs_perf_in_container.sh")",
    "perf_metadata_fallback.py": "$(harness_sha "$SOURCE/docker/compose-xfstests/perf_metadata_fallback.py")",
    "perf_manifest.py": "$(harness_sha "$SOURCE/tools/perf/perf_manifest.py")",
    "run_native_perf.sh": "$(harness_sha "$NATIVE_DIR/run_native_perf.sh")"
  }
}
EOF
chmod 0644 "$ROOT/image-manifest.json" "$ROOT/image-source-commit"

if [[ "${BREWFS_PERF_KEEP_PAYLOAD:-0}" != 1 ]]; then
    rm -rf "$PAYLOAD_DIR"
fi
sync

printf 'READY\n' >"$STATUS_FILE"
step "performance VM preparation complete (base=$BASE_COMMIT overlay=$OVERLAY_COMMIT)"
