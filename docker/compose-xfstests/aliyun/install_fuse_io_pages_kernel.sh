#!/usr/bin/env bash
# Build and install a small Ubuntu-compatible kernel with the upstream FUSE
# buffered-read io_pages fix. The base Ubuntu 6.8 kernel has FUSE built in
# (CONFIG_FUSE_FS=y), so replacing a module is not possible.
set -Eeuo pipefail

export DEBIAN_FRONTEND=noninteractive

ROOT="${BREWFS_PERF_KERNEL_ROOT:-/opt/brewfs-perf/kernel}"
SOURCE_URL="${BREWFS_FUSE_KERNEL_SOURCE_URL:-https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.8.12.tar.xz}"
SOURCE_URLS="${BREWFS_FUSE_KERNEL_SOURCE_URLS:-https://mirrors.aliyun.com/linux-kernel/v6.x/linux-6.8.12.tar.xz https://mirrors.edge.kernel.org/pub/linux/kernel/v6.x/linux-6.8.12.tar.xz $SOURCE_URL}"
DOWNLOAD_CONNECT_TIMEOUT="${BREWFS_FUSE_KERNEL_CONNECT_TIMEOUT:-15}"
DOWNLOAD_MAX_TIME="${BREWFS_FUSE_KERNEL_MAX_TIME:-300}"
PATCH_ID="98b4ca2378e1f6b6c06a74f699623ebecfb3549d"
LOCALVERSION="${BREWFS_FUSE_KERNEL_LOCALVERSION:--brewfs-io-pages}"
JOBS="${BREWFS_FUSE_KERNEL_JOBS:-$(nproc)}"
CONFIG_MODE="${BREWFS_FUSE_KERNEL_CONFIG_MODE:-localmodconfig}"
MANIFEST="/var/lib/brewfs-perf/kernel-manifest.txt"

log() { printf '[fuse-io-pages] %s\n' "$*"; }
fail() { log "ERROR: $*" >&2; exit 1; }
wait_for_dpkg_lock() {
    exec 9>/var/lib/dpkg/lock-frontend
    for _ in $(seq 1 180); do
        if flock -n 9; then
            flock -u 9
            exec 9>&-
            return 0
        fi
        log 'waiting for another dpkg/apt process to release the package lock'
        sleep 5
    done
    exec 9>&-
    fail 'timed out waiting for the dpkg package lock'
}

[[ "$(id -u)" -eq 0 ]] || fail 'must run as root'
[[ "$JOBS" =~ ^[1-9][0-9]*$ ]] || fail "invalid job count: $JOBS"

running_release="$(uname -r)"
if [[ -r "$MANIFEST" ]] && grep -q "patch=$PATCH_ID" "$MANIFEST" &&
   grep -q "localversion=$LOCALVERSION" "$MANIFEST"; then
    log "patched kernel already installed (manifest=$MANIFEST)"
    exit 0
fi

log "installing kernel build dependencies"
wait_for_dpkg_lock
apt-get update -qq
wait_for_dpkg_lock
apt-get install --no-upgrade --no-install-recommends -y -qq \
    bc bison build-essential ca-certificates cpio debhelper-compat flex fakeroot \
    kmod libelf-dev libssl-dev openssl pkg-config rsync xz-utils

mkdir -p "$ROOT"
archive="$ROOT/linux-6.8.12.tar.xz"
source_dir="$ROOT/linux-6.8.12"
if [[ ! -s "$archive" ]]; then
    downloaded=0
    downloaded_source_url=""
    for source_url in $SOURCE_URLS; do
        log "downloading $source_url"
        rm -f "$archive.part"
        if curl --fail --location --retry 2 --retry-all-errors \
            --connect-timeout "$DOWNLOAD_CONNECT_TIMEOUT" --max-time "$DOWNLOAD_MAX_TIME" \
            "$source_url" --output "$archive.part" && \
            tar -tJf "$archive.part" >/dev/null 2>&1; then
            mv "$archive.part" "$archive"
            downloaded=1
            downloaded_source_url="$source_url"
            break
        fi
        log "download failed or archive is invalid; trying the next source"
    done
    (( downloaded == 1 )) || fail "unable to download a valid kernel archive from: $SOURCE_URLS"
else
    downloaded_source_url="$SOURCE_URL"
fi
tar -tJf "$archive" >/dev/null 2>&1 || fail "invalid kernel archive: $archive"

if [[ ! -d "$source_dir" ]]; then
    log 'extracting kernel source'
    tar -xJf "$archive" -C "$ROOT"
fi
cd "$source_dir"

if ! grep -q "fm->sb->s_bdi->io_pages = fc->max_pages;" fs/fuse/inode.c; then
    log "applying upstream patch $PATCH_ID"
    python3 - "$PATCH_ID" <<'PY'
from pathlib import Path

path = Path('fs/fuse/inode.c')
text = path.read_text()
needle = "\t\tfm->sb->s_bdi->ra_pages =\n\t\t\t\tmin(fm->sb->s_bdi->ra_pages, ra_pages);\n"
replacement = needle + "\t\tfm->sb->s_bdi->io_pages = fc->max_pages;\n"
if needle not in text:
    raise SystemExit('FUSE init context did not match the expected 6.8 source')
path.write_text(text.replace(needle, replacement, 1))
PY
fi
grep -q "fm->sb->s_bdi->io_pages = fc->max_pages;" fs/fuse/inode.c || \
    fail 'io_pages patch was not present after patching'

if [[ ! -f .config ]]; then
    config="/boot/config-$running_release"
    [[ -r "$config" ]] || fail "missing running kernel config: $config"
    cp "$config" .config
fi

# Ubuntu's config references certificates that are not part of a vanilla
# kernel tarball. Keep the distro configuration but disable those paths.
scripts/config --set-str LOCALVERSION "$LOCALVERSION" 2>/dev/null || true
scripts/config --disable LOCALVERSION_AUTO 2>/dev/null || true
scripts/config --set-str SYSTEM_TRUSTED_KEYS '' 2>/dev/null || true
scripts/config --set-str SYSTEM_REVOCATION_KEYS '' 2>/dev/null || true
# BTF/debug metadata is not needed for the FUSE benchmark and would add a
# large pahole dependency to the image builder.
scripts/config --disable DEBUG_INFO_BTF 2>/dev/null || true
scripts/config --disable DEBUG_INFO 2>/dev/null || true
scripts/config --enable FUSE_FS 2>/dev/null || true
case "$CONFIG_MODE" in
    localmodconfig)
        log 'reducing kernel configuration to modules used by the ECS base system'
        yes '' | make localmodconfig
        scripts/config --enable FUSE_FS 2>/dev/null || true
        ;;
    olddefconfig)
        ;;
    *)
        fail "invalid kernel config mode: $CONFIG_MODE (expected localmodconfig or olddefconfig)"
        ;;
esac
make olddefconfig

log "building kernel (jobs=$JOBS)"
make -j"$JOBS" bindeb-pkg \
    LOCALVERSION="$LOCALVERSION" \
    KDEB_PKGVERSION="1~brewfs1"

mapfile -t image_packages < <(find .. -maxdepth 1 -type f -name 'linux-image-*.deb' -printf '%p\n' | sort)
mapfile -t header_packages < <(find .. -maxdepth 1 -type f -name 'linux-headers-*.deb' -printf '%p\n' | sort)
(( ${#image_packages[@]} > 0 )) || fail 'kernel image package was not produced'

wait_for_dpkg_lock
log 'installing generated kernel packages'
dpkg -i "${image_packages[@]}" "${header_packages[@]}" || apt-get -f install -y -qq
update-grub

mkdir -p "$(dirname "$MANIFEST")"
{
    printf 'patch=%s\n' "$PATCH_ID"
    printf 'source_url=%s\n' "$downloaded_source_url"
    printf 'source_urls=%s\n' "$SOURCE_URLS"
    printf 'source_dir=%s\n' "$source_dir"
    printf 'base_release=%s\n' "$running_release"
    printf 'localversion=%s\n' "$LOCALVERSION"
    printf 'config_mode=%s\n' "$CONFIG_MODE"
    printf 'built_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'image_packages=%s\n' "$(printf '%s,' "${image_packages[@]}" | sed 's/,$//')"
    printf 'header_packages=%s\n' "$(printf '%s,' "${header_packages[@]}" | sed 's/,$//')"
} >"$MANIFEST"
chmod 0644 "$MANIFEST"
log 'installed; reboot is required to activate the patched kernel'
if [[ "${BREWFS_FUSE_KERNEL_REBOOT:-0}" == 1 ]]; then
    log 'reboot requested by BREWFS_FUSE_KERNEL_REBOOT=1'
    systemctl reboot
fi
