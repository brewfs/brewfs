#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
root=/opt/entity-packed-million-r2
test "$(id -u)" = 0
mkdir -p "$root/artifacts"
export DEBIAN_FRONTEND=noninteractive
apt-get -o Acquire::Retries=3 -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30 update
apt-get -o Acquire::Retries=3 -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30 install -y --no-install-recommends \
  docker.io docker-compose-v2 nginx fio fuse3 libfuse3-3 liburing2 libssl3t64 \
  libzstd1 liblz4-1 zlib1g libstdc++6 python3 curl ca-certificates util-linux
systemctl disable --now nginx
systemctl enable --now docker
dpkg-query -W docker.io docker-compose-v2 nginx fio fuse3 python3 liburing2 libssl3t64 > "$root/artifacts/software-packages.txt"
printf 'ready\n' > "$root/packages-ready"
