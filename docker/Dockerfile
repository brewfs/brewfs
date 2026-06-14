FROM debian:trixie-slim

ARG XFSTESTS_BINARY=xfs_io

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        fuse3 \
        libsqlite3-0 \
        libssl3 \
        procps \
        tini \
        util-linux \
        xfsprogs && \
    rm -rf /var/lib/apt/lists/*

COPY target/release/brewfs /usr/local/bin/brewfs
COPY tests/scripts/xfstests-prebuilt/xfstests-prebuilt.tar.gz /tmp/xfstests-prebuilt.tar.gz
COPY docker/entrypoint.sh /usr/local/bin/brewfs-entrypoint

RUN set -e; \
    mkdir -p /opt/xfstests/bin /mnt/brewfs /run/brewfs /var/lib/brewfs/data; \
    tar -xzf /tmp/xfstests-prebuilt.tar.gz -C /tmp xfstests/bundled/bin/${XFSTESTS_BINARY}; \
    cp /tmp/xfstests/bundled/bin/${XFSTESTS_BINARY} /opt/xfstests/bin/${XFSTESTS_BINARY}; \
    chmod +x /usr/local/bin/brewfs /usr/local/bin/brewfs-entrypoint /opt/xfstests/bin/${XFSTESTS_BINARY}; \
    rm -rf /tmp/xfstests /tmp/xfstests-prebuilt.tar.gz; \
    grep -q '^user_allow_other$' /etc/fuse.conf 2>/dev/null || echo 'user_allow_other' >> /etc/fuse.conf

ENV BREWFS_HOME=/var/lib/brewfs \
    BREWFS_MOUNT_POINT=/mnt/brewfs \
    BREWFS_DATA_BACKEND=local-fs \
    BREWFS_DATA_DIR=/var/lib/brewfs/data \
    BREWFS_META_BACKEND=sqlite \
    BREWFS_SQLITE_PATH=/var/lib/brewfs/metadata.db \
    RUST_LOG=brewfs=info

VOLUME ["/mnt/brewfs", "/var/lib/brewfs"]

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/brewfs-entrypoint"]