FROM debian:trixie-slim

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
        xfsprogs \
        acl \
        attr \
        bc \
        dbench \
        dump \
        e2fsprogs \
        fio \
        liburing2 \
        gawk \
        libuuid1 \
        lvm2 \
        make \
        perl \
        psmisc \
        python3 \
        quota \
        redis-tools \
        sed \
        stress-ng \
        strace \
        sudo \
        uuid-runtime \
        xfsdump \
        exfatprogs \
        f2fs-tools \
        udftools && \
    rm -rf /var/lib/apt/lists/*

COPY target/docker/brewfs /usr/local/bin/brewfs
COPY tests/scripts/xfstests-prebuilt/xfstests-prebuilt.tar.gz /opt/xfstests-prebuilt.tar.gz
COPY tests/scripts/xfstests_slayer.exclude /opt/xfstests_slayer.exclude
COPY docker/xfstests_report.sh /usr/local/bin/xfstests_report.sh
COPY docker/compose-xfstests/run_xfstests_in_container.sh /usr/local/bin/run_xfstests_in_container.sh
COPY docker/compose-xfstests/perf_metadata_fallback.py /usr/local/bin/perf_metadata_fallback.py

RUN set -e; \
    chmod +x /usr/local/bin/brewfs /usr/local/bin/xfstests_report.sh /usr/local/bin/run_xfstests_in_container.sh /usr/local/bin/perf_metadata_fallback.py; \
    mkdir -p /opt/xfstests-dev /mnt/brewfs /run/brewfs /var/lib/brewfs/data /artifacts; \
    if gzip -t /opt/xfstests-prebuilt.tar.gz >/dev/null 2>&1; then \
        tar -xzf /opt/xfstests-prebuilt.tar.gz -C /opt --transform 's|^xfstests|xfstests-dev|'; \
    else \
        echo "warning: xfstests prebuilt archive is unavailable; fio-only perf runs can still proceed" >&2; \
    fi; \
    chmod +x /opt/xfstests-dev/check /opt/xfstests-dev/src/* 2>/dev/null || true; \
    cp -f /opt/xfstests_slayer.exclude /opt/xfstests-dev/xfstests_slayer.exclude; \
    rm -f /opt/xfstests-prebuilt.tar.gz; \
    grep -q '^user_allow_other$' /etc/fuse.conf 2>/dev/null || echo 'user_allow_other' >> /etc/fuse.conf

ENV BREWFS_HOME=/var/lib/brewfs \
    BREWFS_MOUNT_POINT=/mnt/brewfs \
    BREWFS_DATA_BACKEND=local-fs \
    BREWFS_DATA_DIR=/var/lib/brewfs/data \
    BREWFS_S3_BUCKET=brewfs-data \
    BREWFS_S3_ENDPOINT=http://rustfs:9000 \
    BREWFS_S3_REGION=us-east-1 \
    BREWFS_S3_FORCE_PATH_STYLE=true \
    BREWFS_META_BACKEND=redis \
    BREWFS_META_URL=redis://redis:6379/0 \
    BREWFS_CONFIG_PATH=/run/brewfs/config.yaml \
    BREWFS_LOG_FILE=/artifacts/brewfs.log \
    XFSTESTS_DIR=/opt/xfstests-dev \
    RUST_LOG=brewfs=info

VOLUME ["/mnt/brewfs", "/var/lib/brewfs", "/artifacts"]

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/run_xfstests_in_container.sh"]
