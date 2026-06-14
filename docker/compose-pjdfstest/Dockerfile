FROM debian:trixie-slim AS builder

ARG PJDFSTEST_REPOSITORY=https://github.com/pjd/pjdfstest.git
ARG PJDFSTEST_REF=master

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        autoconf \
        automake \
        build-essential \
        ca-certificates \
        git \
        perl \
        pkg-config && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build
RUN git clone --depth 1 --branch "$PJDFSTEST_REF" "$PJDFSTEST_REPOSITORY" . && \
    autoreconf -ifs && \
    ./configure && \
    make -j"$(nproc)"

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
        acl \
        attr \
        gawk \
        make \
        perl \
        psmisc \
        sed \
        sudo && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build /opt/pjdfstest
COPY target/release/brewfs /usr/local/bin/brewfs
COPY docker/compose-pjdfstest/run_pjdfstest_in_container.sh /usr/local/bin/run_pjdfstest_in_container.sh

RUN set -e; \
    chmod +x /usr/local/bin/brewfs /usr/local/bin/run_pjdfstest_in_container.sh; \
    mkdir -p /mnt/brewfs /run/brewfs /var/lib/brewfs/data /artifacts; \
    grep -q '^user_allow_other$' /etc/fuse.conf 2>/dev/null || echo 'user_allow_other' >> /etc/fuse.conf

ENV BREWFS_HOME=/var/lib/brewfs \
    BREWFS_MOUNT_POINT=/mnt/brewfs \
    BREWFS_DATA_BACKEND=s3 \
    BREWFS_DATA_DIR=/var/lib/brewfs/data \
    BREWFS_S3_BUCKET=brewfs-data \
    BREWFS_S3_ENDPOINT=http://rustfs:9000 \
    BREWFS_S3_REGION=us-east-1 \
    BREWFS_S3_FORCE_PATH_STYLE=true \
    BREWFS_META_BACKEND=redis \
    BREWFS_META_URL=redis://redis:6379/0 \
    BREWFS_CONFIG_PATH=/run/brewfs/config.yaml \
    BREWFS_LOG_FILE=/artifacts/brewfs.log \
    PJDFSTEST_DIR=/opt/pjdfstest \
    RUST_LOG=brewfs=info

VOLUME ["/mnt/brewfs", "/var/lib/brewfs", "/artifacts"]

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/run_pjdfstest_in_container.sh"]
