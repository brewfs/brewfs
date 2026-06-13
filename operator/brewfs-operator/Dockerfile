FROM rust:1.96-bookworm AS builder

WORKDIR /workspace
RUN apt-get update \
    && apt-get install -y --no-install-recommends binutils \
    && rm -rf /var/lib/apt/lists/*

COPY . .
RUN cargo build --release \
    && strip target/release/brewfs-operator

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /workspace/target/release/brewfs-operator /usr/local/bin/brewfs-operator
ENTRYPOINT ["/usr/local/bin/brewfs-operator"]
