FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY target/release/brewfs-operator /usr/local/bin/brewfs-operator
ENTRYPOINT ["/usr/local/bin/brewfs-operator"]
