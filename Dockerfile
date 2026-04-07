# syntax=docker/dockerfile:1.7
# ─── Builder ────────────────────────────────────────────────────────────────
FROM rust:1.89-slim-bookworm AS builder

WORKDIR /src

# Cache deps separately from source by copying manifests first
COPY Cargo.toml Cargo.lock ./
COPY crates/storage/Cargo.toml crates/storage/Cargo.toml
COPY crates/query/Cargo.toml   crates/query/Cargo.toml
COPY crates/server/Cargo.toml  crates/server/Cargo.toml
COPY crates/cli/Cargo.toml     crates/cli/Cargo.toml

# Create empty src trees so cargo can resolve+download deps without source
RUN mkdir -p crates/storage/src crates/query/src crates/server/src crates/cli/src \
 && echo 'pub fn _stub() {}' > crates/storage/src/lib.rs \
 && echo 'pub fn _stub() {}' > crates/query/src/lib.rs \
 && echo 'pub fn _stub() {}' > crates/server/src/lib.rs \
 && echo 'fn main() {}'      > crates/server/src/main.rs \
 && echo 'fn main() {}'      > crates/cli/src/main.rs \
 && cargo build --release -p powdb-server 2>/dev/null || true

# Now copy real source and build for real
COPY crates ./crates
RUN touch crates/storage/src/lib.rs crates/query/src/lib.rs crates/server/src/lib.rs crates/server/src/main.rs crates/cli/src/main.rs \
 && cargo build --release -p powdb-server

# ─── Runtime ────────────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

# tini reaps zombies and forwards signals so SIGTERM from fly cleanly stops the server
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates tini \
 && rm -rf /var/lib/apt/lists/*

# Persistent data dir; fly volume will be mounted here
RUN mkdir -p /data
VOLUME ["/data"]

COPY --from=builder /src/target/release/powdb-server /usr/local/bin/powdb-server

ENV RUST_LOG=info \
    POWDB_DATA=/data \
    POWDB_PORT=5433

EXPOSE 5433

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/usr/local/bin/powdb-server"]
