# Container image for a real, networked gRPC D server (Milestone 2 PR step 7).
#
# A multi-stage build so the produced binary is glibc-portable: it is compiled in
# the Rust image and run on the matching Debian runtime, with no dependency on the
# host's toolchain or libc (the host may be far newer than any base image). The
# `d-server` role is a subcommand of the single `wyrd` binary (ADR-0014,
# ADR-0016): it opens a local filesystem ChunkStore and serves it over the gRPC
# ChunkStore service. Used by `cargo xtask integration` /
# `tests/tier2_integration.rs`.

# Build stage — compile the wyrd binary against the image's pinned glibc.
# Pin the exact patch (`1.96.0`) to match `rust-toolchain.toml`, so the base tag
# floating to a newer patch can't trigger a build-time toolchain re-download.
FROM rust:1.96.0-bookworm AS build
# Optional cargo feature set for the build. Empty by default so the root dev
# `docker-compose.yml` and the `crates/chunkstore-grpc/tests/docker-compose.yml`
# integration fixture keep the plain default-feature `d-server` binary unchanged.
# The consolidated single-zone stack (`deploy/small-multi-node/`) passes
# `FEATURES=tikv,etcd` so `wyrd d-server`/`custodian`/`s3` can dial the etcd
# coordination ensemble and the TiKV metadata backend (see that compose file).
ARG FEATURES=""
# The `tikv`/`etcd` features pull native build deps (grpcio/protobuf via the TiKV
# client). Install them only when a feature set is requested, so the default build
# stays lean; `cmake`/`protobuf-compiler`/`libssl-dev` cover the tikv-client + etcd
# transitive native builds.
RUN if [ -n "$FEATURES" ]; then \
        apt-get update && apt-get install -y --no-install-recommends \
            cmake protobuf-compiler libssl-dev pkg-config \
        && rm -rf /var/lib/apt/lists/*; \
    fi
WORKDIR /src
COPY . .
# `--locked` so the container build uses the committed `Cargo.lock` exactly,
# matching the gated workspace rather than silently resolving newer deps.
RUN cargo build --release --locked --bin wyrd ${FEATURES:+--features "$FEATURES"}

# Runtime stage — a minimal image hosting just the d-server role.
FROM debian:bookworm-slim
COPY --from=build /src/target/release/wyrd /usr/local/bin/wyrd
# Drop root (issue #286): a dedicated unprivileged system user/group, and
# `/data` pre-created and owned by it so a compose service's `--data-dir /data`
# (e.g. `deploy/small-multi-node/docker-compose.yml`) is writable once we switch to
# it below. `--system` skips password/home-dir setup nothing here needs; the
# fixed 10001:10001 keeps the id stable across rebuilds without depending on
# base-image UID allocation order.
RUN groupadd --system --gid 10001 dserver \
    && useradd --system --uid 10001 --gid dserver --no-create-home --shell /usr/sbin/nologin dserver \
    && mkdir -p /data \
    && chown dserver:dserver /data
# gRPC listen port; the compose file publishes it on an ephemeral host port.
EXPOSE 50051
# Non-root for the process the ENTRYPOINT starts (must come after the file
# ownership above and before ENTRYPOINT to actually apply to it).
USER dserver:dserver
ENTRYPOINT ["wyrd"]
