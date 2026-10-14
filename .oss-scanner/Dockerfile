# Scan image for Anthropic's OSS Scanner (https://github.com/anthropics/oss-scanner),
# issue #890. The scanner builds this file with the repository as the build context and
# network access, then runs the finished image with NO network. So everything a test or
# a build needs is fetched and compiled here; nothing is downloaded later.
#
# What the image holds, all at /src:
#   * the workspace (minus `wyrd-dst`) built with ALL features — `tikv`, `fdb`, `etcd` —
#     in the dev profile (debug info, debug assertions and overflow checks on): every
#     library, both binaries (`wyrd`, `wyrd-validate`), and every test binary, in target/;
#   * the madsim DST tier (`wyrd-dst`, `--cfg madsim`) built in target-madsim/, kept in
#     its own target dir so its RUSTFLAGS never invalidate the main build.
# `threat_model.md`, next to this file, says how to run each part.
#
# Native build deps are the same set `deploy/docker/wyrd/Dockerfile` installs for its
# `fdb` + `tikv,etcd` flavors; keep the two lists and `FDB_VERSION` in step with it.

# Pin the exact patch to match `rust-toolchain.toml`, so the base image's toolchain is
# the one the repository builds with.
FROM rust:1.96.0-bookworm
ARG FDB_VERSION=7.3.77
ENV DEBIAN_FRONTEND=noninteractive

# cmake/protobuf-compiler/libssl-dev/pkg-config: the tikv-client and etcd-client native
# builds. clang/libclang-dev: `foundationdb-sys`' bindgen. foundationdb-clients: the FDB
# C headers and `libfdb_c`, which the `fdb` feature links at build and load time.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        cmake protobuf-compiler libssl-dev pkg-config clang libclang-dev curl ca-certificates; \
    curl -fsSL -o /tmp/fdb-clients.deb \
        "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-clients_${FDB_VERSION}-1_amd64.deb"; \
    dpkg -i /tmp/fdb-clients.deb; \
    rm -f /tmp/fdb-clients.deb; \
    rm -rf /var/lib/apt/lists/*

# Scanner contract: the checkout lives inside the image at /src.
COPY . /src
WORKDIR /src

# Install any `rust-toolchain.toml` components the base image lacks while the network
# is still up, then fetch every crate in Cargo.lock (all features, all targets,
# including the madsim-only ones) so the offline phase never reaches crates.io.
RUN rustup show active-toolchain || rustup toolchain install
RUN cargo fetch --locked

# The main build: what `cargo xtask ci` builds, plus the three off-by-default backend
# features, so the scanner sees the FDB, TiKV and etcd code paths compiled and testable.
RUN cargo build --locked --workspace --exclude wyrd-dst --all-targets --all-features \
    && cargo test --locked --workspace --exclude wyrd-dst --all-features --no-run

# The DST tier, as `cargo xtask dst` builds it.
RUN RUSTFLAGS="--cfg madsim" CARGO_TARGET_DIR=/src/target-madsim \
    cargo test --locked -p wyrd-dst --no-run

# Run the test suites once so the build log shows the image works, without letting a
# failure abort the build. Tests that need a live FDB/TiKV/etcd cluster skip when none
# is configured.
RUN (cargo test --locked --workspace --exclude wyrd-dst --all-features \
        || echo "WARNING: workspace tests reported failures") \
    && (RUSTFLAGS="--cfg madsim" CARGO_TARGET_DIR=/src/target-madsim MADSIM_TEST_NUM=50 \
        cargo test --locked -p wyrd-dst \
        || echo "WARNING: DST tests reported failures")
