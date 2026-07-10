# First-class OCI image for the `wyrd` binary — the production artifact an operator
# actually runs (issue #470, ADR-0042). Until now the only Dockerfile that built
# `wyrd` lived under `crates/chunkstore-grpc/tests/dserver/` — a TEST fixture reused
# ad hoc by `deploy/small-multi-node/` — and it could not build the `fdb` feature at
# all (its build stage installs only `cmake protobuf-compiler libssl-dev pkg-config`,
# never the FoundationDB client headers/`libfdb_c`/`libclang` that `foundationdb-sys`'
# `bindgen` build script needs, and its runtime stage carries no `libfdb_c`/`fdbcli`).
#
# This image fixes both. It is a multi-stage build so the produced binary is
# glibc-portable (compiled in the Rust image, run on the matching Debian runtime),
# mirroring `crates/chunkstore-grpc/tests/dserver/Dockerfile`. It is PARAMETERIZED
# (`ARG FEATURES`, `ARG FDB_VERSION`) so #471 can adopt the same skeleton for the
# `wyrd:tikv` flavor by passing `FEATURES=tikv,etcd`; this bundle does not migrate the
# TiKV stack (that is #471's slice).
#
# ── FoundationDB version coupling (the whole point) ──
# `FDB_VERSION` is the SINGLE SOURCE OF TRUTH for the client baked into the image. It
# MUST stay lockstep with the cluster the repo deploys and the crate the binary links:
#   * exact patch == the image tag in `deploy/fdb-single-node/docker-compose.yml`
#     (`foundationdb/foundationdb:7.3.77`); and
#   * major.minor == the crate pin `foundationdb = { … features = ["fdb-7_3"] }`
#     (`Cargo.toml:108` → 7.3).
# A silent drift between the baked client and the deployed cluster is exactly the
# failure mode #441's version-skew guard exists to diagnose; `xtask/tests/fdb_image.rs`
# mechanically pins all three together so drift fails the gate, not production.

# Default matches `deploy/fdb-single-node/docker-compose.yml`'s pinned cluster tag.
ARG FDB_VERSION=7.3.77
# Optional cargo feature set. Empty by default so a plain `docker build` produces the
# default-feature binary unchanged, exactly like the dserver fixture.
ARG FEATURES=""

# ── Build stage — compile `wyrd` against the image's pinned glibc. ──
# Pin the exact patch (`1.96.0`) to match `rust-toolchain.toml` (`:4`) so a floating
# base tag can't trigger a build-time toolchain re-download.
FROM rust:1.96.0-bookworm AS build
ARG FEATURES
ARG FDB_VERSION
# Native build deps are installed only when a feature set is requested, so the default
# build stays lean. `cmake`/`protobuf-compiler`/`libssl-dev`/`pkg-config` cover the
# tikv-client + etcd transitive native builds; when `fdb` is in the set we additionally
# install `clang`/`libclang-dev` (`foundationdb-sys`' `bindgen` `dlopen`s `libclang`,
# the reason `deny.toml:39-45` allowlists ISC for `libloading`) and the pinned
# `foundationdb-clients` package (FDB C headers + `libfdb_c` for the FFI bindings).
RUN set -eux; \
    if [ -n "$FEATURES" ]; then \
        apt-get update; \
        apt-get install -y --no-install-recommends \
            cmake protobuf-compiler libssl-dev pkg-config; \
        case ",$FEATURES," in \
            *,fdb,*) \
                apt-get install -y --no-install-recommends \
                    clang libclang-dev curl ca-certificates; \
                curl -fsSL -o /tmp/fdb-clients.deb \
                    "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-clients_${FDB_VERSION}-1_amd64.deb"; \
                dpkg -i /tmp/fdb-clients.deb; \
                rm -f /tmp/fdb-clients.deb; \
                ;; \
        esac; \
        rm -rf /var/lib/apt/lists/*; \
    fi
WORKDIR /src
COPY . .
# `--locked` so the container build uses the committed `Cargo.lock` exactly, matching
# the gated workspace rather than silently resolving newer deps.
RUN cargo build --release --locked --bin wyrd ${FEATURES:+--features "$FEATURES"}

# ── Runtime stage — a minimal image hosting just the `wyrd` binary. ──
FROM debian:bookworm-slim
ARG FEATURES
ARG FDB_VERSION
# When `fdb` is in the feature set, install the SAME pinned `foundationdb-clients`
# package so the binary links `libfdb_c` at load time and operators (and #441's doctor
# row, #439) have `fdbcli`. `curl`/`ca-certificates` are pulled only to fetch the `.deb`
# and purged afterwards so the runtime image carries no download tooling.
RUN set -eux; \
    apt-get update; \
    case ",$FEATURES," in \
        *,fdb,*) \
            apt-get install -y --no-install-recommends curl ca-certificates; \
            curl -fsSL -o /tmp/fdb-clients.deb \
                "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-clients_${FDB_VERSION}-1_amd64.deb"; \
            dpkg -i /tmp/fdb-clients.deb; \
            rm -f /tmp/fdb-clients.deb; \
            apt-get purge -y --auto-remove curl; \
            ;; \
    esac; \
    rm -rf /var/lib/apt/lists/*
# Bake the external-client directory into the image LAYOUT (empty in steady state) and
# wire #441's multi-version client support to it: #441
# (`crates/metadata-fdb/src/lib.rs:1119-1128`, merged in PR #495) reads
# `WYRD_FDB_EXTERNAL_CLIENT_DIR` and sets `NetworkOption::ExternalClientDirectory` — WITHOUT
# `DisableLocalClient` — so the binary's *linked* `libfdb_c` (the pinned `${FDB_VERSION}`
# primary) stays the client for the steady-state single-version case, and the external
# directory supplies ADDITIONAL versions only during a lockstep upgrade.
#
# The directory is deliberately left EMPTY here — it must NOT be pre-populated with the
# primary's OWN version. FoundationDB's multi-version client rejects an external client whose
# protocol version equals the linked client's as a `DuplicateClientVersion` and DISABLES it
# (verified in the client trace: `Keeping="internal" Disabling=".../libfdb_c_7.3.77.so"`);
# with the only external client disabled and the local client fronting an otherwise-empty
# multi-version set, `get_client_status()` fails immediately and the connect is misreported as
# `Unreachable` (issue #470 leg (c) reproduced exactly this before this fix). An empty
# directory + the env set is byte-safe: FDB uses the linked primary and connects normally.
#
# The multi-version upgrade dance (07-deployment-view.md §7.6, "the multi-version client
# upgrade dance") is then an IMAGE REBUILD, not a code change: to bridge `${FDB_VERSION}` → a
# NEW cluster version, add the NEW (different) `libfdb_c` into this directory (the linked
# primary keeps speaking the old protocol; the external client speaks the new) — "two client
# libraries", the linked one plus one dropped here. That drop is a `COPY` a future image
# revision adds; today, one version, so the directory stays empty.
RUN set -eux; \
    mkdir -p /var/lib/wyrd/fdb/external-clients
ENV WYRD_FDB_EXTERNAL_CLIENT_DIR=/var/lib/wyrd/fdb/external-clients
# OCI labels record the baked client/cli version so an operator (and #441's doctor) can
# read the image's FDB line without unpacking it.
LABEL org.opencontainers.image.title="wyrd" \
      org.opencontainers.image.description="Wyrd binary with the FoundationDB metadata backend (ADR-0042)" \
      org.opencontainers.image.source="https://github.com/getwyrd/wyrd" \
      dev.wyrd.fdb.libfdb_c.version="${FDB_VERSION}" \
      dev.wyrd.fdb.fdbcli.version="${FDB_VERSION}"
COPY --from=build /src/target/release/wyrd /usr/local/bin/wyrd
# Drop root (issue #286, mirroring the dserver fixture at `:56-64`): a dedicated
# unprivileged system user/group with the stable fixed 10001:10001 id, and `/data`
# pre-created and owned by it so a compose service's `--data-dir /data` is writable
# once we switch to it. `--system` skips the password/home-dir setup nothing here needs.
RUN groupadd --system --gid 10001 wyrd \
    && useradd --system --uid 10001 --gid wyrd --no-create-home --shell /usr/sbin/nologin wyrd \
    && mkdir -p /data \
    && chown wyrd:wyrd /data /var/lib/wyrd/fdb/external-clients
# Non-root for the process the ENTRYPOINT starts (must come after the file ownership
# above and before ENTRYPOINT to actually apply to it — the order is load-bearing, see
# the dserver fixture's comment at `:62-63`).
USER wyrd:wyrd
ENTRYPOINT ["wyrd"]
