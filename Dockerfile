# syntax=docker/dockerfile:1
#
# Multi-stage build for stream-server with the libtorrent backend.
# Mirrors the "check"/"build-linux" jobs in .github/workflows/release.yml:
# same base OS family (Debian/Ubuntu), same libtorrent version, same
# cargo feature flags, so the image matches what CI already validates.

########################################
# Stage 1: cargo-chef base
########################################
FROM rust:1-bookworm AS chef
RUN cargo install cargo-chef --locked
WORKDIR /app

########################################
# Stage 2: dependency recipe (for Docker layer caching)
########################################
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

########################################
# Stage 3: builder
########################################
FROM chef AS builder

ENV DEBIAN_FRONTEND=noninteractive \
    LIBTORRENT_STATIC=1 \
    PKG_CONFIG_PATH=/usr/local/lib/pkgconfig

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential cmake curl pkg-config \
        libssl-dev libboost-dev libclang-dev \
        libgtk-3-dev libayatana-appindicator3-dev \
    && rm -rf /var/lib/apt/lists/*

# Build libtorrent-rasterbar 2.1.1 from source, statically, the same way CI does.
COPY .github/scripts/install-libtorrent.sh .github/scripts/install-libtorrent.sh
RUN bash .github/scripts/install-libtorrent.sh

# Cache dependency compilation separately from application code.
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json --features libtorrent --no-default-features

COPY . .
RUN cargo build --release --features libtorrent --no-default-features

########################################
# Stage 4: runtime
########################################
FROM debian:bookworm-slim AS runtime

ENV DEBIAN_FRONTEND=noninteractive \
    RUST_LOG=info

# server links against the desktop-integration stack (tray icon / settings GUI)
# on all cfg(unix/windows/macos) targets, so the runtime image needs the
# shared libraries those bring in even though it starts headless. Installing
# the -dev metapackages (rather than hand-picking .so names) guarantees every
# transitive runtime library apt would install for the equivalent build
# dependency is present too, avoiding "error while loading shared libraries"
# at startup.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates ffmpeg \
        libssl-dev libboost-dev libgtk-3-dev libayatana-appindicator3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/server /usr/local/bin/stream-server

# HTTP API / streaming, HTTPS. BitTorrent peer traffic uses an
# ephemeral/DHT-negotiated port by default -- run with --network=host or
# publish additional ports if you pin one via the /settings API.
EXPOSE 11470/tcp 12470/tcp

VOLUME ["/root/.config/stremio-server", "/root/.cache/stremio-server"]

# --no-tray forces the headless code path (no tray/GUI init), since the
# release binary otherwise defaults to tray mode, which requires a display.
ENTRYPOINT ["/usr/local/bin/stream-server", "--no-tray"]
