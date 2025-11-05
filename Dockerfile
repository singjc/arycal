# syntax=docker/dockerfile:1.4

# ─── Stage 1: compile both binaries ────────────────────────────────────
FROM rust:1.85-slim AS builder
WORKDIR /app

# 1) Install your system deps (MPI, GTK, etc.) and C/C++ toolchain
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      build-essential \
      pkg-config \
      libssl-dev \
      libopenmpi-dev openmpi-bin \
      libx11-dev libxrandr-dev libxi-dev \
      libgl1-mesa-dev libegl1-mesa libgtk-3-dev && \
    rm -rf /var/lib/apt/lists/*

# 2) Copy just the manifests & crates folder so cargo fetch can cache
COPY Cargo.toml .
COPY crates/ ./crates/

# 3) Pre-fetch all deps
RUN cargo fetch

# 4) Copy the rest of your source
COPY . .

# ─── Build & strip optimized release binaries ───────────────────────────

# Build arycal and immediately strip symbols
RUN --mount=type=cache,target=/app/target \
    cargo build --release --bin arycal && \
    strip target/release/arycal

# # Build arycal-gui and strip
# RUN --mount=type=cache,target=/app/target \
#     cargo build --release --bin arycal-gui && \
#     strip target/release/arycal-gui

# ─── Stage 2: minimal runtime ───────────────────────────────────────────
FROM debian:bullseye-slim
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      ca-certificates && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/arycal    /usr/local/bin/arycal
# COPY --from=builder /app/target/release/arycal-gui /usr/local/bin/arycal-gui

ENTRYPOINT ["arycal"]
CMD ["--help"]
