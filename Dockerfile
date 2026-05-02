# syntax=docker/dockerfile:1.4

# ─── Stage 1: compile arycal on the same old base as runtime ─────────────
FROM rust:1.85.0-slim-bullseye AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      build-essential \
      pkg-config \
      ca-certificates \
      libssl-dev \
      libopenmpi-dev openmpi-bin \
      libx11-dev libxrandr-dev libxi-dev \
      libgl1-mesa-dev libegl1-mesa libgtk-3-dev \
      binutils && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Copy manifests/crates first for better cache behavior
COPY Cargo.toml .
COPY crates/ ./crates/

RUN cargo fetch

# Copy full source
COPY . .

# Build and strip
RUN --mount=type=cache,target=/app/target \
    cargo build --release --bin arycal && \
    cp /app/target/release/arycal /app/arycal && \
    strip /app/arycal


# ─── Stage 2: Debian 11 runtime ─────────────────────────────────────────
FROM debian:bullseye-slim

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      ca-certificates \
      libstdc++6 \
      libgcc-s1 \
      && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/arycal /usr/local/bin/arycal

ENTRYPOINT ["/usr/local/bin/arycal"]
CMD ["--help"]
