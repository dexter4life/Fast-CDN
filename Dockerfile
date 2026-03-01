# syntax=docker/dockerfile:1.5

# ==============================================================================
# CDN Engine - Multi-stage Dockerfile
# Uses cargo-chef for efficient build caching and distroless for minimal runtime
# ==============================================================================

# ------------------------------------------------------------------------------
# Stage 1: Cargo Chef Planner
# Analyzes the project and prepares a build plan
# ------------------------------------------------------------------------------
FROM rust:slim-bookworm AS chef
RUN cargo install cargo-chef --locked
WORKDIR /app

# ------------------------------------------------------------------------------
# Stage 2: Dependency Planning
# Creates a recipe.json that captures all dependencies
# ------------------------------------------------------------------------------
FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo chef prepare --recipe-path recipe.json

# ------------------------------------------------------------------------------
# Stage 3: Builder
# Builds dependencies first (cached), then the application
# ------------------------------------------------------------------------------
FROM chef AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Build dependencies (this layer is cached unless Cargo.toml/Cargo.lock change)
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Build the application
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Build with optimizations
ARG RUSTFLAGS="-C target-cpu=native -C link-arg=-s"
RUN cargo build --release --features http2

# Strip the binary for smaller size
RUN strip /app/target/release/cdn-engine || true

# ------------------------------------------------------------------------------
# Stage 4: Runtime (Distroless)
# Minimal attack surface with only necessary runtime dependencies
# ------------------------------------------------------------------------------
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime

# Copy the compiled binary
COPY --from=builder --chown=nonroot:nonroot /app/target/release/cdn-engine /usr/local/bin/cdn-engine

# Copy CA certificates for HTTPS connections
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Create cache directory mount point
VOLUME ["/var/cache/cdn"]

# Expose ports
# 8080 - HTTP server
# 8443 - HTTPS server (if enabled)
# 9090 - Metrics endpoint
EXPOSE 8080 8443 9090

# Set environment defaults
ENV CDN_SERVER_HTTP_ADDR="0.0.0.0:8080" \
    CDN_LOG_LEVEL="info" \
    CDN_LOG_FORMAT="json" \
    CDN_CACHE_PATH="/var/cache/cdn"

# Run as non-root user
USER nonroot:nonroot

# Health check endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/cdn-engine", "--help"] || exit 1

# Entry point
ENTRYPOINT ["/usr/local/bin/cdn-engine"]
