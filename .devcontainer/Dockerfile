# CDN Engine Development Container
FROM mcr.microsoft.com/devcontainers/rust:1-bookworm

# Install build dependencies required by the project
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    ca-certificates \
    cmake \
    clang \
    llvm \
    mold \
    && rm -rf /var/lib/apt/lists/*

# Install additional Cargo tools for development
RUN cargo install cargo-watch cargo-audit cargo-outdated cargo-expand

# Set up faster linking with mold
ENV RUSTFLAGS="-C link-arg=-fuse-ld=mold"

# Create cache directory
RUN mkdir -p /var/cache/cdn && chown vscode:vscode /var/cache/cdn
