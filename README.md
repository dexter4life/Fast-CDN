# CDN Engine

A high-performance, production-scale Content Delivery Network (CDN) data plane written in Rust, optimized for S3-compatible backends (AWS S3, MinIO, Cloudflare R2).

## Features

### Core Functionality
- **S3-Compatible Origin Integration** - Works with AWS S3, MinIO, Cloudflare R2, and any S3-compatible storage
- **SigV4 Authentication** - Proper AWS Signature Version 4 signing for secure S3 access
- **Range Request Support** - Efficient video/audio streaming with byte-range requests
- **Virtual & Path-Style URIs** - Support for both S3 addressing schemes

### Performance
- **HTTP/2 Support** - Multiplexed connections for improved performance
- **HTTP/3 (QUIC)** - Optional HTTP/3 support for lower latency (feature flag)
- **Zero-Copy Streaming** - Efficient memory usage with backpressure handling
- **Request Collapsing** - Thundering herd protection for cache misses
- **Two-Tier Caching** - RAM (TinyLFU) + Disk hybrid cache

### Security
- **HMAC URL Signing** - Cryptographic URL validation with expiry
- **Rate Limiting** - Token bucket algorithm with per-IP tracking
- **Distroless Runtime** - Minimal attack surface Docker image

### Observability
- **Prometheus Metrics** - Request counts, latencies, cache hit rates
- **Structured Logging** - JSON logging with trace correlation
- **OpenTelemetry Ready** - Distributed tracing support

## Quick Start

### Using Docker Compose

```bash
# Start CDN with MinIO backend
docker compose up -d

# With Prometheus/Grafana monitoring
docker compose --profile monitoring up -d
```

### Using Cargo

```bash
# Build
cargo build --release --features http2

# Run with config file
./target/release/cdn-engine --config config.yml

# Run with environment variables
CDN_S3_ENDPOINT=http://localhost:9000 \
CDN_S3_ACCESS_KEY_ID=minioadmin \
CDN_S3_SECRET_ACCESS_KEY=minioadmin \
./target/release/cdn-engine
```

## Configuration

Configuration can be provided via:
1. YAML config file (`--config path/to/config.yml`)
2. Environment variables (`CDN_*`)
3. CLI arguments

See [config.example.yml](config.example.yml) for all available options.

### Key Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `CDN_SERVER_HTTP_ADDR` | HTTP listen address | `0.0.0.0:8080` |
| `CDN_S3_ENDPOINT` | S3 endpoint URL | AWS default |
| `CDN_S3_REGION` | AWS region | `us-east-1` |
| `CDN_S3_ACCESS_KEY_ID` | AWS access key | - |
| `CDN_S3_SECRET_ACCESS_KEY` | AWS secret key | - |
| `CDN_S3_DEFAULT_BUCKET` | Default bucket name | - |
| `CDN_CACHE_PATH` | Disk cache directory | `/var/cache/cdn` |
| `CDN_HMAC_SECRET` | HMAC secret (base64) | - |
| `CDN_LOG_LEVEL` | Log level | `info` |

## URL Patterns

### Basic Asset Request
```
GET /bucket-name/path/to/object.jpg
```

### Path-Style with Default Bucket
```
GET /path/to/object.jpg
```

### With Range Headers (Video Streaming)
```
GET /videos/movie.mp4
Range: bytes=0-1048576
```

### Signed URLs (when enabled)
```
GET /assets/secure-file.pdf?exp=1735689600&sig=abc123...
```

### Image Transformations (with feature flag)
```
GET /images/photo.jpg?w=800&h=600&q=85&fmt=webp
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         CDN Engine                              │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │ HTTP Server │  │  Security   │  │     Observability       │  │
│  │ (Hyper/H2)  │  │ Middleware  │  │  (Prometheus/Tracing)   │  │
│  └──────┬──────┘  └──────┬──────┘  └─────────────────────────┘  │
│         │                │                                      │
│  ┌──────▼────────────────▼───────┐                              │
│  │        Request Handler         │                              │
│  └──────────────┬────────────────┘                              │
│                 │                                                │
│  ┌──────────────▼───────────────┐                               │
│  │        Hybrid Cache           │◄── Request Collapsing        │
│  │  ┌─────────┐  ┌────────────┐ │                               │
│  │  │ Memory  │  │   Disk     │ │                               │
│  │  │ (moka)  │  │  (sled)    │ │                               │
│  │  └─────────┘  └────────────┘ │                               │
│  └──────────────┬───────────────┘                               │
│                 │ Cache Miss                                    │
│  ┌──────────────▼───────────────┐                               │
│  │         S3 Client            │                               │
│  │  ┌─────────┐  ┌────────────┐ │                               │
│  │  │ SigV4   │  │   Retry    │ │                               │
│  │  │ Signer  │  │  + Circuit │ │                               │
│  │  └─────────┘  └────────────┘ │                               │
│  └──────────────────────────────┘                               │
└─────────────────────────────────────────────────────────────────┘
                          │
                          ▼
         ┌────────────────────────────────┐
         │   S3-Compatible Storage        │
         │  (AWS / MinIO / R2 / etc.)     │
         └────────────────────────────────┘
```

## Building

### Development Build
```bash
cargo build
```

### Release Build with HTTP/2
```bash
cargo build --release --features http2
```

### Release Build with HTTP/3 (QUIC)
```bash
cargo build --release --features "http2,http3"
```

### Docker Build
```bash
docker build -t cdn-engine .
```

## Metrics

The CDN exposes Prometheus metrics on the configured metrics port (default: 9090):

| Metric | Type | Description |
|--------|------|-------------|
| `cdn_requests_total` | Counter | Total requests by method, status |
| `cdn_request_duration_seconds` | Histogram | Request latency distribution |
| `cdn_bytes_transferred_total` | Counter | Bytes served |
| `cdn_cache_hits_total` | Counter | Cache hits by tier |
| `cdn_cache_misses_total` | Counter | Cache misses |
| `cdn_s3_requests_total` | Counter | S3 backend requests |
| `cdn_s3_errors_total` | Counter | S3 errors by type |
| `cdn_active_connections` | Gauge | Current connections |

## Performance Tuning

### System Settings
```bash
# Increase file descriptor limits
ulimit -n 65535

# Enable TCP optimizations (Linux)
sysctl -w net.core.somaxconn=65535
sysctl -w net.ipv4.tcp_max_syn_backlog=65535
```

### CDN Configuration
- Increase `memory_cache_size_mb` for higher hit rates
- Enable `request_collapsing` for popular content
- Tune `worker_threads` based on CPU cores

## License

MIT License - See LICENSE file for details.

## Contributing

Contributions are welcome! Here's how you can help:

### Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/Fast-CDN.git`
3. Create a feature branch: `git checkout -b feature/your-feature-name`

### Development Setup

```bash
# Install Rust if you haven't already
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Build the project
cargo build

# Run tests
cargo test

# Run with debug logging
RUST_LOG=debug cargo run
```

### Code Style

- Follow Rust's standard formatting (`cargo fmt`)
- Run clippy for linting: `cargo clippy -- -D warnings`
- Write tests for new features

### Submitting Changes

1. Push your changes to your fork
2. Create a Pull Request against the `main` branch
3. Ensure all tests pass and CI checks succeed
4. Update documentation if needed

### Reporting Issues

- Use GitHub Issues for bug reports and feature requests
- Include relevant details like Rust version, OS, and configuration

---

## About

**Fast-CDN** is a high-performance CDN data plane written in Rust, optimized for S3-compatible storage backends. It provides efficient caching, request collapsing, and security features suitable for production workloads.

Built with ❤️ by the open source community.

### Key Highlights

- 🚀 Blazing fast performance with Rust
- 💾 Hybrid memory + disk caching
- 🔒 Enterprise-grade security features
- 📊 Full observability with Prometheus metrics
- 🐳 Docker support for easy deployment
