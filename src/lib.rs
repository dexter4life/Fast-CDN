//! Production-grade CDN engine optimized for S3-compatible backends.
//!
//! # Features
//!
//! - **Multi-cloud S3 support**: Works with AWS S3, MinIO, Cloudflare R2
//! - **Two-tier caching**: RAM (TinyLFU) + NVMe disk cache
//! - **Request collapsing**: Thundering herd protection
//! - **Signed URLs**: HMAC-SHA256 URL validation
//! - **Image transforms**: JIT optimization (resize, format conversion)
//! - **HTTP/2 & HTTP/3**: Modern protocol support
//! - **Zero-copy streaming**: Minimal memory overhead
//! - **Observability**: Prometheus metrics + structured logging
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────┐
//! │                      CDN Engine                              │
//! │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
//! │  │  Security   │──│   Proxy     │──│   Storage   │         │
//! │  │ (HMAC/Rate) │  │  (Handler)  │  │ (S3 Client) │         │
//! │  └─────────────┘  └──────┬──────┘  └─────────────┘         │
//! │                          │                                   │
//! │                   ┌──────┴──────┐                           │
//! │                   │    Cache    │                           │
//! │            ┌──────┴──────┬──────┴──────┐                   │
//! │            │  Memory     │    Disk     │                    │
//! │            │ (TinyLFU)   │  (NVMe)     │                    │
//! │            └─────────────┴─────────────┘                    │
//! └──────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Quick Start
//!
//! ```no_run
//! use cdn_engine::{CdnConfig, CdnServer};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Load configuration
//!     let config = CdnConfig::load(Some("config.toml"))?;
//!     
//!     // Run server
//!     cdn_engine::run(config).await
//! }
//! ```

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]
#![forbid(unsafe_code)]

pub mod cache;
pub mod config;
pub mod observability;
pub mod proxy;
pub mod security;
pub mod storage;
pub mod types;

pub use config::CdnConfig;
pub use proxy::CdnServer;

use anyhow::Result;
use std::sync::Arc;

/// Run the CDN server with the given configuration.
pub async fn run(config: CdnConfig) -> Result<()> {
    // Initialize logging
    observability::init_logging(&config.observability)?;

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        "Starting CDN engine"
    );

    // Validate configuration
    config.validate()?;

    // Initialize S3 client
    let s3_client = storage::S3Client::new(config.s3.clone()).await?;

    // Initialize cache
    let cache = Arc::new(cache::HybridCache::new(config.cache.clone()).await?);

    // Initialize security middleware
    let security = security::SecurityMiddleware::new(config.security.clone())?;

    // Initialize metrics
    let metrics = observability::Metrics::new();

    // Create and run server
    let server = CdnServer::new(config, s3_client, cache, security, metrics).await?;

    server.run().await
}

/// Version information
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Build information
pub fn build_info() -> BuildInfo {
    BuildInfo {
        version: env!("CARGO_PKG_VERSION"),
        git_hash: option_env!("GIT_HASH").unwrap_or("unknown"),
        build_date: option_env!("BUILD_DATE").unwrap_or("unknown"),
        rust_version: option_env!("RUSTC_VERSION").unwrap_or("unknown"),
    }
}

/// Build metadata
#[derive(Debug, Clone)]
pub struct BuildInfo {
    /// Package version
    pub version: &'static str,
    /// Git commit hash
    pub git_hash: &'static str,
    /// Build date
    pub build_date: &'static str,
    /// Rust compiler version
    pub rust_version: &'static str,
}
