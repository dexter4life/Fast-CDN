//! CDN Engine - Production-grade CDN for S3-compatible backends.

use anyhow::Result;
use cdn_engine::{CdnConfig, build_info};
use clap::Parser;
use std::path::PathBuf;

/// CDN Engine - High-performance content delivery
#[derive(Parser, Debug)]
#[command(name = "cdn-engine")]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to configuration file
    #[arg(short, long, env = "CDN_CONFIG")]
    config: Option<PathBuf>,

    /// HTTP listen address
    #[arg(long, env = "CDN_SERVER_HTTP_ADDR")]
    http_addr: Option<String>,

    /// S3 endpoint URL
    #[arg(long, env = "CDN_S3_ENDPOINT")]
    s3_endpoint: Option<String>,

    /// S3 region
    #[arg(long, env = "CDN_S3_REGION", default_value = "us-east-1")]
    s3_region: String,

    /// S3 access key ID
    #[arg(long, env = "CDN_S3_ACCESS_KEY_ID")]
    s3_access_key: Option<String>,

    /// S3 secret access key
    #[arg(long, env = "CDN_S3_SECRET_ACCESS_KEY")]
    s3_secret_key: Option<String>,

    /// Default S3 bucket
    #[arg(long, env = "CDN_S3_DEFAULT_BUCKET")]
    s3_bucket: Option<String>,

    /// Cache directory
    #[arg(long, env = "CDN_CACHE_PATH")]
    cache_path: Option<PathBuf>,

    /// HMAC secret key (base64)
    #[arg(long, env = "CDN_HMAC_SECRET")]
    hmac_secret: Option<String>,

    /// Log level
    #[arg(long, env = "CDN_LOG_LEVEL", default_value = "info")]
    log_level: String,

    /// Log format (json or pretty)
    #[arg(long, env = "CDN_LOG_FORMAT", default_value = "json")]
    log_format: String,

    /// Print version and exit
    #[arg(long)]
    build_info: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env file if present
    dotenvy::dotenv().ok();

    let args = Args::parse();

    // Print build info if requested
    if args.build_info {
        let info = build_info();
        println!("CDN Engine {}", info.version);
        println!("  Git hash:     {}", info.git_hash);
        println!("  Build date:   {}", info.build_date);
        println!("  Rust version: {}", info.rust_version);
        return Ok(());
    }

    // Load configuration
    let mut config = match &args.config {
        Some(path) => CdnConfig::load(Some(path.to_str().unwrap()))?,
        None => CdnConfig::default(),
    };

    // Apply CLI overrides
    if let Some(ref addr) = args.http_addr {
        config.server.http_addr = addr.clone();
    }
    if let Some(ref endpoint) = args.s3_endpoint {
        config.s3.endpoint = Some(endpoint.clone());
    }
    config.s3.region = args.s3_region;
    if let Some(ref key) = args.s3_access_key {
        config.s3.access_key_id = Some(key.clone());
    }
    if let Some(ref secret) = args.s3_secret_key {
        config.s3.secret_access_key = Some(secret.clone());
    }
    if let Some(ref bucket) = args.s3_bucket {
        config.s3.default_bucket = Some(bucket.clone());
    }
    if let Some(ref path) = args.cache_path {
        config.cache.disk_cache_path = path.clone();
    }
    if let Some(ref secret) = args.hmac_secret {
        config.security.hmac_secret = Some(secret.clone());
    }
    config.observability.log_level = args.log_level;
    config.observability.log_format = args.log_format;

    // Run the CDN server
    cdn_engine::run(config).await
}
