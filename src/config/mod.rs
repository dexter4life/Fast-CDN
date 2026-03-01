//! Configuration module for CDN engine.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Main configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdnConfig {
    pub server: ServerConfig,
    pub s3: S3Config,
    pub cache: CacheConfig,
    pub security: SecurityConfig,
    pub observability: ObservabilityConfig,
}

impl Default for CdnConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            s3: S3Config::default(),
            cache: CacheConfig::default(),
            security: SecurityConfig::default(),
            observability: ObservabilityConfig::default(),
        }
    }
}

/// HTTP server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Listen address for HTTP/1.1 and HTTP/2
    pub http_addr: String,
    
    /// Listen address for HTTP/3 (QUIC)
    pub http3_addr: Option<String>,
    
    /// TLS certificate path
    pub tls_cert: Option<PathBuf>,
    
    /// TLS private key path
    pub tls_key: Option<PathBuf>,
    
    /// Maximum concurrent connections
    pub max_connections: usize,
    
    /// Connection keep-alive timeout
    #[serde(with = "humantime_serde")]
    pub keep_alive_timeout: Duration,
    
    /// Request body size limit
    pub max_body_size: usize,
    
    /// Enable HTTP/2
    pub enable_http2: bool,
    
    /// Enable HTTP/3 (QUIC)
    pub enable_http3: bool,
    
    /// Graceful shutdown timeout
    #[serde(with = "humantime_serde")]
    pub shutdown_timeout: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            http_addr: "0.0.0.0:8080".to_string(),
            http3_addr: Some("0.0.0.0:8443".to_string()),
            tls_cert: None,
            tls_key: None,
            max_connections: 10000,
            keep_alive_timeout: Duration::from_secs(60),
            max_body_size: 10 * 1024 * 1024, // 10MB
            enable_http2: true,
            enable_http3: true,
            shutdown_timeout: Duration::from_secs(30),
        }
    }
}

/// S3 backend configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    /// S3 endpoint URL (for MinIO, R2, etc.)
    pub endpoint: Option<String>,
    
    /// AWS region
    pub region: String,
    
    /// AWS access key ID
    pub access_key_id: Option<String>,
    
    /// AWS secret access key
    pub secret_access_key: Option<String>,
    
    /// Default bucket name
    pub default_bucket: Option<String>,
    
    /// Use path-style addressing (for MinIO)
    pub force_path_style: bool,
    
    /// Connection timeout
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
    
    /// Read timeout
    #[serde(with = "humantime_serde")]
    pub read_timeout: Duration,
    
    /// Maximum retries
    pub max_retries: u32,
    
    /// Initial retry delay
    #[serde(with = "humantime_serde")]
    pub retry_initial_delay: Duration,
    
    /// Maximum retry delay
    #[serde(with = "humantime_serde")]
    pub retry_max_delay: Duration,
    
    /// Circuit breaker failure threshold
    pub circuit_breaker_threshold: u32,
    
    /// Circuit breaker reset timeout
    #[serde(with = "humantime_serde")]
    pub circuit_breaker_timeout: Duration,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            endpoint: None,
            region: "us-east-1".to_string(),
            access_key_id: None,
            secret_access_key: None,
            default_bucket: None,
            force_path_style: false,
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(30),
            max_retries: 3,
            retry_initial_delay: Duration::from_millis(100),
            retry_max_delay: Duration::from_secs(10),
            circuit_breaker_threshold: 5,
            circuit_breaker_timeout: Duration::from_secs(30),
        }
    }
}

/// Cache configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Enable memory cache (hot tier)
    pub enable_memory_cache: bool,
    
    /// Memory cache size in bytes
    pub memory_cache_size: usize,
    
    /// Maximum item size for memory cache
    pub memory_max_item_size: usize,
    
    /// Enable disk cache (cold tier)
    pub enable_disk_cache: bool,
    
    /// Disk cache directory
    pub disk_cache_path: PathBuf,
    
    /// Disk cache size limit in bytes
    pub disk_cache_size: u64,
    
    /// Maximum item size for disk cache
    pub disk_max_item_size: u64,
    
    /// Default TTL for cached items
    #[serde(with = "humantime_serde")]
    pub default_ttl: Duration,
    
    /// Honor S3 Cache-Control headers
    pub honor_cache_control: bool,
    
    /// Enable stale-while-revalidate
    pub stale_while_revalidate: bool,
    
    /// Stale-while-revalidate max age
    #[serde(with = "humantime_serde")]
    pub stale_max_age: Duration,
    
    /// Enable request collapsing (thundering herd protection)
    pub enable_request_collapsing: bool,
    
    /// Request collapse timeout
    #[serde(with = "humantime_serde")]
    pub collapse_timeout: Duration,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enable_memory_cache: true,
            memory_cache_size: 512 * 1024 * 1024, // 512MB
            memory_max_item_size: 10 * 1024 * 1024, // 10MB
            enable_disk_cache: true,
            disk_cache_path: PathBuf::from("/var/cache/cdn"),
            disk_cache_size: 50 * 1024 * 1024 * 1024, // 50GB
            disk_max_item_size: 1024 * 1024 * 1024, // 1GB
            default_ttl: Duration::from_secs(3600),
            honor_cache_control: true,
            stale_while_revalidate: true,
            stale_max_age: Duration::from_secs(86400),
            enable_request_collapsing: true,
            collapse_timeout: Duration::from_secs(30),
        }
    }
}

/// Security configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    /// Enable signed URL validation
    pub enable_signed_urls: bool,
    
    /// HMAC secret key (base64 encoded)
    pub hmac_secret: Option<String>,
    
    /// URL signature parameter name
    pub signature_param: String,
    
    /// Expiry timestamp parameter name
    pub expires_param: String,
    
    /// Allow unsigned requests for certain paths
    pub unsigned_paths: Vec<String>,
    
    /// Maximum URL expiry time
    #[serde(with = "humantime_serde")]
    pub max_expiry_time: Duration,
    
    /// Allowed origins for CORS
    pub cors_origins: Vec<String>,
    
    /// Enable rate limiting
    pub enable_rate_limiting: bool,
    
    /// Rate limit per IP (requests per second)
    pub rate_limit_rps: u32,
    
    /// Rate limit burst size
    pub rate_limit_burst: u32,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            enable_signed_urls: true,
            hmac_secret: None,
            signature_param: "token".to_string(),
            expires_param: "expires".to_string(),
            unsigned_paths: vec!["/health".to_string(), "/metrics".to_string()],
            max_expiry_time: Duration::from_secs(86400 * 7), // 7 days
            cors_origins: vec!["*".to_string()],
            enable_rate_limiting: true,
            rate_limit_rps: 1000,
            rate_limit_burst: 5000,
        }
    }
}

/// Observability configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    /// Log level
    pub log_level: String,
    
    /// Log format (json or pretty)
    pub log_format: String,
    
    /// Enable Prometheus metrics
    pub enable_metrics: bool,
    
    /// Metrics endpoint path
    pub metrics_path: String,
    
    /// Enable OpenTelemetry tracing
    pub enable_tracing: bool,
    
    /// OTLP endpoint for traces
    pub otlp_endpoint: Option<String>,
    
    /// Service name for tracing
    pub service_name: String,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            log_level: "info".to_string(),
            log_format: "json".to_string(),
            enable_metrics: true,
            metrics_path: "/metrics".to_string(),
            enable_tracing: false,
            otlp_endpoint: None,
            service_name: "cdn-engine".to_string(),
        }
    }
}

impl CdnConfig {
    /// Load configuration from file and environment
    pub fn load(path: Option<&str>) -> anyhow::Result<Self> {
        let mut builder = config::Config::builder();

        // Load from file if specified
        if let Some(path) = path {
            builder = builder.add_source(config::File::with_name(path));
        }

        // Override with environment variables
        builder = builder.add_source(
            config::Environment::with_prefix("CDN")
                .separator("__")
                .try_parsing(true),
        );

        let config = builder.build()?;
        let cdn_config: CdnConfig = config.try_deserialize()?;

        Ok(cdn_config)
    }

    /// Validate configuration
    pub fn validate(&self) -> anyhow::Result<()> {
        // Validate HMAC secret if signed URLs are enabled
        if self.security.enable_signed_urls && self.security.hmac_secret.is_none() {
            anyhow::bail!("HMAC secret is required when signed URLs are enabled");
        }

        // Validate cache paths
        if self.cache.enable_disk_cache && !self.cache.disk_cache_path.exists() {
            std::fs::create_dir_all(&self.cache.disk_cache_path)?;
        }

        // Validate TLS configuration
        if self.server.http3_addr.is_some() && self.server.enable_http3 {
            if self.server.tls_cert.is_none() || self.server.tls_key.is_none() {
                anyhow::bail!("TLS certificate and key are required for HTTP/3");
            }
        }

        Ok(())
    }
}

mod humantime_serde {
    use serde::{self, Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&humantime::format_duration(*duration).to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        humantime::parse_duration(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = CdnConfig::default();
        assert_eq!(config.server.http_addr, "0.0.0.0:8080");
        assert!(config.cache.enable_memory_cache);
        assert!(config.security.enable_signed_urls);
    }
}
