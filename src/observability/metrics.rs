//! Prometheus metrics for CDN observability.

use once_cell::sync::Lazy;
use prometheus::{
    Counter, CounterVec, Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, Opts,
    Registry, TextEncoder,
};
use std::sync::Arc;
use std::time::Duration;

/// Metric labels
const PATH_LABEL: &str = "path";
const STATUS_LABEL: &str = "status";
const BUCKET_LABEL: &str = "bucket";
const CACHE_TIER_LABEL: &str = "tier";

/// Global metrics registry
static METRICS_REGISTRY: Lazy<Registry> = Lazy::new(|| {
    let registry = Registry::new();
    
    // Register process metrics
    if let Err(e) = registry.register(Box::new(prometheus::process_collector::ProcessCollector::for_self())) {
        tracing::warn!(error = %e, "Failed to register process collector");
    }
    
    registry
});

/// CDN metrics collection
#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    // Request metrics
    requests_total: CounterVec,
    request_duration: HistogramVec,
    
    // S3 metrics
    s3_requests_total: CounterVec,
    s3_latency: HistogramVec,
    s3_errors: CounterVec,
    
    // Cache metrics
    cache_hits: CounterVec,
    cache_misses: CounterVec,
    cache_size_bytes: GaugeVec,
    cache_entries: GaugeVec,
    
    // Transfer metrics
    bytes_proxied_total: Counter,
    bytes_cached_total: Counter,
    
    // Request collapsing metrics
    collapsed_requests: Counter,
    
    // Connection metrics
    active_connections: Gauge,
}

impl Metrics {
    /// Create and register metrics
    pub fn new() -> Self {
        let inner = Arc::new(MetricsInner::new());
        Self { inner }
    }

    /// Record an HTTP request
    pub fn record_request(&self, path: &str, status: u16, duration: Duration) {
        let path_label = normalize_path(path);
        let status_label = status.to_string();

        self.inner
            .requests_total
            .with_label_values(&[&path_label, &status_label])
            .inc();

        self.inner
            .request_duration
            .with_label_values(&[&path_label])
            .observe(duration.as_secs_f64());
    }

    /// Record an S3 request
    pub fn record_s3_request(&self, bucket: &str, duration: Duration, success: bool) {
        let status = if success { "success" } else { "error" };

        self.inner
            .s3_requests_total
            .with_label_values(&[bucket, status])
            .inc();

        self.inner
            .s3_latency
            .with_label_values(&[bucket])
            .observe(duration.as_secs_f64() * 1000.0); // Convert to milliseconds
    }

    /// Record S3 error by type
    pub fn record_s3_error(&self, bucket: &str, error_type: &str) {
        self.inner
            .s3_errors
            .with_label_values(&[bucket, error_type])
            .inc();
    }

    /// Record cache hit
    pub fn record_cache_hit(&self) {
        self.inner
            .cache_hits
            .with_label_values(&["memory"])
            .inc();
    }

    /// Record cache hit with tier
    pub fn record_cache_hit_tier(&self, tier: &str) {
        self.inner.cache_hits.with_label_values(&[tier]).inc();
    }

    /// Record cache miss
    pub fn record_cache_miss(&self) {
        self.inner
            .cache_misses
            .with_label_values(&["memory"])
            .inc();
    }

    /// Update cache size metrics
    pub fn update_cache_size(&self, tier: &str, size_bytes: u64, entries: u64) {
        self.inner
            .cache_size_bytes
            .with_label_values(&[tier])
            .set(size_bytes as f64);

        self.inner
            .cache_entries
            .with_label_values(&[tier])
            .set(entries as f64);
    }

    /// Record bytes proxied from origin
    pub fn record_bytes_proxied(&self, bytes: u64) {
        self.inner.bytes_proxied_total.inc_by(bytes as f64);
    }

    /// Record bytes served from cache
    pub fn record_bytes_cached(&self, bytes: u64) {
        self.inner.bytes_cached_total.inc_by(bytes as f64);
    }

    /// Record a collapsed request
    pub fn record_collapsed_request(&self) {
        self.inner.collapsed_requests.inc();
    }

    /// Set active connection count
    pub fn set_active_connections(&self, count: i64) {
        self.inner.active_connections.set(count as f64);
    }

    /// Increment active connections
    pub fn inc_active_connections(&self) {
        self.inner.active_connections.inc();
    }

    /// Decrement active connections
    pub fn dec_active_connections(&self) {
        self.inner.active_connections.dec();
    }

    /// Gather all metrics as Prometheus text format
    pub fn gather(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = METRICS_REGISTRY.gather();
        
        let mut buffer = Vec::new();
        if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
            tracing::error!(error = %e, "Failed to encode metrics");
            return String::new();
        }

        String::from_utf8(buffer).unwrap_or_default()
    }

    /// Calculate cache hit ratio
    pub fn cache_hit_ratio(&self) -> f64 {
        let hits = self.inner.cache_hits.with_label_values(&["memory"]).get();
        let misses = self.inner.cache_misses.with_label_values(&["memory"]).get();
        let total = hits + misses;

        if total > 0.0 {
            hits / total
        } else {
            0.0
        }
    }
}

impl MetricsInner {
    fn new() -> Self {
        // Request metrics
        let requests_total = CounterVec::new(
            Opts::new("cdn_requests_total", "Total number of HTTP requests"),
            &[PATH_LABEL, STATUS_LABEL],
        )
        .expect("Failed to create requests_total metric");

        let request_duration = HistogramVec::new(
            HistogramOpts::new("cdn_request_duration_seconds", "HTTP request duration")
                .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
            &[PATH_LABEL],
        )
        .expect("Failed to create request_duration metric");

        // S3 metrics
        let s3_requests_total = CounterVec::new(
            Opts::new("cdn_s3_requests_total", "Total S3 requests"),
            &[BUCKET_LABEL, STATUS_LABEL],
        )
        .expect("Failed to create s3_requests_total metric");

        let s3_latency = HistogramVec::new(
            HistogramOpts::new("cdn_s3_latency_ms", "S3 request latency in milliseconds")
                .buckets(vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0]),
            &[BUCKET_LABEL],
        )
        .expect("Failed to create s3_latency metric");

        let s3_errors = CounterVec::new(
            Opts::new("cdn_s3_errors_total", "Total S3 errors by type"),
            &[BUCKET_LABEL, "error_type"],
        )
        .expect("Failed to create s3_errors metric");

        // Cache metrics
        let cache_hits = CounterVec::new(
            Opts::new("cdn_cache_hits_total", "Cache hits"),
            &[CACHE_TIER_LABEL],
        )
        .expect("Failed to create cache_hits metric");

        let cache_misses = CounterVec::new(
            Opts::new("cdn_cache_misses_total", "Cache misses"),
            &[CACHE_TIER_LABEL],
        )
        .expect("Failed to create cache_misses metric");

        let cache_size_bytes = GaugeVec::new(
            Opts::new("cdn_cache_size_bytes", "Current cache size in bytes"),
            &[CACHE_TIER_LABEL],
        )
        .expect("Failed to create cache_size_bytes metric");

        let cache_entries = GaugeVec::new(
            Opts::new("cdn_cache_entries", "Current number of cache entries"),
            &[CACHE_TIER_LABEL],
        )
        .expect("Failed to create cache_entries metric");

        // Transfer metrics
        let bytes_proxied_total = Counter::new(
            "cdn_bytes_proxied_total",
            "Total bytes proxied from origin",
        )
        .expect("Failed to create bytes_proxied_total metric");

        let bytes_cached_total = Counter::new(
            "cdn_bytes_cached_total",
            "Total bytes served from cache",
        )
        .expect("Failed to create bytes_cached_total metric");

        // Request collapsing
        let collapsed_requests = Counter::new(
            "cdn_collapsed_requests_total",
            "Number of requests that waited for a collapsed request",
        )
        .expect("Failed to create collapsed_requests metric");

        // Connection metrics
        let active_connections = Gauge::new(
            "cdn_active_connections",
            "Current number of active connections",
        )
        .expect("Failed to create active_connections metric");

        // Register all metrics
        let registry = &*METRICS_REGISTRY;
        
        registry.register(Box::new(requests_total.clone())).ok();
        registry.register(Box::new(request_duration.clone())).ok();
        registry.register(Box::new(s3_requests_total.clone())).ok();
        registry.register(Box::new(s3_latency.clone())).ok();
        registry.register(Box::new(s3_errors.clone())).ok();
        registry.register(Box::new(cache_hits.clone())).ok();
        registry.register(Box::new(cache_misses.clone())).ok();
        registry.register(Box::new(cache_size_bytes.clone())).ok();
        registry.register(Box::new(cache_entries.clone())).ok();
        registry.register(Box::new(bytes_proxied_total.clone())).ok();
        registry.register(Box::new(bytes_cached_total.clone())).ok();
        registry.register(Box::new(collapsed_requests.clone())).ok();
        registry.register(Box::new(active_connections.clone())).ok();

        Self {
            requests_total,
            request_duration,
            s3_requests_total,
            s3_latency,
            s3_errors,
            cache_hits,
            cache_misses,
            cache_size_bytes,
            cache_entries,
            bytes_proxied_total,
            bytes_cached_total,
            collapsed_requests,
            active_connections,
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Normalize path for metrics (avoid high cardinality)
fn normalize_path(path: &str) -> String {
    // Keep only the first two path segments
    let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).take(2).collect();
    
    if parts.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", parts.join("/"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_path() {
        assert_eq!(normalize_path("/"), "/");
        assert_eq!(normalize_path("/health"), "/health");
        assert_eq!(normalize_path("/bucket/key"), "/bucket/key");
        assert_eq!(normalize_path("/bucket/path/to/file.png"), "/bucket/path");
    }

    #[test]
    fn test_metrics_creation() {
        let metrics = Metrics::new();
        
        metrics.record_request("/test", 200, Duration::from_millis(50));
        metrics.record_cache_hit();
        metrics.record_cache_miss();
        
        let output = metrics.gather();
        assert!(output.contains("cdn_requests_total"));
        assert!(output.contains("cdn_cache_hits_total"));
    }

    #[test]
    fn test_cache_hit_ratio() {
        let metrics = Metrics::new();
        
        // Initially should be 0
        assert_eq!(metrics.cache_hit_ratio(), 0.0);
        
        // Add some hits and misses
        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_miss();
        
        // Should be ~0.66
        let ratio = metrics.cache_hit_ratio();
        assert!(ratio > 0.6 && ratio < 0.7);
    }
}
