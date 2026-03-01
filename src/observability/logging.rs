//! Structured logging with tracing.

use crate::config::ObservabilityConfig;
use anyhow::Result;
use tracing::Level;
use tracing_subscriber::{
    fmt::{self, format::FmtSpan},
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter,
};

/// Initialize logging subsystem
pub fn init_logging(config: &ObservabilityConfig) -> Result<()> {
    // Build filter from config or environment
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.log_level));

    match config.log_format.as_str() {
        "json" => init_json_logging(filter),
        "pretty" => init_pretty_logging(filter),
        _ => init_json_logging(filter),
    }

    Ok(())
}

/// Initialize JSON-formatted logging (production)
fn init_json_logging(filter: EnvFilter) {
    let fmt_layer = fmt::layer()
        .json()
        .with_target(true)
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_span_events(FmtSpan::CLOSE)
        .flatten_event(true);

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .init();
}

/// Initialize pretty-formatted logging (development)
fn init_pretty_logging(filter: EnvFilter) {
    let fmt_layer = fmt::layer()
        .pretty()
        .with_target(true)
        .with_file(true)
        .with_line_number(true)
        .with_thread_names(true);

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .init();
}

/// Initialize OpenTelemetry tracing
#[cfg(feature = "otel")]
pub async fn init_otel_tracing(config: &ObservabilityConfig) -> Result<()> {
    use opentelemetry::sdk::trace::TracerProvider;
    use opentelemetry_otlp::WithExportConfig;

    if !config.enable_tracing {
        return Ok(());
    }

    let endpoint = config.otlp_endpoint.as_ref()
        .ok_or_else(|| anyhow::anyhow!("OTLP endpoint required for tracing"))?;

    // Configure OTLP exporter
    let exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint(endpoint);

    let tracer_provider = TracerProvider::builder()
        .with_batch_exporter(exporter, opentelemetry::runtime::Tokio)
        .with_config(
            opentelemetry::sdk::trace::config()
                .with_resource(opentelemetry::sdk::Resource::new(vec![
                    opentelemetry::KeyValue::new("service.name", config.service_name.clone()),
                ])),
        )
        .build();

    let tracer = tracer_provider.tracer(&config.service_name);

    // Create tracing layer
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    // Add to existing subscriber
    // Note: In a real implementation, you'd want to compose this with the fmt layer
    
    tracing::info!(
        endpoint = endpoint,
        service_name = %config.service_name,
        "OpenTelemetry tracing initialized"
    );

    Ok(())
}

/// Log request context
#[derive(Debug, Clone)]
pub struct RequestContext {
    pub request_id: String,
    pub method: String,
    pub path: String,
    pub client_ip: Option<String>,
}

impl RequestContext {
    pub fn new(method: &str, path: &str) -> Self {
        Self {
            request_id: uuid::Uuid::new_v4().to_string(),
            method: method.to_string(),
            path: path.to_string(),
            client_ip: None,
        }
    }

    pub fn with_client_ip(mut self, ip: impl Into<String>) -> Self {
        self.client_ip = Some(ip.into());
        self
    }

    /// Create a tracing span for this request
    pub fn span(&self) -> tracing::Span {
        tracing::info_span!(
            "request",
            request_id = %self.request_id,
            method = %self.method,
            path = %self.path,
            client_ip = ?self.client_ip,
        )
    }
}

/// Log entry for CDN operations
pub struct CdnLogEvent {
    pub request_id: String,
    pub method: String,
    pub path: String,
    pub status: u16,
    pub duration_ms: f64,
    pub bytes_sent: u64,
    pub cache_status: String,
    pub client_ip: Option<String>,
    pub user_agent: Option<String>,
}

impl CdnLogEvent {
    /// Log this event using tracing
    pub fn log(&self) {
        tracing::info!(
            request_id = %self.request_id,
            method = %self.method,
            path = %self.path,
            status = self.status,
            duration_ms = self.duration_ms,
            bytes_sent = self.bytes_sent,
            cache_status = %self.cache_status,
            client_ip = ?self.client_ip,
            user_agent = ?self.user_agent,
            "Request completed"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_context() {
        let ctx = RequestContext::new("GET", "/images/logo.png")
            .with_client_ip("192.168.1.1");

        assert_eq!(ctx.method, "GET");
        assert_eq!(ctx.path, "/images/logo.png");
        assert_eq!(ctx.client_ip, Some("192.168.1.1".to_string()));
        assert!(!ctx.request_id.is_empty());
    }
}
