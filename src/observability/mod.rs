//! Observability module with Prometheus metrics and structured logging.

mod metrics;
mod logging;

pub use metrics::*;
pub use logging::*;
