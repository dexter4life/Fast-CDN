//! S3 client with multi-cloud compatibility and streaming support.

use crate::config::S3Config;
use crate::types::{BucketName, ByteRange, ContentType, ETag, ObjectKey};
use anyhow::Result;
use aws_credential_types::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as AwsS3Client;
use bytes::Bytes;
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, instrument, warn};

use super::retry::RetryPolicy;

/// S3 object metadata
#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    pub content_type: ContentType,
    pub content_length: u64,
    pub etag: Option<ETag>,
    pub last_modified: Option<chrono::DateTime<chrono::Utc>>,
    pub cache_control: Option<String>,
    pub content_encoding: Option<String>,
    pub custom_metadata: std::collections::HashMap<String, String>,
}

impl ObjectMetadata {
    /// Parse Cache-Control max-age directive
    pub fn max_age(&self) -> Option<Duration> {
        self.cache_control.as_ref().and_then(|cc| {
            cc.split(',')
                .find_map(|directive| {
                    let directive = directive.trim();
                    if directive.starts_with("max-age=") {
                        directive[8..].parse::<u64>().ok().map(Duration::from_secs)
                    } else {
                        None
                    }
                })
        })
    }

    /// Check if response should not be cached
    pub fn is_no_cache(&self) -> bool {
        self.cache_control
            .as_ref()
            .map(|cc| cc.contains("no-cache") || cc.contains("no-store"))
            .unwrap_or(false)
    }

    /// Check for stale-while-revalidate directive
    pub fn stale_while_revalidate(&self) -> Option<Duration> {
        self.cache_control.as_ref().and_then(|cc| {
            cc.split(',')
                .find_map(|directive| {
                    let directive = directive.trim();
                    if directive.starts_with("stale-while-revalidate=") {
                        directive[23..].parse::<u64>().ok().map(Duration::from_secs)
                    } else {
                        None
                    }
                })
        })
    }
}

/// Streaming response from S3
pub struct S3StreamingResponse {
    pub metadata: ObjectMetadata,
    pub stream: S3ByteStream,
}

/// Wrapper for S3 byte stream that implements Stream
pub struct S3ByteStream {
    inner: ByteStream,
}

impl S3ByteStream {
    pub fn new(stream: ByteStream) -> Self {
        Self { inner: stream }
    }
}

impl Stream for S3ByteStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use futures::StreamExt;
        
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => Poll::Ready(Some(Ok(bytes))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("{:?}", e),
            )))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// S3 client error types
#[derive(Debug, thiserror::Error)]
pub enum S3Error {
    #[error("Object not found: {bucket}/{key}")]
    NotFound { bucket: String, key: String },

    #[error("Access denied: {bucket}/{key}")]
    AccessDenied { bucket: String, key: String },

    #[error("Bucket not found: {bucket}")]
    BucketNotFound { bucket: String },

    #[error("Request throttled, retry after {retry_after_ms}ms")]
    Throttled { retry_after_ms: u64 },

    #[error("Circuit breaker open")]
    CircuitBreakerOpen,

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Timeout after {0:?}")]
    Timeout(Duration),

    #[error("Internal S3 error: {0}")]
    Internal(String),

    #[error("Invalid range request")]
    InvalidRange,
}

/// Circuit breaker state
#[derive(Debug, Clone)]
enum CircuitState {
    Closed,
    Open { until: std::time::Instant },
    HalfOpen,
}

/// Circuit breaker for S3 backend
struct CircuitBreaker {
    state: CircuitState,
    failure_count: u32,
    threshold: u32,
    reset_timeout: Duration,
}

impl CircuitBreaker {
    fn new(threshold: u32, reset_timeout: Duration) -> Self {
        Self {
            state: CircuitState::Closed,
            failure_count: 0,
            threshold,
            reset_timeout,
        }
    }

    fn check(&mut self) -> Result<(), S3Error> {
        match &self.state {
            CircuitState::Closed => Ok(()),
            CircuitState::Open { until } => {
                if std::time::Instant::now() >= *until {
                    self.state = CircuitState::HalfOpen;
                    Ok(())
                } else {
                    Err(S3Error::CircuitBreakerOpen)
                }
            }
            CircuitState::HalfOpen => Ok(()),
        }
    }

    fn record_success(&mut self) {
        self.failure_count = 0;
        self.state = CircuitState::Closed;
    }

    fn record_failure(&mut self) {
        self.failure_count += 1;
        if self.failure_count >= self.threshold {
            self.state = CircuitState::Open {
                until: std::time::Instant::now() + self.reset_timeout,
            };
            warn!(
                threshold = self.threshold,
                reset_timeout = ?self.reset_timeout,
                "Circuit breaker opened"
            );
        }
    }
}

/// Production S3 client with retry, circuit breaking, and streaming
pub struct S3Client {
    client: AwsS3Client,
    config: S3Config,
    retry_policy: RetryPolicy,
    circuit_breaker: Arc<Mutex<CircuitBreaker>>,
}

impl S3Client {
    /// Create a new S3 client from configuration
    pub async fn new(config: S3Config) -> Result<Self> {
        let credentials = match (&config.access_key_id, &config.secret_access_key) {
            (Some(key_id), Some(secret)) => Some(Credentials::new(
                key_id.clone(),
                secret.clone(),
                None,
                None,
                "cdn-static-credentials",
            )),
            _ => None,
        };

        let mut sdk_config_builder = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(config.region.clone()));

        if let Some(endpoint) = &config.endpoint {
            sdk_config_builder = sdk_config_builder.endpoint_url(endpoint);
        }

        if let Some(creds) = credentials {
            sdk_config_builder =
                sdk_config_builder.credentials_provider(aws_credential_types::provider::SharedCredentialsProvider::new(creds));
        }

        let sdk_config = sdk_config_builder.load().await;

        let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
            .force_path_style(config.force_path_style)
            .build();

        let client = AwsS3Client::from_conf(s3_config);

        let retry_policy = RetryPolicy::new(
            config.max_retries,
            config.retry_initial_delay,
            config.retry_max_delay,
        );

        let circuit_breaker = Arc::new(Mutex::new(CircuitBreaker::new(
            config.circuit_breaker_threshold,
            config.circuit_breaker_timeout,
        )));

        info!(
            endpoint = ?config.endpoint,
            region = %config.region,
            force_path_style = config.force_path_style,
            "S3 client initialized"
        );

        Ok(Self {
            client,
            config,
            retry_policy,
            circuit_breaker,
        })
    }

    /// Check if circuit breaker allows request
    async fn check_circuit(&self) -> Result<(), S3Error> {
        let mut cb = self.circuit_breaker.lock().await;
        cb.check()
    }

    /// Record success to circuit breaker
    async fn record_success(&self) {
        let mut cb = self.circuit_breaker.lock().await;
        cb.record_success();
    }

    /// Record failure to circuit breaker
    async fn record_failure(&self) {
        let mut cb = self.circuit_breaker.lock().await;
        cb.record_failure();
    }

    /// Get object with optional range request
    #[instrument(skip(self), fields(bucket = %bucket, key = %key))]
    pub async fn get_object(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        range: Option<ByteRange>,
    ) -> Result<S3StreamingResponse, S3Error> {
        self.check_circuit().await?;

        let result = self
            .retry_policy
            .execute(|| self.do_get_object(bucket, key, range.clone()))
            .await;

        match &result {
            Ok(_) => {
                self.record_success().await;
                debug!("S3 GET successful");
            }
            Err(e) => {
                // Don't open circuit for client errors (404, 403)
                if !matches!(e, S3Error::NotFound { .. } | S3Error::AccessDenied { .. }) {
                    self.record_failure().await;
                }
                error!(error = %e, "S3 GET failed");
            }
        }

        result
    }

    /// Internal get object implementation
    async fn do_get_object(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        range: Option<ByteRange>,
    ) -> Result<S3StreamingResponse, S3Error> {
        let mut request = self
            .client
            .get_object()
            .bucket(bucket.as_str())
            .key(key.as_str());

        if let Some(range) = range {
            request = request.range(range.to_header());
        }

        let response = tokio::time::timeout(self.config.read_timeout, request.send())
            .await
            .map_err(|_| S3Error::Timeout(self.config.read_timeout))?
            .map_err(|e| self.map_sdk_error(e, bucket, key))?;

        let metadata = ObjectMetadata {
            content_type: ContentType::new(
                response
                    .content_type()
                    .unwrap_or("application/octet-stream"),
            ),
            content_length: response.content_length().unwrap_or(0) as u64,
            etag: response.e_tag().map(ETag::new),
            last_modified: response.last_modified().and_then(|dt| {
                chrono::DateTime::from_timestamp(dt.secs(), dt.subsec_nanos())
            }),
            cache_control: response.cache_control().map(String::from),
            content_encoding: response.content_encoding().map(String::from),
            custom_metadata: response
                .metadata()
                .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                .unwrap_or_default(),
        };

        Ok(S3StreamingResponse {
            metadata,
            stream: S3ByteStream::new(response.body),
        })
    }

    /// Head object to get metadata without body
    #[instrument(skip(self), fields(bucket = %bucket, key = %key))]
    pub async fn head_object(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<ObjectMetadata, S3Error> {
        self.check_circuit().await?;

        let result = self
            .retry_policy
            .execute(|| self.do_head_object(bucket, key))
            .await;

        match &result {
            Ok(_) => self.record_success().await,
            Err(e) => {
                if !matches!(e, S3Error::NotFound { .. } | S3Error::AccessDenied { .. }) {
                    self.record_failure().await;
                }
            }
        }

        result
    }

    async fn do_head_object(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<ObjectMetadata, S3Error> {
        let response = tokio::time::timeout(
            self.config.connect_timeout,
            self.client
                .head_object()
                .bucket(bucket.as_str())
                .key(key.as_str())
                .send(),
        )
        .await
        .map_err(|_| S3Error::Timeout(self.config.connect_timeout))?
        .map_err(|e| self.map_sdk_error(e, bucket, key))?;

        Ok(ObjectMetadata {
            content_type: ContentType::new(
                response
                    .content_type()
                    .unwrap_or("application/octet-stream"),
            ),
            content_length: response.content_length().unwrap_or(0) as u64,
            etag: response.e_tag().map(ETag::new),
            last_modified: response.last_modified().and_then(|dt| {
                chrono::DateTime::from_timestamp(dt.secs(), dt.subsec_nanos())
            }),
            cache_control: response.cache_control().map(String::from),
            content_encoding: response.content_encoding().map(String::from),
            custom_metadata: response
                .metadata()
                .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                .unwrap_or_default(),
        })
    }

    /// Check if object exists
    pub async fn object_exists(&self, bucket: &BucketName, key: &ObjectKey) -> Result<bool, S3Error> {
        match self.head_object(bucket, key).await {
            Ok(_) => Ok(true),
            Err(S3Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Map AWS SDK errors to our error types
    fn map_sdk_error<E: std::fmt::Display + std::fmt::Debug>(
        &self,
        error: aws_sdk_s3::error::SdkError<E>,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> S3Error {
        use aws_sdk_s3::error::SdkError;

        match error {
            SdkError::ServiceError(service_err) => {
                let raw = service_err.raw();
                let status = raw.status().as_u16();

                match status {
                    404 => S3Error::NotFound {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                    },
                    403 => S3Error::AccessDenied {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                    },
                    416 => S3Error::InvalidRange,
                    429 | 503 => S3Error::Throttled {
                        retry_after_ms: 1000,
                    },
                    500..=599 => S3Error::Internal(format!("{:?}", service_err)),
                    _ => S3Error::Internal(format!("{:?}", service_err)),
                }
            }
            SdkError::TimeoutError(_) => S3Error::Timeout(self.config.read_timeout),
            SdkError::DispatchFailure(e) => S3Error::Connection(format!("{:?}", e)),
            _ => S3Error::Internal(error.to_string()),
        }
    }
}

impl Clone for S3Client {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            config: self.config.clone(),
            retry_policy: self.retry_policy.clone(),
            circuit_breaker: self.circuit_breaker.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_max_age() {
        let metadata = ObjectMetadata {
            content_type: ContentType::new("image/png"),
            content_length: 1024,
            etag: None,
            last_modified: None,
            cache_control: Some("max-age=3600, public".to_string()),
            content_encoding: None,
            custom_metadata: Default::default(),
        };

        assert_eq!(metadata.max_age(), Some(Duration::from_secs(3600)));
    }

    #[test]
    fn test_metadata_no_cache() {
        let metadata = ObjectMetadata {
            content_type: ContentType::new("text/html"),
            content_length: 512,
            etag: None,
            last_modified: None,
            cache_control: Some("no-cache, no-store".to_string()),
            content_encoding: None,
            custom_metadata: Default::default(),
        };

        assert!(metadata.is_no_cache());
    }

    #[test]
    fn test_stale_while_revalidate() {
        let metadata = ObjectMetadata {
            content_type: ContentType::new("image/jpeg"),
            content_length: 2048,
            etag: None,
            last_modified: None,
            cache_control: Some("max-age=300, stale-while-revalidate=600".to_string()),
            content_encoding: None,
            custom_metadata: Default::default(),
        };

        assert_eq!(
            metadata.stale_while_revalidate(),
            Some(Duration::from_secs(600))
        );
    }
}
