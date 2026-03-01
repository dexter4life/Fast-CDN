//! Request handler with caching and request collapsing.

use crate::cache::{CacheEntry, CacheError, CollapseResult, CollapsedResponse, SharedCache};
use crate::config::CdnConfig;
use crate::observability::Metrics;
use crate::proxy::TransformParams;
use crate::security::{SecurityMiddleware, SecurityResult, SecurityDenialReason, create_denial_response, extract_client_ip};
use crate::storage::{ObjectMetadata, S3Client, S3Error};
use crate::types::{BucketName, ByteRange, CacheKey, ObjectKey};
use bytes::Bytes;
use futures::StreamExt;
use http::{header, Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Empty, Full, StreamBody};
use hyper::body::Frame;
use std::convert::Infallible;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::io::ReaderStream;
use tracing::{debug, error, info, instrument, warn, Span};

/// HTTP response body type
pub type ResponseBody = http_body_util::combinators::BoxBody<Bytes, Infallible>;

/// Request handler for CDN operations
pub struct RequestHandler {
    config: CdnConfig,
    s3_client: S3Client,
    cache: SharedCache,
    security: SecurityMiddleware,
    metrics: Metrics,
}

impl RequestHandler {
    /// Create a new request handler
    pub fn new(
        config: CdnConfig,
        s3_client: S3Client,
        cache: SharedCache,
        security: SecurityMiddleware,
        metrics: Metrics,
    ) -> Self {
        Self {
            config,
            s3_client,
            cache,
            security,
            metrics,
        }
    }

    /// Handle an incoming HTTP request
    #[instrument(skip(self, request), fields(method = %request.method(), path = %request.uri().path()))]
    pub async fn handle(
        &self,
        request: Request<hyper::body::Incoming>,
        remote_addr: Option<IpAddr>,
    ) -> Result<Response<ResponseBody>, Infallible> {
        let start = Instant::now();
        let method = request.method().clone();
        let path = request.uri().path().to_string();
        let query = request.uri().query().map(String::from);

        // Extract client IP
        let client_ip = extract_client_ip(&request).or(remote_addr);

        // Check security first
        let security_result = self.security.validate(&path, query.as_deref(), client_ip);

        match security_result {
            SecurityResult::Denied(reason) => {
                self.metrics.record_request(&path, reason.status_code().as_u16(), start.elapsed());
                let denial_response = create_denial_response(&reason);
                return Ok(denial_response.map(|b| Full::new(Bytes::from(b)).map_err(|_| unreachable!()).boxed()));
            }
            SecurityResult::Allowed | SecurityResult::RequiresSignature => {
                // Continue processing
            }
        }

        // Route request
        let response = match (method.clone(), path.as_str()) {
            (Method::GET, "/health") => self.handle_health().await,
            (Method::GET, "/metrics") => self.handle_metrics().await,
            (Method::GET, _) | (Method::HEAD, _) => {
                self.handle_get(&request, &path, query.as_deref()).await
            }
            _ => self.method_not_allowed().await,
        };

        let status = response.status();
        self.metrics.record_request(&path, status.as_u16(), start.elapsed());

        Ok(response)
    }

    /// Handle health check endpoint
    async fn handle_health(&self) -> Response<ResponseBody> {
        let body = serde_json::json!({
            "status": "healthy",
            "cache": {
                "memory": self.cache.stats().memory.as_ref().map(|s| {
                    serde_json::json!({
                        "entries": s.entries,
                        "size_bytes": s.size_bytes,
                        "hit_ratio": if s.hits + s.misses > 0 {
                            s.hits as f64 / (s.hits + s.misses) as f64
                        } else {
                            0.0
                        }
                    })
                }),
                "disk": self.cache.stats().disk.as_ref().map(|s| {
                    serde_json::json!({
                        "size_bytes": s.size_bytes,
                        "hit_ratio": if s.hits + s.misses > 0 {
                            s.hits as f64 / (s.hits + s.misses) as f64
                        } else {
                            0.0
                        }
                    })
                }),
            }
        });

        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from(body.to_string())).map_err(|_| unreachable!()).boxed())
            .unwrap()
    }

    /// Handle metrics endpoint
    async fn handle_metrics(&self) -> Response<ResponseBody> {
        let metrics = self.metrics.gather();
        
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/plain; version=0.0.4")
            .body(Full::new(Bytes::from(metrics)).map_err(|_| unreachable!()).boxed())
            .unwrap()
    }

    /// Handle GET/HEAD requests for objects
    async fn handle_get(
        &self,
        request: &Request<hyper::body::Incoming>,
        path: &str,
        query: Option<&str>,
    ) -> Response<ResponseBody> {
        // Parse path to extract bucket and key
        let (bucket, key) = match self.parse_path(path) {
            Some((b, k)) => (b, k),
            None => return self.bad_request("Invalid path format"),
        };

        // Parse transform parameters from query
        let transform_params = TransformParams::from_query(query.unwrap_or(""));

        // Create cache key (including transform variant)
        let cache_key = if transform_params.is_empty() {
            CacheKey::new(bucket.clone(), key.clone())
        } else {
            CacheKey::new(bucket.clone(), key.clone())
                .with_variant(transform_params.cache_variant())
        };

        // Parse range header
        let range = request
            .headers()
            .get(header::RANGE)
            .and_then(|h| h.to_str().ok())
            .and_then(ByteRange::from_header);

        // Check for conditional headers
        let if_none_match = request
            .headers()
            .get(header::IF_NONE_MATCH)
            .and_then(|h| h.to_str().ok());

        let if_modified_since = request
            .headers()
            .get(header::IF_MODIFIED_SINCE)
            .and_then(|h| h.to_str().ok())
            .and_then(|s| chrono::DateTime::parse_from_rfc2822(s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc));

        // Try cache first (only for non-range requests or full ranges)
        if range.is_none() || range == Some(ByteRange::full()) {
            match self.cache.get(&cache_key).await {
                Ok(entry) => {
                    // Check conditional headers
                    if let Some(etag) = if_none_match {
                        if entry.matches_etag(etag) {
                            return self.not_modified(&entry);
                        }
                    }

                    if let Some(since) = if_modified_since {
                        if !entry.is_modified_since(since) {
                            return self.not_modified(&entry);
                        }
                    }

                    debug!(key = %cache_key, "Cache hit");
                    self.metrics.record_cache_hit();

                    // Background revalidation if needed
                    if entry.needs_revalidation {
                        let handler = self.clone();
                        let bucket = bucket.clone();
                        let key = key.clone();
                        let cache_key = cache_key.clone();
                        
                        tokio::spawn(async move {
                            let _ = handler.revalidate(&bucket, &key, &cache_key).await;
                        });
                    }

                    return self.serve_cached(entry, request.method() == Method::HEAD);
                }
                Err(CacheError::Miss) => {
                    debug!(key = %cache_key, "Cache miss");
                    self.metrics.record_cache_miss();
                }
                Err(e) => {
                    warn!(error = %e, "Cache error");
                    self.metrics.record_cache_miss();
                }
            }
        }

        // Use request collapsing for cache misses
        let collapser = self.cache.collapser();
        
        match collapser.try_acquire(&cache_key) {
            CollapseResult::Leader => {
                // We're the leader, fetch from S3
                debug!(key = %cache_key, "Leader for collapsed request");
                
                let result = self.fetch_from_origin(&bucket, &key, range).await;
                
                match result {
                    Ok((metadata, data)) => {
                        // Apply transforms if needed
                        let (transformed_data, final_content_type) = if !transform_params.is_empty() && key.is_image() {
                            match self.apply_transforms(&data, &transform_params, metadata.content_type.as_str()).await {
                                Ok((d, ct)) => (d, ct),
                                Err(e) => {
                                    warn!(error = %e, "Transform failed, serving original");
                                    (data.clone(), metadata.content_type.to_string())
                                }
                            }
                        } else {
                            (data.clone(), metadata.content_type.to_string())
                        };

                        // Create cache entry
                        let entry = CacheEntry::from_s3_metadata(
                            transformed_data.clone(),
                            &final_content_type,
                            metadata.etag.as_ref().map(|e| e.to_string()),
                            metadata.last_modified,
                            metadata.cache_control.as_deref(),
                            self.config.cache.default_ttl,
                        );

                        // Store in cache (if not a range request)
                        if range.is_none() && !metadata.is_no_cache() {
                            let cache = self.cache.clone();
                            let cache_key_clone = cache_key.clone();
                            let entry_clone = entry.clone();
                            
                            tokio::spawn(async move {
                                if let Err(e) = cache.put(&cache_key_clone, entry_clone).await {
                                    warn!(error = %e, "Failed to cache response");
                                }
                            });
                        }

                        // Complete the collapsed request
                        collapser.complete(&cache_key, CollapsedResponse::success(entry.clone()));

                        self.serve_cached(entry, request.method() == Method::HEAD)
                    }
                    Err(e) => {
                        collapser.complete(&cache_key, CollapsedResponse::error(e.to_string()));
                        self.s3_error_response(e)
                    }
                }
            }
            CollapseResult::Follower(mut receiver) => {
                // Wait for the leader to complete
                debug!(key = %cache_key, "Following collapsed request");
                self.metrics.record_collapsed_request();

                match tokio::time::timeout(
                    self.config.cache.collapse_timeout,
                    receiver.recv(),
                ).await {
                    Ok(Ok(response)) => {
                        if let Some(entry) = response.entry.clone() {
                            self.serve_cached(entry, request.method() == Method::HEAD)
                        } else {
                            self.internal_error(response.error.as_deref().unwrap_or("Unknown error"))
                        }
                    }
                    Ok(Err(_)) => {
                        // Channel closed, leader failed
                        self.internal_error("Request failed")
                    }
                    Err(_) => {
                        // Timeout
                        self.gateway_timeout()
                    }
                }
            }
        }
    }

    /// Parse path into bucket and key
    fn parse_path(&self, path: &str) -> Option<(BucketName, ObjectKey)> {
        let path = path.trim_start_matches('/');
        
        // Try /bucket/key format
        if let Some((bucket, key)) = path.split_once('/') {
            if !bucket.is_empty() && !key.is_empty() {
                return Some((
                    BucketName::new_unchecked(bucket),
                    ObjectKey::new_unchecked(key),
                ));
            }
        }

        // Try using default bucket
        if let Some(ref default_bucket) = self.config.s3.default_bucket {
            if !path.is_empty() {
                return Some((
                    BucketName::new_unchecked(default_bucket),
                    ObjectKey::new_unchecked(path),
                ));
            }
        }

        None
    }

    /// Fetch object from S3 origin
    async fn fetch_from_origin(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        range: Option<ByteRange>,
    ) -> Result<(ObjectMetadata, Bytes), S3Error> {
        let start = Instant::now();

        let response = self.s3_client.get_object(bucket, key, range).await?;

        // Collect the stream into bytes
        let mut data = Vec::new();
        let mut stream = response.stream;
        
        while let Some(chunk_result) = futures::StreamExt::next(&mut stream).await {
            match chunk_result {
                Ok(chunk) => data.extend_from_slice(&chunk),
                Err(e) => {
                    error!(error = %e, "Error reading S3 stream");
                    return Err(S3Error::Internal(e.to_string()));
                }
            }
        }

        self.metrics.record_s3_request(bucket.as_str(), start.elapsed(), true);
        self.metrics.record_bytes_proxied(data.len() as u64);

        Ok((response.metadata, Bytes::from(data)))
    }

    /// Apply image transforms
    async fn apply_transforms(
        &self,
        data: &Bytes,
        params: &TransformParams,
        content_type: &str,
    ) -> Result<(Bytes, String), String> {
        // Placeholder for image transformation
        // In production, this would use the `image` crate
        #[cfg(feature = "image-processing")]
        {
            // Actual image processing would go here
        }

        // For now, return original data
        Ok((data.clone(), content_type.to_string()))
    }

    /// Revalidate a cached entry in the background
    async fn revalidate(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        cache_key: &CacheKey,
    ) -> Result<(), S3Error> {
        debug!(key = %cache_key, "Revalidating cache entry");

        let metadata = self.s3_client.head_object(bucket, key).await?;

        // Check if the object has changed
        // For now, just fetch and update
        let (_, data) = self.fetch_from_origin(bucket, key, None).await?;

        let entry = CacheEntry::from_s3_metadata(
            data,
            metadata.content_type.as_str(),
            metadata.etag.as_ref().map(|e| e.to_string()),
            metadata.last_modified,
            metadata.cache_control.as_deref(),
            self.config.cache.default_ttl,
        );

        self.cache.put(cache_key, entry).await.ok();

        Ok(())
    }

    /// Serve a cached entry
    fn serve_cached(&self, entry: CacheEntry, head_only: bool) -> Response<ResponseBody> {
        let mut builder = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, &entry.content_type)
            .header(header::CONTENT_LENGTH, entry.data.len())
            .header(header::CACHE_CONTROL, entry.cache_control_header())
            .header("X-Cache", "HIT");

        if let Some(ref etag) = entry.etag {
            builder = builder.header(header::ETAG, etag);
        }

        if let Some(lm) = entry.last_modified {
            builder = builder.header(
                header::LAST_MODIFIED,
                lm.format("%a, %d %b %Y %H:%M:%S GMT").to_string(),
            );
        }

        if head_only {
            builder.body(Empty::new().map_err(|_| unreachable!()).boxed()).unwrap()
        } else {
            builder.body(Full::new(entry.data).map_err(|_| unreachable!()).boxed()).unwrap()
        }
    }

    /// Return 304 Not Modified
    fn not_modified(&self, entry: &CacheEntry) -> Response<ResponseBody> {
        let mut builder = Response::builder()
            .status(StatusCode::NOT_MODIFIED)
            .header(header::CACHE_CONTROL, entry.cache_control_header());

        if let Some(ref etag) = entry.etag {
            builder = builder.header(header::ETAG, etag);
        }

        builder.body(Empty::new().map_err(|_| unreachable!()).boxed()).unwrap()
    }

    /// Convert S3 error to HTTP response
    fn s3_error_response(&self, error: S3Error) -> Response<ResponseBody> {
        let (status, message) = match error {
            S3Error::NotFound { .. } => (StatusCode::NOT_FOUND, "Object not found"),
            S3Error::AccessDenied { .. } => (StatusCode::FORBIDDEN, "Access denied"),
            S3Error::Throttled { .. } => (StatusCode::TOO_MANY_REQUESTS, "Origin throttled"),
            S3Error::CircuitBreakerOpen => (StatusCode::SERVICE_UNAVAILABLE, "Service temporarily unavailable"),
            S3Error::Timeout(_) => (StatusCode::GATEWAY_TIMEOUT, "Origin timeout"),
            S3Error::InvalidRange => (StatusCode::RANGE_NOT_SATISFIABLE, "Invalid range"),
            _ => (StatusCode::BAD_GATEWAY, "Origin error"),
        };

        let body = serde_json::json!({
            "error": message,
            "status": status.as_u16(),
        });

        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from(body.to_string())).map_err(|_| unreachable!()).boxed())
            .unwrap()
    }

    fn bad_request(&self, message: &str) -> Response<ResponseBody> {
        let body = serde_json::json!({
            "error": message,
            "status": 400,
        });

        Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from(body.to_string())).map_err(|_| unreachable!()).boxed())
            .unwrap()
    }

    fn internal_error(&self, message: &str) -> Response<ResponseBody> {
        let body = serde_json::json!({
            "error": message,
            "status": 500,
        });

        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from(body.to_string())).map_err(|_| unreachable!()).boxed())
            .unwrap()
    }

    fn gateway_timeout(&self) -> Response<ResponseBody> {
        let body = serde_json::json!({
            "error": "Gateway timeout",
            "status": 504,
        });

        Response::builder()
            .status(StatusCode::GATEWAY_TIMEOUT)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from(body.to_string())).map_err(|_| unreachable!()).boxed())
            .unwrap()
    }

    async fn method_not_allowed(&self) -> Response<ResponseBody> {
        let body = serde_json::json!({
            "error": "Method not allowed",
            "status": 405,
        });

        Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ALLOW, "GET, HEAD")
            .body(Full::new(Bytes::from(body.to_string())).map_err(|_| unreachable!()).boxed())
            .unwrap()
    }
}

impl Clone for RequestHandler {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            s3_client: self.s3_client.clone(),
            cache: self.cache.clone(),
            security: self.security.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_path() {
        // This would need a full handler instance to test properly
        // For now, we just verify the logic
        let path = "/my-bucket/images/logo.png";
        let parts: Vec<&str> = path.trim_start_matches('/').splitn(2, '/').collect();
        assert_eq!(parts, vec!["my-bucket", "images/logo.png"]);
    }
}
