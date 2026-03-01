//! Security middleware for request validation.

use crate::config::SecurityConfig;
use crate::security::{HmacError, HmacValidator, RateLimitConfig, RateLimiter, is_unsigned_path, parse_query_params};
use http::{Request, Response, StatusCode};
use std::net::IpAddr;
use std::sync::Arc;
use tracing::{debug, warn};

/// Security validation result
#[derive(Debug)]
pub enum SecurityResult {
    /// Request is allowed
    Allowed,
    /// Request requires signature validation
    RequiresSignature,
    /// Request is denied with reason
    Denied(SecurityDenialReason),
}

/// Reason for denying a request
#[derive(Debug, Clone)]
pub enum SecurityDenialReason {
    RateLimited { remaining: u32 },
    MissingSignature,
    InvalidSignature,
    ExpiredSignature,
    Forbidden,
}

impl SecurityDenialReason {
    pub fn status_code(&self) -> StatusCode {
        match self {
            SecurityDenialReason::RateLimited { .. } => StatusCode::TOO_MANY_REQUESTS,
            SecurityDenialReason::MissingSignature => StatusCode::UNAUTHORIZED,
            SecurityDenialReason::InvalidSignature => StatusCode::FORBIDDEN,
            SecurityDenialReason::ExpiredSignature => StatusCode::GONE,
            SecurityDenialReason::Forbidden => StatusCode::FORBIDDEN,
        }
    }

    pub fn message(&self) -> &'static str {
        match self {
            SecurityDenialReason::RateLimited { .. } => "Rate limit exceeded",
            SecurityDenialReason::MissingSignature => "Missing URL signature",
            SecurityDenialReason::InvalidSignature => "Invalid URL signature",
            SecurityDenialReason::ExpiredSignature => "URL has expired",
            SecurityDenialReason::Forbidden => "Access forbidden",
        }
    }
}

/// Security middleware for validating requests
pub struct SecurityMiddleware {
    config: SecurityConfig,
    hmac_validator: Option<HmacValidator>,
    rate_limiter: Option<RateLimiter>,
}

impl SecurityMiddleware {
    /// Create new security middleware from configuration
    pub fn new(config: SecurityConfig) -> Result<Self, HmacError> {
        let hmac_validator = if config.enable_signed_urls {
            Some(HmacValidator::new(&config)?)
        } else {
            None
        };

        let rate_limiter = if config.enable_rate_limiting {
            Some(RateLimiter::new(RateLimitConfig {
                requests_per_second: config.rate_limit_rps,
                burst: config.rate_limit_burst,
            }))
        } else {
            None
        };

        Ok(Self {
            config,
            hmac_validator,
            rate_limiter,
        })
    }

    /// Validate a request
    pub fn validate(&self, path: &str, query: Option<&str>, client_ip: Option<IpAddr>) -> SecurityResult {
        // Check rate limiting first
        if let (Some(ref limiter), Some(ip)) = (&self.rate_limiter, client_ip) {
            if !limiter.check(ip) {
                let remaining = limiter.remaining(&ip);
                warn!(
                    ip = %ip,
                    path = path,
                    "Rate limit exceeded"
                );
                return SecurityResult::Denied(SecurityDenialReason::RateLimited { remaining });
            }
        }

        // Check if path allows unsigned requests
        if is_unsigned_path(path, &self.config.unsigned_paths) {
            debug!(path = path, "Path allows unsigned access");
            return SecurityResult::Allowed;
        }

        // Check HMAC signature if enabled
        if let Some(ref validator) = self.hmac_validator {
            let params = query
                .map(parse_query_params)
                .unwrap_or_default();

            match validator.validate(path, &params) {
                Ok(()) => {
                    debug!(path = path, "Signature validated");
                    SecurityResult::Allowed
                }
                Err(HmacError::MissingToken | HmacError::MissingExpiry) => {
                    debug!(path = path, "Missing signature parameters");
                    SecurityResult::Denied(SecurityDenialReason::MissingSignature)
                }
                Err(HmacError::Expired) => {
                    debug!(path = path, "URL has expired");
                    SecurityResult::Denied(SecurityDenialReason::ExpiredSignature)
                }
                Err(HmacError::InvalidSignature) => {
                    warn!(path = path, "Invalid signature");
                    SecurityResult::Denied(SecurityDenialReason::InvalidSignature)
                }
                Err(_) => {
                    SecurityResult::Denied(SecurityDenialReason::Forbidden)
                }
            }
        } else {
            // Signatures not enabled, allow all
            SecurityResult::Allowed
        }
    }

    /// Generate a signed URL
    pub fn sign_url(&self, path: &str, ttl_seconds: i64) -> Option<String> {
        self.hmac_validator.as_ref().map(|v| v.sign_url(path, ttl_seconds))
    }

    /// Get rate limiter statistics
    pub fn rate_limit_stats(&self) -> Option<crate::security::RateLimitStats> {
        self.rate_limiter.as_ref().map(|l| l.stats())
    }
}

impl Clone for SecurityMiddleware {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            hmac_validator: self.config.enable_signed_urls
                .then(|| HmacValidator::new(&self.config).ok())
                .flatten(),
            rate_limiter: self.rate_limiter.clone(),
        }
    }
}

/// Extract client IP from request headers (handles X-Forwarded-For, etc.)
pub fn extract_client_ip<B>(request: &Request<B>) -> Option<IpAddr> {
    // Try X-Forwarded-For first
    if let Some(xff) = request.headers().get("x-forwarded-for") {
        if let Ok(xff_str) = xff.to_str() {
            // Take the first IP in the chain
            if let Some(first_ip) = xff_str.split(',').next() {
                if let Ok(ip) = first_ip.trim().parse() {
                    return Some(ip);
                }
            }
        }
    }

    // Try X-Real-IP
    if let Some(xri) = request.headers().get("x-real-ip") {
        if let Ok(xri_str) = xri.to_str() {
            if let Ok(ip) = xri_str.parse() {
                return Some(ip);
            }
        }
    }

    // Try CF-Connecting-IP (Cloudflare)
    if let Some(cf_ip) = request.headers().get("cf-connecting-ip") {
        if let Ok(cf_str) = cf_ip.to_str() {
            if let Ok(ip) = cf_str.parse() {
                return Some(ip);
            }
        }
    }

    None
}

/// Create an error response for security denial
pub fn create_denial_response(reason: &SecurityDenialReason) -> Response<String> {
    let body = serde_json::json!({
        "error": reason.message(),
        "status": reason.status_code().as_u16(),
    }).to_string();

    let mut response = Response::new(body);
    *response.status_mut() = reason.status_code();
    
    // Add rate limit headers if applicable
    if let SecurityDenialReason::RateLimited { remaining } = reason {
        response.headers_mut().insert(
            "X-RateLimit-Remaining",
            remaining.to_string().parse().unwrap(),
        );
        response.headers_mut().insert(
            "Retry-After",
            "1".parse().unwrap(),
        );
    }

    response.headers_mut().insert(
        "Content-Type",
        "application/json".parse().unwrap(),
    );

    response
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> SecurityConfig {
        SecurityConfig {
            enable_signed_urls: true,
            hmac_secret: Some(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                b"test-secret-key-1234567890",
            )),
            signature_param: "token".to_string(),
            expires_param: "expires".to_string(),
            unsigned_paths: vec!["/health".to_string(), "/metrics".to_string()],
            max_expiry_time: std::time::Duration::from_secs(86400 * 7),
            cors_origins: vec!["*".to_string()],
            enable_rate_limiting: true,
            rate_limit_rps: 100,
            rate_limit_burst: 200,
        }
    }

    #[test]
    fn test_unsigned_path_allowed() {
        let config = create_test_config();
        let middleware = SecurityMiddleware::new(config).unwrap();

        let result = middleware.validate("/health", None, None);
        assert!(matches!(result, SecurityResult::Allowed));
    }

    #[test]
    fn test_signed_path_requires_signature() {
        let config = create_test_config();
        let middleware = SecurityMiddleware::new(config).unwrap();

        let result = middleware.validate("/images/logo.png", None, None);
        assert!(matches!(
            result,
            SecurityResult::Denied(SecurityDenialReason::MissingSignature)
        ));
    }

    #[test]
    fn test_valid_signature_allowed() {
        let config = create_test_config();
        let middleware = SecurityMiddleware::new(config).unwrap();

        // Generate a valid signed URL
        let signed_url = middleware.sign_url("/images/logo.png", 3600).unwrap();
        let (path, query) = signed_url.split_once('?').unwrap();

        let result = middleware.validate(path, Some(query), None);
        assert!(matches!(result, SecurityResult::Allowed));
    }

    #[test]
    fn test_rate_limiting() {
        let mut config = create_test_config();
        config.enable_signed_urls = false;
        config.rate_limit_rps = 1;
        config.rate_limit_burst = 2;

        let middleware = SecurityMiddleware::new(config).unwrap();
        let ip: IpAddr = "127.0.0.1".parse().unwrap();

        // First two requests allowed (burst)
        assert!(matches!(
            middleware.validate("/test", None, Some(ip)),
            SecurityResult::Allowed
        ));
        assert!(matches!(
            middleware.validate("/test", None, Some(ip)),
            SecurityResult::Allowed
        ));

        // Third request denied
        assert!(matches!(
            middleware.validate("/test", None, Some(ip)),
            SecurityResult::Denied(SecurityDenialReason::RateLimited { .. })
        ));
    }

    #[test]
    fn test_extract_client_ip_xff() {
        let request = Request::builder()
            .header("x-forwarded-for", "1.2.3.4, 5.6.7.8")
            .body(())
            .unwrap();

        let ip = extract_client_ip(&request);
        assert_eq!(ip, Some("1.2.3.4".parse().unwrap()));
    }
}
