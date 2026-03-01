//! HMAC-based URL signature validation.
//!
//! Validates signed URLs using HMAC-SHA256 to prevent unauthorized access
//! and URL tampering.

use crate::config::SecurityConfig;
use crate::types::ExpiryTimestamp;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;
use thiserror::Error;
use tracing::{debug, warn};

type HmacSha256 = Hmac<Sha256>;

/// HMAC validation errors
#[derive(Debug, Error)]
pub enum HmacError {
    #[error("Missing signature token")]
    MissingToken,

    #[error("Missing expiry timestamp")]
    MissingExpiry,

    #[error("Invalid expiry format")]
    InvalidExpiry,

    #[error("URL has expired")]
    Expired,

    #[error("Invalid signature")]
    InvalidSignature,

    #[error("Invalid secret key format")]
    InvalidSecretKey,
}

/// HMAC URL validator
pub struct HmacValidator {
    secret_key: Vec<u8>,
    signature_param: String,
    expires_param: String,
}

impl HmacValidator {
    /// Create a new HMAC validator from configuration
    pub fn new(config: &SecurityConfig) -> Result<Self, HmacError> {
        let secret_key = config
            .hmac_secret
            .as_ref()
            .ok_or(HmacError::InvalidSecretKey)?;

        // Decode base64 secret key
        let secret_bytes = base64::engine::general_purpose::STANDARD
            .decode(secret_key)
            .map_err(|_| HmacError::InvalidSecretKey)?;

        Ok(Self {
            secret_key: secret_bytes,
            signature_param: config.signature_param.clone(),
            expires_param: config.expires_param.clone(),
        })
    }

    /// Create a validator with a raw secret key (for testing)
    pub fn with_secret(secret: &[u8]) -> Self {
        Self {
            secret_key: secret.to_vec(),
            signature_param: "token".to_string(),
            expires_param: "expires".to_string(),
        }
    }

    /// Validate a request URL
    pub fn validate(&self, path: &str, query_params: &HashMap<String, String>) -> Result<(), HmacError> {
        // Extract token
        let token = query_params
            .get(&self.signature_param)
            .ok_or(HmacError::MissingToken)?;

        // Extract expiry
        let expires_str = query_params
            .get(&self.expires_param)
            .ok_or(HmacError::MissingExpiry)?;

        let expires: i64 = expires_str
            .parse()
            .map_err(|_| HmacError::InvalidExpiry)?;

        let expiry = ExpiryTimestamp::new(expires);

        // Check expiry first (fast path)
        if expiry.is_expired() {
            debug!(
                expires = expires,
                remaining = expiry.remaining_seconds(),
                "URL has expired"
            );
            return Err(HmacError::Expired);
        }

        // Build the string to sign (path + expiry)
        let string_to_sign = format!("{}?{}={}", path, self.expires_param, expires);

        // Calculate expected signature
        let expected_signature = self.sign(&string_to_sign);

        // Decode provided token
        let provided_signature = self.decode_token(token)?;

        // Constant-time comparison
        if !self.constant_time_compare(&expected_signature, &provided_signature) {
            warn!(
                path = path,
                "Invalid signature"
            );
            return Err(HmacError::InvalidSignature);
        }

        debug!(path = path, "Signature validated successfully");
        Ok(())
    }

    /// Generate a signature for a string
    fn sign(&self, data: &str) -> Vec<u8> {
        let mut mac = HmacSha256::new_from_slice(&self.secret_key)
            .expect("HMAC can take key of any size");
        mac.update(data.as_bytes());
        mac.finalize().into_bytes().to_vec()
    }

    /// Decode a base64url or hex token
    fn decode_token(&self, token: &str) -> Result<Vec<u8>, HmacError> {
        // Try base64url first
        if let Ok(bytes) = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(token) {
            return Ok(bytes);
        }

        // Try standard base64
        if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(token) {
            return Ok(bytes);
        }

        // Try hex
        if let Ok(bytes) = hex::decode(token) {
            return Ok(bytes);
        }

        Err(HmacError::InvalidSignature)
    }

    /// Constant-time comparison to prevent timing attacks
    fn constant_time_compare(&self, a: &[u8], b: &[u8]) -> bool {
        use ring::constant_time::verify_slices_are_equal;
        verify_slices_are_equal(a, b).is_ok()
    }

    /// Generate a signed URL
    pub fn sign_url(&self, path: &str, ttl_seconds: i64) -> String {
        let expires = chrono::Utc::now().timestamp() + ttl_seconds;
        let string_to_sign = format!("{}?{}={}", path, self.expires_param, expires);
        let signature = self.sign(&string_to_sign);
        let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&signature);

        format!(
            "{}?{}={}&{}={}",
            path, self.expires_param, expires, self.signature_param, token
        )
    }
}

/// Extract query parameters from a URL
pub fn parse_query_params(query: &str) -> HashMap<String, String> {
    query
        .split('&')
        .filter_map(|param| {
            let mut parts = param.splitn(2, '=');
            let key = parts.next()?;
            let value = parts.next().unwrap_or("");
            Some((
                urlencoding::decode(key).ok()?.into_owned(),
                urlencoding::decode(value).ok()?.into_owned(),
            ))
        })
        .collect()
}

/// Check if a path is in the allowed unsigned paths list
pub fn is_unsigned_path(path: &str, unsigned_paths: &[String]) -> bool {
    unsigned_paths.iter().any(|allowed| {
        if allowed.ends_with('*') {
            path.starts_with(allowed.trim_end_matches('*'))
        } else {
            path == allowed
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_validator() -> HmacValidator {
        HmacValidator::with_secret(b"test-secret-key-1234567890")
    }

    #[test]
    fn test_sign_and_validate() {
        let validator = create_test_validator();
        
        let path = "/images/logo.png";
        let signed_url = validator.sign_url(path, 3600); // 1 hour

        // Parse the signed URL
        let (path_part, query_part) = signed_url.split_once('?').unwrap();
        let params = parse_query_params(query_part);

        let result = validator.validate(path_part, &params);
        assert!(result.is_ok());
    }

    #[test]
    fn test_expired_url() {
        let validator = create_test_validator();
        
        let path = "/images/logo.png";
        let signed_url = validator.sign_url(path, -1); // Already expired

        let (path_part, query_part) = signed_url.split_once('?').unwrap();
        let params = parse_query_params(query_part);

        let result = validator.validate(path_part, &params);
        assert!(matches!(result, Err(HmacError::Expired)));
    }

    #[test]
    fn test_tampered_signature() {
        let validator = create_test_validator();
        
        let path = "/images/logo.png";
        let signed_url = validator.sign_url(path, 3600);

        let (path_part, query_part) = signed_url.split_once('?').unwrap();
        let mut params = parse_query_params(query_part);
        
        // Tamper with the token
        params.insert("token".to_string(), "invalid-token".to_string());

        let result = validator.validate(path_part, &params);
        assert!(matches!(result, Err(HmacError::InvalidSignature)));
    }

    #[test]
    fn test_missing_token() {
        let validator = create_test_validator();
        
        let params: HashMap<String, String> = [
            ("expires".to_string(), "9999999999".to_string()),
        ].into_iter().collect();

        let result = validator.validate("/images/logo.png", &params);
        assert!(matches!(result, Err(HmacError::MissingToken)));
    }

    #[test]
    fn test_missing_expiry() {
        let validator = create_test_validator();
        
        let params: HashMap<String, String> = [
            ("token".to_string(), "some-token".to_string()),
        ].into_iter().collect();

        let result = validator.validate("/images/logo.png", &params);
        assert!(matches!(result, Err(HmacError::MissingExpiry)));
    }

    #[test]
    fn test_parse_query_params() {
        let query = "token=abc123&expires=1234567890&foo=bar%20baz";
        let params = parse_query_params(query);

        assert_eq!(params.get("token"), Some(&"abc123".to_string()));
        assert_eq!(params.get("expires"), Some(&"1234567890".to_string()));
        assert_eq!(params.get("foo"), Some(&"bar baz".to_string()));
    }

    #[test]
    fn test_unsigned_path_matching() {
        let unsigned_paths = vec![
            "/health".to_string(),
            "/metrics".to_string(),
            "/public/*".to_string(),
        ];

        assert!(is_unsigned_path("/health", &unsigned_paths));
        assert!(is_unsigned_path("/metrics", &unsigned_paths));
        assert!(is_unsigned_path("/public/images/logo.png", &unsigned_paths));
        assert!(!is_unsigned_path("/private/data.json", &unsigned_paths));
    }
}
