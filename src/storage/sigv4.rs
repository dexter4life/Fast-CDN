//! AWS SigV4 signing implementation for custom S3 requests.
//!
//! This module provides SigV4 signing for cases where we need to make
//! direct HTTP requests to S3-compatible storage without using the AWS SDK.

use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

type HmacSha256 = Hmac<Sha256>;

/// SigV4 credentials
#[derive(Debug, Clone)]
pub struct SigV4Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

/// HTTP method for signing
#[derive(Debug, Clone, Copy)]
pub enum HttpMethod {
    Get,
    Put,
    Post,
    Delete,
    Head,
}

impl HttpMethod {
    pub fn as_str(&self) -> &'static str {
        match self {
            HttpMethod::Get => "GET",
            HttpMethod::Put => "PUT",
            HttpMethod::Post => "POST",
            HttpMethod::Delete => "DELETE",
            HttpMethod::Head => "HEAD",
        }
    }
}

/// Request to be signed
pub struct SigningRequest {
    pub method: HttpMethod,
    pub host: String,
    pub path: String,
    pub query: BTreeMap<String, String>,
    pub headers: BTreeMap<String, String>,
    pub payload_hash: String,
}

impl SigningRequest {
    /// Create a new signing request
    pub fn new(method: HttpMethod, host: &str, path: &str) -> Self {
        Self {
            method,
            host: host.to_string(),
            path: path.to_string(),
            query: BTreeMap::new(),
            headers: BTreeMap::new(),
            payload_hash: EMPTY_SHA256.to_string(),
        }
    }

    /// Add a query parameter
    pub fn query(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.query.insert(key.into(), value.into());
        self
    }

    /// Add a header
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into().to_lowercase(), value.into());
        self
    }

    /// Set the payload hash
    pub fn payload_hash(mut self, hash: impl Into<String>) -> Self {
        self.payload_hash = hash.into();
        self
    }
}

/// Signed request result
pub struct SignedRequest {
    pub authorization: String,
    pub x_amz_date: String,
    pub x_amz_content_sha256: String,
    pub x_amz_security_token: Option<String>,
}

/// SHA256 hash of empty string
pub const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// SigV4 signer
pub struct SigV4Signer {
    credentials: SigV4Credentials,
    region: String,
    service: String,
}

impl SigV4Signer {
    /// Create a new SigV4 signer
    pub fn new(credentials: SigV4Credentials, region: impl Into<String>) -> Self {
        Self {
            credentials,
            region: region.into(),
            service: "s3".to_string(),
        }
    }

    /// Sign a request
    pub fn sign(&self, request: &SigningRequest, timestamp: DateTime<Utc>) -> SignedRequest {
        let date_stamp = timestamp.format("%Y%m%d").to_string();
        let amz_date = timestamp.format("%Y%m%dT%H%M%SZ").to_string();

        // Step 1: Create canonical request
        let canonical_request = self.create_canonical_request(request, &amz_date);

        // Step 2: Create string to sign
        let credential_scope = format!(
            "{}/{}/{}/aws4_request",
            date_stamp, self.region, self.service
        );
        let string_to_sign =
            self.create_string_to_sign(&amz_date, &credential_scope, &canonical_request);

        // Step 3: Calculate signature
        let signature = self.calculate_signature(&date_stamp, &string_to_sign);

        // Step 4: Build authorization header
        let signed_headers = self.get_signed_headers(request);
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.credentials.access_key_id, credential_scope, signed_headers, signature
        );

        SignedRequest {
            authorization,
            x_amz_date: amz_date,
            x_amz_content_sha256: request.payload_hash.clone(),
            x_amz_security_token: self.credentials.session_token.clone(),
        }
    }

    /// Create canonical request string
    fn create_canonical_request(&self, request: &SigningRequest, amz_date: &str) -> String {
        // Canonical URI
        let canonical_uri = url_encode_path(&request.path);

        // Canonical query string
        let canonical_query_string = self.create_canonical_query_string(&request.query);

        // Canonical headers
        let mut headers = request.headers.clone();
        headers.insert("host".to_string(), request.host.clone());
        headers.insert("x-amz-date".to_string(), amz_date.to_string());
        headers.insert(
            "x-amz-content-sha256".to_string(),
            request.payload_hash.clone(),
        );

        if let Some(ref token) = self.credentials.session_token {
            headers.insert("x-amz-security-token".to_string(), token.clone());
        }

        let canonical_headers = headers
            .iter()
            .map(|(k, v)| format!("{}:{}", k.to_lowercase(), v.trim()))
            .collect::<Vec<_>>()
            .join("\n");

        let signed_headers = headers
            .keys()
            .map(|k| k.to_lowercase())
            .collect::<Vec<_>>()
            .join(";");

        format!(
            "{}\n{}\n{}\n{}\n\n{}\n{}",
            request.method.as_str(),
            canonical_uri,
            canonical_query_string,
            canonical_headers,
            signed_headers,
            request.payload_hash
        )
    }

    /// Create canonical query string
    fn create_canonical_query_string(&self, query: &BTreeMap<String, String>) -> String {
        query
            .iter()
            .map(|(k, v)| format!("{}={}", url_encode(k), url_encode(v)))
            .collect::<Vec<_>>()
            .join("&")
    }

    /// Get signed headers string
    fn get_signed_headers(&self, request: &SigningRequest) -> String {
        let mut headers: Vec<String> = request.headers.keys().map(|k| k.to_lowercase()).collect();
        headers.push("host".to_string());
        headers.push("x-amz-content-sha256".to_string());
        headers.push("x-amz-date".to_string());

        if self.credentials.session_token.is_some() {
            headers.push("x-amz-security-token".to_string());
        }

        headers.sort();
        headers.join(";")
    }

    /// Create string to sign
    fn create_string_to_sign(
        &self,
        amz_date: &str,
        credential_scope: &str,
        canonical_request: &str,
    ) -> String {
        let canonical_request_hash = sha256_hex(canonical_request.as_bytes());

        format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            amz_date, credential_scope, canonical_request_hash
        )
    }

    /// Calculate signature
    fn calculate_signature(&self, date_stamp: &str, string_to_sign: &str) -> String {
        let k_date = hmac_sha256(
            format!("AWS4{}", self.credentials.secret_access_key).as_bytes(),
            date_stamp.as_bytes(),
        );
        let k_region = hmac_sha256(&k_date, self.region.as_bytes());
        let k_service = hmac_sha256(&k_region, self.service.as_bytes());
        let k_signing = hmac_sha256(&k_service, b"aws4_request");

        hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes()))
    }

    /// Generate a presigned URL for GET requests
    pub fn presign_url(
        &self,
        host: &str,
        path: &str,
        expires_in_seconds: u64,
        timestamp: DateTime<Utc>,
    ) -> String {
        let date_stamp = timestamp.format("%Y%m%d").to_string();
        let amz_date = timestamp.format("%Y%m%dT%H%M%SZ").to_string();

        let credential_scope = format!(
            "{}/{}/{}/aws4_request",
            date_stamp, self.region, self.service
        );
        let credential = format!("{}/{}", self.credentials.access_key_id, credential_scope);

        let signed_headers = "host";

        // Build query parameters
        let mut query = BTreeMap::new();
        query.insert("X-Amz-Algorithm".to_string(), "AWS4-HMAC-SHA256".to_string());
        query.insert("X-Amz-Credential".to_string(), credential);
        query.insert("X-Amz-Date".to_string(), amz_date.clone());
        query.insert(
            "X-Amz-Expires".to_string(),
            expires_in_seconds.to_string(),
        );
        query.insert(
            "X-Amz-SignedHeaders".to_string(),
            signed_headers.to_string(),
        );

        if let Some(ref token) = self.credentials.session_token {
            query.insert("X-Amz-Security-Token".to_string(), token.clone());
        }

        // Canonical request for presigned URL
        let canonical_uri = url_encode_path(path);
        let canonical_query_string = query
            .iter()
            .map(|(k, v)| format!("{}={}", url_encode(k), url_encode(v)))
            .collect::<Vec<_>>()
            .join("&");

        let canonical_headers = format!("host:{}", host);
        let payload_hash = "UNSIGNED-PAYLOAD";

        let canonical_request = format!(
            "GET\n{}\n{}\n{}\n\n{}\n{}",
            canonical_uri, canonical_query_string, canonical_headers, signed_headers, payload_hash
        );

        let canonical_request_hash = sha256_hex(canonical_request.as_bytes());
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            amz_date, credential_scope, canonical_request_hash
        );

        let signature = self.calculate_signature(&date_stamp, &string_to_sign);

        format!(
            "https://{}{}?{}&X-Amz-Signature={}",
            host, path, canonical_query_string, signature
        )
    }
}

/// Calculate SHA256 hash and return hex string
fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Calculate HMAC-SHA256
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// URL encode a string (RFC 3986)
fn url_encode(s: &str) -> String {
    let mut encoded = String::with_capacity(s.len() * 3);
    for ch in s.bytes() {
        if ch.is_ascii_alphanumeric() || ch == b'-' || ch == b'_' || ch == b'.' || ch == b'~' {
            encoded.push(ch as char);
        } else {
            encoded.push_str(&format!("%{:02X}", ch));
        }
    }
    encoded
}

/// URL encode a path (preserving slashes)
fn url_encode_path(path: &str) -> String {
    path.split('/')
        .map(url_encode)
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha256_hex() {
        assert_eq!(sha256_hex(b""), EMPTY_SHA256);
    }

    #[test]
    fn test_url_encode() {
        assert_eq!(url_encode("hello world"), "hello%20world");
        assert_eq!(url_encode("test-file_name.txt"), "test-file_name.txt");
        assert_eq!(url_encode("a/b/c"), "a%2Fb%2Fc");
    }

    #[test]
    fn test_url_encode_path() {
        assert_eq!(url_encode_path("/images/test file.png"), "/images/test%20file.png");
    }

    #[test]
    fn test_signing() {
        let credentials = SigV4Credentials {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            session_token: None,
        };

        let signer = SigV4Signer::new(credentials, "us-east-1");
        
        let request = SigningRequest::new(HttpMethod::Get, "examplebucket.s3.amazonaws.com", "/test.txt");
        
        let timestamp = DateTime::parse_from_rfc3339("2023-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        
        let signed = signer.sign(&request, timestamp);
        
        assert!(signed.authorization.starts_with("AWS4-HMAC-SHA256"));
        assert!(signed.authorization.contains("Credential="));
        assert!(!signed.authorization.is_empty());
    }
}
