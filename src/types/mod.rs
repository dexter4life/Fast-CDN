//! Strong types for CDN domain concepts using NewType patterns.
//!
//! These types prevent logic errors by ensuring type safety at compile time.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;
use thiserror::Error;

/// Errors for type validation
#[derive(Debug, Error)]
pub enum TypeValidationError {
    #[error("Empty value not allowed for {0}")]
    Empty(&'static str),
    
    #[error("Invalid format for {field}: {reason}")]
    InvalidFormat {
        field: &'static str,
        reason: String,
    },
    
    #[error("Value too long: {field} max length is {max}, got {actual}")]
    TooLong {
        field: &'static str,
        max: usize,
        actual: usize,
    },
    
    #[error("Expired timestamp")]
    Expired,
}

/// S3 bucket name with validation
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct BucketName(String);

impl BucketName {
    const MIN_LEN: usize = 3;
    const MAX_LEN: usize = 63;

    pub fn new(name: impl Into<String>) -> Result<Self, TypeValidationError> {
        let name = name.into();
        
        if name.is_empty() {
            return Err(TypeValidationError::Empty("BucketName"));
        }
        
        if name.len() < Self::MIN_LEN || name.len() > Self::MAX_LEN {
            return Err(TypeValidationError::InvalidFormat {
                field: "BucketName",
                reason: format!(
                    "must be between {} and {} characters",
                    Self::MIN_LEN,
                    Self::MAX_LEN
                ),
            });
        }

        // Validate bucket name format (simplified S3 rules)
        let valid = name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.');
        
        if !valid {
            return Err(TypeValidationError::InvalidFormat {
                field: "BucketName",
                reason: "must contain only lowercase letters, numbers, hyphens, and periods".to_string(),
            });
        }

        Ok(Self(name))
    }

    /// Create without validation (for trusted sources)
    pub fn new_unchecked(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for BucketName {
    type Error = TypeValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        BucketName::new(value)
    }
}

impl From<BucketName> for String {
    fn from(bucket: BucketName) -> Self {
        bucket.0
    }
}

impl Deref for BucketName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for BucketName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// S3 object key (path within bucket)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ObjectKey(String);

impl ObjectKey {
    const MAX_LEN: usize = 1024;

    pub fn new(key: impl Into<String>) -> Result<Self, TypeValidationError> {
        let key = key.into();
        
        if key.is_empty() {
            return Err(TypeValidationError::Empty("ObjectKey"));
        }
        
        if key.len() > Self::MAX_LEN {
            return Err(TypeValidationError::TooLong {
                field: "ObjectKey",
                max: Self::MAX_LEN,
                actual: key.len(),
            });
        }

        Ok(Self(key))
    }

    pub fn new_unchecked(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Get file extension if present
    pub fn extension(&self) -> Option<&str> {
        self.0.rsplit_once('.').map(|(_, ext)| ext)
    }

    /// Get the filename portion
    pub fn filename(&self) -> &str {
        self.0.rsplit_once('/').map(|(_, name)| name).unwrap_or(&self.0)
    }

    /// Check if this is an image based on extension
    pub fn is_image(&self) -> bool {
        matches!(
            self.extension().map(|e| e.to_lowercase()).as_deref(),
            Some("jpg" | "jpeg" | "png" | "gif" | "webp" | "avif" | "svg" | "bmp" | "ico")
        )
    }

    /// Check if this is a video based on extension
    pub fn is_video(&self) -> bool {
        matches!(
            self.extension().map(|e| e.to_lowercase()).as_deref(),
            Some("mp4" | "webm" | "mkv" | "avi" | "mov" | "m4v" | "flv" | "wmv")
        )
    }
}

impl TryFrom<String> for ObjectKey {
    type Error = TypeValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        ObjectKey::new(value)
    }
}

impl From<ObjectKey> for String {
    fn from(key: ObjectKey) -> Self {
        key.0
    }
}

impl Deref for ObjectKey {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unix timestamp for URL expiration
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ExpiryTimestamp(i64);

impl ExpiryTimestamp {
    pub fn new(timestamp: i64) -> Self {
        Self(timestamp)
    }

    pub fn from_duration(duration: std::time::Duration) -> Self {
        let now = chrono::Utc::now().timestamp();
        Self(now + duration.as_secs() as i64)
    }

    pub fn is_expired(&self) -> bool {
        let now = chrono::Utc::now().timestamp();
        self.0 < now
    }

    pub fn validate(&self) -> Result<(), TypeValidationError> {
        if self.is_expired() {
            Err(TypeValidationError::Expired)
        } else {
            Ok(())
        }
    }

    pub fn as_i64(&self) -> i64 {
        self.0
    }

    pub fn remaining_seconds(&self) -> i64 {
        let now = chrono::Utc::now().timestamp();
        self.0 - now
    }
}

impl From<i64> for ExpiryTimestamp {
    fn from(timestamp: i64) -> Self {
        Self::new(timestamp)
    }
}

impl fmt::Display for ExpiryTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// HMAC token for signed URLs
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SignatureToken(Vec<u8>);

impl SignatureToken {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub fn from_hex(hex_str: &str) -> Result<Self, TypeValidationError> {
        hex::decode(hex_str)
            .map(Self)
            .map_err(|e| TypeValidationError::InvalidFormat {
                field: "SignatureToken",
                reason: e.to_string(),
            })
    }

    pub fn from_base64(b64_str: &str) -> Result<Self, TypeValidationError> {
        use base64::Engine;
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(b64_str)
            .map(Self)
            .map_err(|e| TypeValidationError::InvalidFormat {
                field: "SignatureToken",
                reason: e.to_string(),
            })
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(&self.0)
    }

    pub fn to_base64(&self) -> String {
        use base64::Engine;
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&self.0)
    }

    /// Constant-time comparison to prevent timing attacks
    pub fn verify(&self, other: &Self) -> bool {
        use ring::constant_time::verify_slices_are_equal;
        verify_slices_are_equal(&self.0, &other.0).is_ok()
    }
}

/// Cache key for storing objects
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    bucket: BucketName,
    key: ObjectKey,
    variant: Option<String>,
}

impl CacheKey {
    pub fn new(bucket: BucketName, key: ObjectKey) -> Self {
        Self {
            bucket,
            key,
            variant: None,
        }
    }

    pub fn with_variant(mut self, variant: impl Into<String>) -> Self {
        self.variant = Some(variant.into());
        self
    }

    pub fn bucket(&self) -> &BucketName {
        &self.bucket
    }

    pub fn key(&self) -> &ObjectKey {
        &self.key
    }

    pub fn variant(&self) -> Option<&str> {
        self.variant.as_deref()
    }

    /// Generate a hash-based filename for disk storage
    pub fn disk_path(&self) -> String {
        use sha2::{Digest, Sha256};
        
        let mut hasher = Sha256::new();
        hasher.update(self.bucket.as_bytes());
        hasher.update(b"/");
        hasher.update(self.key.as_bytes());
        if let Some(ref variant) = self.variant {
            hasher.update(b"?");
            hasher.update(variant.as_bytes());
        }
        
        let hash = hasher.finalize();
        let hash_hex = hex::encode(hash);
        
        // Use first 2 chars as directory for sharding
        format!("{}/{}", &hash_hex[..2], hash_hex)
    }
}

impl fmt::Display for CacheKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.bucket, self.key)?;
        if let Some(ref variant) = self.variant {
            write!(f, "?{}", variant)?;
        }
        Ok(())
    }
}

/// Byte range for Range requests
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    pub start: u64,
    pub end: Option<u64>,
}

impl ByteRange {
    pub fn new(start: u64, end: Option<u64>) -> Self {
        Self { start, end }
    }

    pub fn full() -> Self {
        Self { start: 0, end: None }
    }

    pub fn from_header(header: &str) -> Option<Self> {
        // Parse "bytes=start-end" or "bytes=start-"
        let range_spec = header.strip_prefix("bytes=")?;
        let (start_str, end_str) = range_spec.split_once('-')?;
        
        let start = start_str.parse().ok()?;
        let end = if end_str.is_empty() {
            None
        } else {
            Some(end_str.parse().ok()?)
        };
        
        Some(Self { start, end })
    }

    pub fn to_header(&self) -> String {
        match self.end {
            Some(end) => format!("bytes={}-{}", self.start, end),
            None => format!("bytes={}-", self.start),
        }
    }

    pub fn length(&self) -> Option<u64> {
        self.end.map(|e| e - self.start + 1)
    }
}

/// Content type with parsing helpers
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ContentType(String);

impl ContentType {
    pub fn new(mime: impl Into<String>) -> Self {
        Self(mime.into())
    }

    pub fn from_extension(ext: &str) -> Self {
        let mime = match ext.to_lowercase().as_str() {
            "html" | "htm" => "text/html",
            "css" => "text/css",
            "js" | "mjs" => "application/javascript",
            "json" => "application/json",
            "xml" => "application/xml",
            "txt" => "text/plain",
            "png" => "image/png",
            "jpg" | "jpeg" => "image/jpeg",
            "gif" => "image/gif",
            "webp" => "image/webp",
            "avif" => "image/avif",
            "svg" => "image/svg+xml",
            "ico" => "image/x-icon",
            "mp4" => "video/mp4",
            "webm" => "video/webm",
            "mp3" => "audio/mpeg",
            "wav" => "audio/wav",
            "pdf" => "application/pdf",
            "woff" => "font/woff",
            "woff2" => "font/woff2",
            "ttf" => "font/ttf",
            "eot" => "application/vnd.ms-fontobject",
            _ => "application/octet-stream",
        };
        Self(mime.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_image(&self) -> bool {
        self.0.starts_with("image/")
    }

    pub fn is_video(&self) -> bool {
        self.0.starts_with("video/")
    }

    pub fn is_compressible(&self) -> bool {
        self.0.starts_with("text/")
            || self.0.contains("json")
            || self.0.contains("xml")
            || self.0.contains("javascript")
            || self.0 == "image/svg+xml"
    }
}

impl Default for ContentType {
    fn default() -> Self {
        Self("application/octet-stream".to_string())
    }
}

impl fmt::Display for ContentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// ETag for cache validation
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ETag(String);

impl ETag {
    pub fn new(etag: impl Into<String>) -> Self {
        Self(etag.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Check if this is a weak ETag
    pub fn is_weak(&self) -> bool {
        self.0.starts_with("W/")
    }

    /// Strip weak prefix if present
    pub fn strong_part(&self) -> &str {
        self.0.strip_prefix("W/").unwrap_or(&self.0)
    }
}

impl fmt::Display for ETag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_name_validation() {
        assert!(BucketName::new("my-bucket").is_ok());
        assert!(BucketName::new("my.bucket.name").is_ok());
        assert!(BucketName::new("ab").is_err()); // Too short
        assert!(BucketName::new("My-Bucket").is_err()); // Uppercase
        assert!(BucketName::new("").is_err()); // Empty
    }

    #[test]
    fn test_object_key() {
        let key = ObjectKey::new("images/logo.png").unwrap();
        assert_eq!(key.extension(), Some("png"));
        assert_eq!(key.filename(), "logo.png");
        assert!(key.is_image());
    }

    #[test]
    fn test_expiry_timestamp() {
        let future = ExpiryTimestamp::from_duration(std::time::Duration::from_secs(3600));
        assert!(!future.is_expired());
        
        let past = ExpiryTimestamp::new(0);
        assert!(past.is_expired());
    }

    #[test]
    fn test_byte_range_parsing() {
        let range = ByteRange::from_header("bytes=0-1023").unwrap();
        assert_eq!(range.start, 0);
        assert_eq!(range.end, Some(1023));
        
        let range = ByteRange::from_header("bytes=1024-").unwrap();
        assert_eq!(range.start, 1024);
        assert_eq!(range.end, None);
    }

    #[test]
    fn test_cache_key_disk_path() {
        let bucket = BucketName::new_unchecked("my-bucket");
        let key = ObjectKey::new_unchecked("images/logo.png");
        let cache_key = CacheKey::new(bucket, key);
        
        let path = cache_key.disk_path();
        assert!(path.contains('/'));
        assert_eq!(path.len(), 2 + 1 + 64); // 2 chars + / + 64 hex chars
    }
}
