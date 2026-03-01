//! Cache entry structure with metadata.

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// A cached object with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntry {
    /// The cached data
    #[serde(with = "bytes_serde")]
    pub data: Bytes,

    /// Content-Type header
    pub content_type: String,

    /// ETag for validation
    pub etag: Option<String>,

    /// Last-Modified timestamp
    pub last_modified: Option<DateTime<Utc>>,

    /// When this entry expires
    pub expires_at: Option<DateTime<Utc>>,

    /// When this entry was created
    pub created_at: DateTime<Utc>,

    /// Whether this entry needs background revalidation
    #[serde(default)]
    pub needs_revalidation: bool,
}

impl CacheEntry {
    /// Create a new cache entry with default TTL
    pub fn new(data: Bytes, content_type: &str, ttl: Duration) -> Self {
        let now = Utc::now();
        Self {
            data,
            content_type: content_type.to_string(),
            etag: None,
            last_modified: None,
            expires_at: Some(now + chrono::Duration::from_std(ttl).unwrap_or_default()),
            created_at: now,
            needs_revalidation: false,
        }
    }

    /// Create a cache entry from S3 metadata
    pub fn from_s3_metadata(
        data: Bytes,
        content_type: &str,
        etag: Option<String>,
        last_modified: Option<DateTime<Utc>>,
        cache_control: Option<&str>,
        default_ttl: Duration,
    ) -> Self {
        let now = Utc::now();
        
        // Parse max-age from Cache-Control
        let max_age = cache_control.and_then(|cc| {
            cc.split(',')
                .find_map(|directive| {
                    let directive = directive.trim();
                    if directive.starts_with("max-age=") {
                        directive[8..].parse::<i64>().ok()
                    } else {
                        None
                    }
                })
        });

        let expires_at = max_age
            .map(|secs| now + chrono::Duration::seconds(secs))
            .or_else(|| Some(now + chrono::Duration::from_std(default_ttl).unwrap_or_default()));

        Self {
            data,
            content_type: content_type.to_string(),
            etag,
            last_modified,
            expires_at,
            created_at: now,
            needs_revalidation: false,
        }
    }

    /// Check if the entry has expired
    pub fn is_expired(&self) -> bool {
        self.expires_at
            .map(|exp| Utc::now() > exp)
            .unwrap_or(false)
    }

    /// Check if the entry is stale but within stale-while-revalidate window
    pub fn is_stale_revalidatable(&self, stale_max_age: Duration) -> bool {
        if let Some(expires_at) = self.expires_at {
            let now = Utc::now();
            let stale_deadline = expires_at + chrono::Duration::from_std(stale_max_age).unwrap_or_default();
            now > expires_at && now <= stale_deadline
        } else {
            false
        }
    }

    /// Get the age of this entry
    pub fn age(&self) -> Duration {
        let age = Utc::now() - self.created_at;
        age.to_std().unwrap_or_default()
    }

    /// Get time to live
    pub fn ttl(&self) -> Option<Duration> {
        self.expires_at.and_then(|exp| {
            let remaining = exp - Utc::now();
            remaining.to_std().ok()
        })
    }

    /// Size of the cached data
    pub fn size(&self) -> usize {
        self.data.len()
    }

    /// Generate Cache-Control header value
    pub fn cache_control_header(&self) -> String {
        if let Some(ttl) = self.ttl() {
            format!("public, max-age={}", ttl.as_secs())
        } else {
            "public, max-age=0, must-revalidate".to_string()
        }
    }

    /// Check if entry matches If-None-Match header
    pub fn matches_etag(&self, if_none_match: &str) -> bool {
        if let Some(ref etag) = self.etag {
            // Handle weak ETags and wildcards
            if if_none_match == "*" {
                return true;
            }
            
            // Compare strong parts
            let self_strong = etag.strip_prefix("W/").unwrap_or(etag);
            
            for tag in if_none_match.split(',') {
                let tag = tag.trim();
                let tag_strong = tag.strip_prefix("W/").unwrap_or(tag);
                if tag_strong == self_strong {
                    return true;
                }
            }
        }
        false
    }

    /// Check if entry is modified since timestamp
    pub fn is_modified_since(&self, if_modified_since: DateTime<Utc>) -> bool {
        self.last_modified
            .map(|lm| lm > if_modified_since)
            .unwrap_or(true)
    }
}

/// Serde helper for Bytes
mod bytes_serde {
    use bytes::Bytes;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(bytes: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(bytes)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Visitor;

        struct BytesVisitor;

        impl<'de> Visitor<'de> for BytesVisitor {
            type Value = Bytes;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("bytes")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Bytes::copy_from_slice(v))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut vec = Vec::new();
                while let Some(byte) = seq.next_element()? {
                    vec.push(byte);
                }
                Ok(Bytes::from(vec))
            }
        }

        deserializer.deserialize_bytes(BytesVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_entry_expiry() {
        let entry = CacheEntry::new(
            Bytes::from("test"),
            "text/plain",
            Duration::from_secs(3600),
        );
        assert!(!entry.is_expired());
        assert!(entry.ttl().is_some());
    }

    #[test]
    fn test_cache_entry_from_s3_metadata() {
        let entry = CacheEntry::from_s3_metadata(
            Bytes::from("test"),
            "image/png",
            Some("\"abc123\"".to_string()),
            None,
            Some("max-age=300, public"),
            Duration::from_secs(3600),
        );

        assert_eq!(entry.content_type, "image/png");
        assert!(entry.ttl().unwrap().as_secs() <= 300);
    }

    #[test]
    fn test_etag_matching() {
        let entry = CacheEntry::new(
            Bytes::from("test"),
            "text/plain",
            Duration::from_secs(3600),
        );
        let entry = CacheEntry {
            etag: Some("\"abc123\"".to_string()),
            ..entry
        };

        assert!(entry.matches_etag("\"abc123\""));
        assert!(entry.matches_etag("W/\"abc123\""));
        assert!(entry.matches_etag("*"));
        assert!(!entry.matches_etag("\"xyz789\""));
    }
}
