//! Two-tier caching module with request collapsing.
//!
//! - Hot tier: In-memory LRU/TinyLFU cache using moka
//! - Cold tier: Disk-based cache for larger objects

mod memory;
mod disk;
mod collapse;
mod entry;

pub use memory::*;
pub use disk::*;
pub use collapse::*;
pub use entry::*;

use crate::config::CacheConfig;
use crate::types::CacheKey;
use bytes::Bytes;
use std::sync::Arc;
use tracing::{debug, instrument};

/// Result type for cache operations
pub type CacheResult<T> = Result<T, CacheError>;

/// Cache errors
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("Cache miss")]
    Miss,

    #[error("Cache entry expired")]
    Expired,

    #[error("Disk I/O error: {0}")]
    DiskIo(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Entry too large: {size} > {max}")]
    TooLarge { size: usize, max: usize },
}

/// Two-tier hybrid cache
pub struct HybridCache {
    memory_cache: Option<MemoryCache>,
    disk_cache: Option<DiskCache>,
    collapsing: RequestCollapser,
    config: CacheConfig,
}

impl HybridCache {
    /// Create a new hybrid cache from configuration
    pub async fn new(config: CacheConfig) -> CacheResult<Self> {
        let memory_cache = if config.enable_memory_cache {
            Some(MemoryCache::new(
                config.memory_cache_size,
                config.memory_max_item_size,
            ))
        } else {
            None
        };

        let disk_cache = if config.enable_disk_cache {
            Some(DiskCache::new(&config.disk_cache_path, config.disk_cache_size).await?)
        } else {
            None
        };

        let collapsing = RequestCollapser::new(config.collapse_timeout);

        Ok(Self {
            memory_cache,
            disk_cache,
            collapsing,
            config,
        })
    }

    /// Get an entry from the cache
    #[instrument(skip(self), fields(key = %key))]
    pub async fn get(&self, key: &CacheKey) -> CacheResult<CacheEntry> {
        // Check memory cache first
        if let Some(ref cache) = self.memory_cache {
            if let Some(entry) = cache.get(key) {
                if !entry.is_expired() {
                    debug!("Memory cache hit");
                    return Ok(entry);
                } else if self.config.stale_while_revalidate {
                    debug!("Serving stale entry while revalidating");
                    // Return stale entry but mark for revalidation
                    let mut stale = entry;
                    stale.needs_revalidation = true;
                    return Ok(stale);
                }
            }
        }

        // Check disk cache
        if let Some(ref cache) = self.disk_cache {
            if let Ok(entry) = cache.get(key).await {
                if !entry.is_expired() {
                    debug!("Disk cache hit");
                    // Promote to memory cache if small enough
                    self.promote_to_memory(key, &entry);
                    return Ok(entry);
                }
            }
        }

        debug!("Cache miss");
        Err(CacheError::Miss)
    }

    /// Store an entry in the cache
    #[instrument(skip(self, entry), fields(key = %key, size = entry.data.len()))]
    pub async fn put(&self, key: &CacheKey, entry: CacheEntry) -> CacheResult<()> {
        let size = entry.data.len();

        // Store in memory cache if small enough
        if let Some(ref cache) = self.memory_cache {
            if size <= self.config.memory_max_item_size {
                cache.put(key.clone(), entry.clone());
                debug!("Stored in memory cache");
            }
        }

        // Store in disk cache if within limits
        if let Some(ref cache) = self.disk_cache {
            if size as u64 <= self.config.disk_max_item_size {
                cache.put(key, entry).await?;
                debug!("Stored in disk cache");
            }
        }

        Ok(())
    }

    /// Invalidate a cache entry
    pub async fn invalidate(&self, key: &CacheKey) -> CacheResult<()> {
        if let Some(ref cache) = self.memory_cache {
            cache.invalidate(key);
        }

        if let Some(ref cache) = self.disk_cache {
            cache.invalidate(key).await?;
        }

        Ok(())
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let memory_stats = self.memory_cache.as_ref().map(|c| c.stats());
        let disk_stats = self.disk_cache.as_ref().map(|c| c.stats());

        CacheStats {
            memory: memory_stats,
            disk: disk_stats,
        }
    }

    /// Get the request collapser for this cache
    pub fn collapser(&self) -> &RequestCollapser {
        &self.collapsing
    }

    /// Promote a disk entry to memory cache
    fn promote_to_memory(&self, key: &CacheKey, entry: &CacheEntry) {
        if let Some(ref cache) = self.memory_cache {
            if entry.data.len() <= self.config.memory_max_item_size {
                cache.put(key.clone(), entry.clone());
            }
        }
    }

    /// Flush disk cache
    pub async fn flush(&self) -> CacheResult<()> {
        if let Some(ref cache) = self.disk_cache {
            cache.flush().await?;
        }
        Ok(())
    }
}

/// Combined cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub memory: Option<MemoryCacheStats>,
    pub disk: Option<DiskCacheStats>,
}

impl CacheStats {
    /// Calculate overall hit ratio
    pub fn hit_ratio(&self) -> f64 {
        let mut total_hits = 0u64;
        let mut total_requests = 0u64;

        if let Some(ref mem) = self.memory {
            total_hits += mem.hits;
            total_requests += mem.hits + mem.misses;
        }

        if let Some(ref disk) = self.disk {
            total_hits += disk.hits;
            total_requests += disk.hits + disk.misses;
        }

        if total_requests > 0 {
            total_hits as f64 / total_requests as f64
        } else {
            0.0
        }
    }
}

/// Shared cache reference
pub type SharedCache = Arc<HybridCache>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_hybrid_cache() {
        let dir = tempdir().unwrap();
        let config = CacheConfig {
            enable_memory_cache: true,
            memory_cache_size: 10 * 1024 * 1024,
            memory_max_item_size: 1024 * 1024,
            enable_disk_cache: true,
            disk_cache_path: dir.path().to_path_buf(),
            disk_cache_size: 100 * 1024 * 1024,
            disk_max_item_size: 10 * 1024 * 1024,
            default_ttl: Duration::from_secs(3600),
            honor_cache_control: true,
            stale_while_revalidate: false,
            stale_max_age: Duration::from_secs(86400),
            enable_request_collapsing: true,
            collapse_timeout: Duration::from_secs(30),
        };

        let cache = HybridCache::new(config).await.unwrap();

        let key = CacheKey::new(
            crate::types::BucketName::new_unchecked("test-bucket"),
            crate::types::ObjectKey::new_unchecked("test-key"),
        );

        let entry = CacheEntry {
            data: Bytes::from("hello world"),
            content_type: "text/plain".to_string(),
            etag: Some("abc123".to_string()),
            last_modified: None,
            expires_at: Some(chrono::Utc::now() + chrono::Duration::hours(1)),
            created_at: chrono::Utc::now(),
            needs_revalidation: false,
        };

        cache.put(&key, entry.clone()).await.unwrap();

        let retrieved = cache.get(&key).await.unwrap();
        assert_eq!(retrieved.data, entry.data);
    }
}
