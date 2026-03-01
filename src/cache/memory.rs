//! In-memory cache using moka (TinyLFU-based).

use crate::cache::CacheEntry;
use crate::types::CacheKey;
use moka::sync::Cache;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Memory cache statistics
#[derive(Debug, Clone)]
pub struct MemoryCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub entries: u64,
    pub size_bytes: u64,
    pub max_size_bytes: u64,
}

/// In-memory LRU/TinyLFU cache implementation using moka
pub struct MemoryCache {
    cache: Cache<String, CacheEntry>,
    max_item_size: usize,
    hits: Arc<AtomicU64>,
    misses: Arc<AtomicU64>,
}

impl MemoryCache {
    /// Create a new memory cache with specified size limits
    pub fn new(max_size: usize, max_item_size: usize) -> Self {
        let cache = Cache::builder()
            .max_capacity(max_size as u64)
            .weigher(|_key: &String, entry: &CacheEntry| -> u32 {
                // Weight by actual memory usage
                (entry.data.len() + entry.content_type.len() + 100) as u32
            })
            .build();

        Self {
            cache,
            max_item_size,
            hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Get an entry from the cache
    pub fn get(&self, key: &CacheKey) -> Option<CacheEntry> {
        let cache_key = key.to_string();
        
        match self.cache.get(&cache_key) {
            Some(entry) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(entry)
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Store an entry in the cache
    pub fn put(&self, key: CacheKey, entry: CacheEntry) {
        // Don't cache items that are too large
        if entry.data.len() > self.max_item_size {
            return;
        }

        let cache_key = key.to_string();
        self.cache.insert(cache_key, entry);
    }

    /// Invalidate a cache entry
    pub fn invalidate(&self, key: &CacheKey) {
        let cache_key = key.to_string();
        self.cache.invalidate(&cache_key);
    }

    /// Clear all entries
    pub fn clear(&self) {
        self.cache.invalidate_all();
    }

    /// Get cache statistics
    pub fn stats(&self) -> MemoryCacheStats {
        MemoryCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            entries: self.cache.entry_count(),
            size_bytes: self.cache.weighted_size(),
            max_size_bytes: self.cache.policy().max_capacity().unwrap_or(0),
        }
    }

    /// Run maintenance tasks
    pub fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks();
    }
}

impl Clone for MemoryCache {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            max_item_size: self.max_item_size,
            hits: self.hits.clone(),
            misses: self.misses.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::time::Duration;

    #[test]
    fn test_memory_cache_basic() {
        let cache = MemoryCache::new(1024 * 1024, 100 * 1024);

        let key = CacheKey::new(
            crate::types::BucketName::new_unchecked("bucket"),
            crate::types::ObjectKey::new_unchecked("key"),
        );

        let entry = CacheEntry::new(
            Bytes::from("hello world"),
            "text/plain",
            Duration::from_secs(3600),
        );

        cache.put(key.clone(), entry.clone());
        cache.run_pending_tasks();

        let retrieved = cache.get(&key).unwrap();
        assert_eq!(retrieved.data, entry.data);

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 0);
    }

    #[test]
    fn test_memory_cache_miss() {
        let cache = MemoryCache::new(1024 * 1024, 100 * 1024);

        let key = CacheKey::new(
            crate::types::BucketName::new_unchecked("bucket"),
            crate::types::ObjectKey::new_unchecked("nonexistent"),
        );

        let result = cache.get(&key);
        assert!(result.is_none());

        let stats = cache.stats();
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn test_memory_cache_invalidate() {
        let cache = MemoryCache::new(1024 * 1024, 100 * 1024);

        let key = CacheKey::new(
            crate::types::BucketName::new_unchecked("bucket"),
            crate::types::ObjectKey::new_unchecked("key"),
        );

        let entry = CacheEntry::new(
            Bytes::from("hello"),
            "text/plain",
            Duration::from_secs(3600),
        );

        cache.put(key.clone(), entry);
        cache.run_pending_tasks();
        
        assert!(cache.get(&key).is_some());
        
        cache.invalidate(&key);
        cache.run_pending_tasks();
        
        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn test_memory_cache_rejects_large_items() {
        let cache = MemoryCache::new(1024 * 1024, 100); // 100 byte max item size

        let key = CacheKey::new(
            crate::types::BucketName::new_unchecked("bucket"),
            crate::types::ObjectKey::new_unchecked("key"),
        );

        let large_entry = CacheEntry::new(
            Bytes::from(vec![0u8; 200]), // 200 bytes, larger than max
            "application/octet-stream",
            Duration::from_secs(3600),
        );

        cache.put(key.clone(), large_entry);
        cache.run_pending_tasks();

        // Should not be cached
        assert!(cache.get(&key).is_none());
    }
}
