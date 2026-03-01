//! Disk-based cache for cold tier storage.

use crate::cache::{CacheEntry, CacheError, CacheResult};
use crate::types::CacheKey;
use futures::future::BoxFuture;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, error, warn};

/// Disk cache statistics
#[derive(Debug, Clone)]
pub struct DiskCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub writes: u64,
    pub evictions: u64,
    pub size_bytes: u64,
    pub max_size_bytes: u64,
}

/// Index entry for tracking cached files
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct IndexEntry {
    path: String,
    size: u64,
    created_at: chrono::DateTime<chrono::Utc>,
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Disk-based cache for larger objects
pub struct DiskCache {
    base_path: PathBuf,
    max_size: u64,
    current_size: Arc<AtomicU64>,
    hits: Arc<AtomicU64>,
    misses: Arc<AtomicU64>,
    writes: Arc<AtomicU64>,
    evictions: Arc<AtomicU64>,
    // In production, this would be a persistent index
    // For now, we use files directly with metadata
}

impl DiskCache {
    /// Create a new disk cache
    pub async fn new(base_path: &Path, max_size: u64) -> CacheResult<Self> {
        // Create base directory if it doesn't exist
        fs::create_dir_all(base_path).await?;

        // Create subdirectories for sharding (00-ff)
        for i in 0u8..=255 {
            let shard_dir = base_path.join(format!("{:02x}", i));
            if !shard_dir.exists() {
                fs::create_dir(&shard_dir).await.ok();
            }
        }

        // Calculate current size
        let current_size = Self::calculate_size(base_path).await;

        Ok(Self {
            base_path: base_path.to_path_buf(),
            max_size,
            current_size: Arc::new(AtomicU64::new(current_size)),
            hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
            writes: Arc::new(AtomicU64::new(0)),
            evictions: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Calculate total size of cache directory
    fn calculate_size(path: &Path) -> BoxFuture<'static, u64> {
        let path = path.to_path_buf();
        Box::pin(async move {
            let mut total = 0u64;
            
            if let Ok(mut entries) = fs::read_dir(&path).await {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    let entry_path = entry.path();
                    if entry_path.is_dir() {
                        // Recurse into subdirectories
                        total += Self::calculate_size(&entry_path).await;
                    } else if let Ok(metadata) = entry.metadata().await {
                        total += metadata.len();
                    }
                }
            }
            
            total
        })
    }

    /// Get the file path for a cache key
    fn get_file_path(&self, key: &CacheKey) -> PathBuf {
        let disk_path = key.disk_path();
        self.base_path.join(disk_path)
    }

    /// Get metadata file path for a cache key
    fn get_meta_path(&self, key: &CacheKey) -> PathBuf {
        let disk_path = key.disk_path();
        self.base_path.join(format!("{}.meta", disk_path))
    }

    /// Get an entry from the disk cache
    pub async fn get(&self, key: &CacheKey) -> CacheResult<CacheEntry> {
        let file_path = self.get_file_path(key);
        let meta_path = self.get_meta_path(key);

        // Check if files exist
        if !file_path.exists() || !meta_path.exists() {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return Err(CacheError::Miss);
        }

        // Read metadata
        let meta_bytes = fs::read(&meta_path).await.map_err(|e| {
            error!(error = %e, path = ?meta_path, "Failed to read cache metadata");
            CacheError::Miss
        })?;

        let meta: CacheEntryMeta = serde_json::from_slice(&meta_bytes).map_err(|e| {
            warn!(error = %e, "Failed to parse cache metadata");
            CacheError::Miss
        })?;

        // Check expiry
        if let Some(expires_at) = meta.expires_at {
            if chrono::Utc::now() > expires_at {
                debug!("Cache entry expired");
                self.misses.fetch_add(1, Ordering::Relaxed);
                // Clean up expired entry
                tokio::spawn(async move {
                    fs::remove_file(&file_path).await.ok();
                    fs::remove_file(&meta_path).await.ok();
                });
                return Err(CacheError::Expired);
            }
        }

        // Read data
        let data = fs::read(&file_path).await.map_err(|e| {
            error!(error = %e, path = ?file_path, "Failed to read cache file");
            CacheError::Miss
        })?;

        self.hits.fetch_add(1, Ordering::Relaxed);

        Ok(CacheEntry {
            data: bytes::Bytes::from(data),
            content_type: meta.content_type,
            etag: meta.etag,
            last_modified: meta.last_modified,
            expires_at: meta.expires_at,
            created_at: meta.created_at,
            needs_revalidation: false,
        })
    }

    /// Store an entry in the disk cache
    pub async fn put(&self, key: &CacheKey, entry: CacheEntry) -> CacheResult<()> {
        let file_path = self.get_file_path(key);
        let meta_path = self.get_meta_path(key);

        // Create parent directory if needed
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let entry_size = entry.data.len() as u64;

        // Check if we need to evict entries
        self.maybe_evict(entry_size).await?;

        // Write data file
        let mut file = fs::File::create(&file_path).await?;
        file.write_all(&entry.data).await?;
        file.sync_all().await?;

        // Write metadata
        let meta = CacheEntryMeta {
            content_type: entry.content_type,
            etag: entry.etag,
            last_modified: entry.last_modified,
            expires_at: entry.expires_at,
            created_at: entry.created_at,
            size: entry_size,
        };

        let meta_bytes = serde_json::to_vec(&meta).map_err(|e| {
            CacheError::Serialization(e.to_string())
        })?;

        fs::write(&meta_path, &meta_bytes).await?;

        // Update size tracking
        self.current_size.fetch_add(entry_size, Ordering::Relaxed);
        self.writes.fetch_add(1, Ordering::Relaxed);

        debug!(
            path = ?file_path,
            size = entry_size,
            "Wrote cache entry to disk"
        );

        Ok(())
    }

    /// Invalidate a cache entry
    pub async fn invalidate(&self, key: &CacheKey) -> CacheResult<()> {
        let file_path = self.get_file_path(key);
        let meta_path = self.get_meta_path(key);

        // Get size before deletion
        let size = fs::metadata(&file_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0);

        fs::remove_file(&file_path).await.ok();
        fs::remove_file(&meta_path).await.ok();

        // Update size tracking
        self.current_size.fetch_sub(size, Ordering::Relaxed);

        Ok(())
    }

    /// Evict entries if we're over capacity
    async fn maybe_evict(&self, needed_size: u64) -> CacheResult<()> {
        let current = self.current_size.load(Ordering::Relaxed);
        
        if current + needed_size <= self.max_size {
            return Ok(());
        }

        let target_free = (self.max_size as f64 * 0.1) as u64; // Free 10% of cache
        let to_free = (current + needed_size - self.max_size) + target_free;

        debug!(
            current_size = current,
            max_size = self.max_size,
            to_free = to_free,
            "Evicting cache entries"
        );

        // Simple LRU eviction based on access time
        let mut entries = Vec::new();
        self.collect_entries(&self.base_path, &mut entries).await;

        // Sort by access time (oldest first)
        entries.sort_by_key(|(_, atime)| *atime);

        let mut freed = 0u64;
        for (path, _) in entries {
            if freed >= to_free {
                break;
            }

            if let Ok(metadata) = fs::metadata(&path).await {
                let size = metadata.len();
                fs::remove_file(&path).await.ok();
                
                // Remove associated metadata file
                let meta_path = PathBuf::from(format!("{}.meta", path.display()));
                fs::remove_file(&meta_path).await.ok();

                freed += size;
                self.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }

        self.current_size.fetch_sub(freed, Ordering::Relaxed);

        Ok(())
    }

    /// Collect all cache entries with access times
    async fn collect_entries(
        &self,
        path: &Path,
        entries: &mut Vec<(PathBuf, std::time::SystemTime)>,
    ) {
        if let Ok(mut dir) = fs::read_dir(path).await {
            while let Ok(Some(entry)) = dir.next_entry().await {
                let path = entry.path();
                
                if path.is_dir() {
                    Box::pin(self.collect_entries(&path, entries)).await;
                } else if !path.extension().map(|e| e == "meta").unwrap_or(false) {
                    if let Ok(metadata) = fs::metadata(&path).await {
                        let atime = metadata.accessed().unwrap_or(std::time::SystemTime::UNIX_EPOCH);
                        entries.push((path, atime));
                    }
                }
            }
        }
    }

    /// Get cache statistics
    pub fn stats(&self) -> DiskCacheStats {
        DiskCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            writes: self.writes.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            size_bytes: self.current_size.load(Ordering::Relaxed),
            max_size_bytes: self.max_size,
        }
    }

    /// Flush all pending writes
    pub async fn flush(&self) -> CacheResult<()> {
        // sync is handled per-file during write
        Ok(())
    }

    /// Clear the entire cache
    pub async fn clear(&self) -> CacheResult<()> {
        for i in 0u8..=255 {
            let shard_dir = self.base_path.join(format!("{:02x}", i));
            if shard_dir.exists() {
                fs::remove_dir_all(&shard_dir).await.ok();
                fs::create_dir(&shard_dir).await.ok();
            }
        }
        self.current_size.store(0, Ordering::Relaxed);
        Ok(())
    }
}

/// Serializable cache entry metadata
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct CacheEntryMeta {
    content_type: String,
    etag: Option<String>,
    last_modified: Option<chrono::DateTime<chrono::Utc>>,
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
    created_at: chrono::DateTime<chrono::Utc>,
    size: u64,
}

impl Clone for DiskCache {
    fn clone(&self) -> Self {
        Self {
            base_path: self.base_path.clone(),
            max_size: self.max_size,
            current_size: self.current_size.clone(),
            hits: self.hits.clone(),
            misses: self.misses.clone(),
            writes: self.writes.clone(),
            evictions: self.evictions.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::time::Duration;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_disk_cache_basic() {
        let dir = tempdir().unwrap();
        let cache = DiskCache::new(dir.path(), 100 * 1024 * 1024).await.unwrap();

        let key = CacheKey::new(
            crate::types::BucketName::new_unchecked("bucket"),
            crate::types::ObjectKey::new_unchecked("test/key.txt"),
        );

        let entry = CacheEntry::new(
            Bytes::from("hello world"),
            "text/plain",
            Duration::from_secs(3600),
        );

        cache.put(&key, entry.clone()).await.unwrap();

        let retrieved = cache.get(&key).await.unwrap();
        assert_eq!(retrieved.data, entry.data);
        assert_eq!(retrieved.content_type, entry.content_type);
    }

    #[tokio::test]
    async fn test_disk_cache_miss() {
        let dir = tempdir().unwrap();
        let cache = DiskCache::new(dir.path(), 100 * 1024 * 1024).await.unwrap();

        let key = CacheKey::new(
            crate::types::BucketName::new_unchecked("bucket"),
            crate::types::ObjectKey::new_unchecked("nonexistent"),
        );

        let result = cache.get(&key).await;
        assert!(matches!(result, Err(CacheError::Miss)));
    }

    #[tokio::test]
    async fn test_disk_cache_invalidate() {
        let dir = tempdir().unwrap();
        let cache = DiskCache::new(dir.path(), 100 * 1024 * 1024).await.unwrap();

        let key = CacheKey::new(
            crate::types::BucketName::new_unchecked("bucket"),
            crate::types::ObjectKey::new_unchecked("to-delete"),
        );

        let entry = CacheEntry::new(
            Bytes::from("delete me"),
            "text/plain",
            Duration::from_secs(3600),
        );

        cache.put(&key, entry).await.unwrap();
        assert!(cache.get(&key).await.is_ok());

        cache.invalidate(&key).await.unwrap();
        assert!(cache.get(&key).await.is_err());
    }
}
