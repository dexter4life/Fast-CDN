//! Request collapsing (thundering herd protection).
//!
//! When multiple concurrent requests miss the cache for the same object,
//! only one request is sent to the origin. Other requests wait and share
//! the result.

use crate::cache::CacheEntry;
use crate::types::CacheKey;
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{debug, info};

/// Result of a collapsed request
#[derive(Debug)]
pub enum CollapseResult {
    /// This request should fetch from origin
    Leader,
    /// This request should wait for the leader
    Follower(broadcast::Receiver<Arc<CollapsedResponse>>),
}

/// Response shared between collapsed requests
#[derive(Debug, Clone)]
pub struct CollapsedResponse {
    pub entry: Option<CacheEntry>,
    pub error: Option<String>,
}

impl CollapsedResponse {
    pub fn success(entry: CacheEntry) -> Self {
        Self {
            entry: Some(entry),
            error: None,
        }
    }

    pub fn error(msg: impl Into<String>) -> Self {
        Self {
            entry: None,
            error: Some(msg.into()),
        }
    }
}

/// In-flight request tracking
#[derive(Clone)]
struct InFlightRequest {
    sender: broadcast::Sender<Arc<CollapsedResponse>>,
    created_at: std::time::Instant,
}

/// Request collapser for thundering herd protection
pub struct RequestCollapser {
    in_flight: DashMap<String, InFlightRequest>,
    timeout: Duration,
}

impl RequestCollapser {
    /// Create a new request collapser
    pub fn new(timeout: Duration) -> Self {
        Self {
            in_flight: DashMap::new(),
            timeout,
        }
    }

    /// Try to acquire a lock for a cache key.
    /// Returns `Leader` if this request should fetch from origin,
    /// or `Follower` with a receiver to wait for the result.
    pub fn try_acquire(&self, key: &CacheKey) -> CollapseResult {
        let cache_key = key.to_string();

        // Clean up expired entries first
        self.cleanup_expired();

        // Try to get existing in-flight request
        if let Some(existing) = self.in_flight.get(&cache_key) {
            debug!(key = %cache_key, "Joining existing in-flight request");
            return CollapseResult::Follower(existing.sender.subscribe());
        }

        // No existing request, become the leader
        let (sender, _) = broadcast::channel(1);
        let request = InFlightRequest {
            sender: sender.clone(),
            created_at: std::time::Instant::now(),
        };

        // Race to insert
        match self.in_flight.entry(cache_key.clone()) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                // Someone else won the race
                debug!(key = %cache_key, "Lost race, joining existing request");
                CollapseResult::Follower(entry.get().sender.subscribe())
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(request);
                debug!(key = %cache_key, "Became leader for in-flight request");
                CollapseResult::Leader
            }
        }
    }

    /// Complete an in-flight request (called by leader)
    pub fn complete(&self, key: &CacheKey, response: CollapsedResponse) {
        let cache_key = key.to_string();

        if let Some((_, request)) = self.in_flight.remove(&cache_key) {
            let subscriber_count = request.sender.receiver_count();
            let _ = request.sender.send(Arc::new(response));
            
            if subscriber_count > 0 {
                info!(
                    key = %cache_key,
                    collapsed_requests = subscriber_count,
                    "Completed collapsed request"
                );
            }
        }
    }

    /// Cancel an in-flight request
    pub fn cancel(&self, key: &CacheKey) {
        let cache_key = key.to_string();
        self.in_flight.remove(&cache_key);
    }

    /// Get the number of in-flight requests
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }

    /// Clean up expired in-flight requests
    fn cleanup_expired(&self) {
        let now = std::time::Instant::now();
        self.in_flight.retain(|_, request| {
            now.duration_since(request.created_at) < self.timeout
        });
    }
}

impl Clone for RequestCollapser {
    fn clone(&self) -> Self {
        // Share the same in-flight map
        Self {
            in_flight: self.in_flight.clone(),
            timeout: self.timeout,
        }
    }
}

/// Guard for automatically completing/canceling in-flight requests
pub struct CollapseGuard<'a> {
    collapser: &'a RequestCollapser,
    key: CacheKey,
    completed: bool,
}

impl<'a> CollapseGuard<'a> {
    pub fn new(collapser: &'a RequestCollapser, key: CacheKey) -> Self {
        Self {
            collapser,
            key,
            completed: false,
        }
    }

    pub fn complete(mut self, response: CollapsedResponse) {
        self.completed = true;
        self.collapser.complete(&self.key, response);
    }
}

impl Drop for CollapseGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            // Cancel the request if not completed (e.g., due to panic)
            self.collapser.cancel(&self.key);
        }
    }
}

/// Helper for streaming collapsed responses
pub struct StreamingCollapser {
    in_flight: DashMap<String, StreamingInFlight>,
    timeout: Duration,
}

#[derive(Clone)]
struct StreamingInFlight {
    buffer: Arc<parking_lot::RwLock<StreamBuffer>>,
    created_at: std::time::Instant,
}

struct StreamBuffer {
    chunks: Vec<Bytes>,
    complete: bool,
    error: Option<String>,
    metadata: Option<StreamMetadata>,
}

#[derive(Debug, Clone)]
pub struct StreamMetadata {
    pub content_type: String,
    pub content_length: Option<u64>,
    pub etag: Option<String>,
}

impl StreamingCollapser {
    pub fn new(timeout: Duration) -> Self {
        Self {
            in_flight: DashMap::new(),
            timeout,
        }
    }

    /// Start a new streaming request
    pub fn start(&self, key: &CacheKey) -> Option<StreamingLeader> {
        let cache_key = key.to_string();

        // Clean up expired
        self.cleanup_expired();

        let buffer = Arc::new(parking_lot::RwLock::new(StreamBuffer {
            chunks: Vec::new(),
            complete: false,
            error: None,
            metadata: None,
        }));

        let in_flight = StreamingInFlight {
            buffer: buffer.clone(),
            created_at: std::time::Instant::now(),
        };

        match self.in_flight.entry(cache_key) {
            dashmap::mapref::entry::Entry::Occupied(_) => None,
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(in_flight);
                Some(StreamingLeader { buffer })
            }
        }
    }

    /// Join an existing streaming request
    pub fn join(&self, key: &CacheKey) -> Option<StreamingFollower> {
        let cache_key = key.to_string();
        
        self.in_flight.get(&cache_key).map(|entry| StreamingFollower {
            buffer: entry.buffer.clone(),
            read_position: 0,
        })
    }

    /// Remove completed request
    pub fn complete(&self, key: &CacheKey) {
        let cache_key = key.to_string();
        self.in_flight.remove(&cache_key);
    }

    fn cleanup_expired(&self) {
        let now = std::time::Instant::now();
        self.in_flight.retain(|_, request| {
            now.duration_since(request.created_at) < self.timeout
        });
    }
}

/// Leader handle for streaming response
pub struct StreamingLeader {
    buffer: Arc<parking_lot::RwLock<StreamBuffer>>,
}

impl StreamingLeader {
    /// Set metadata for this stream
    pub fn set_metadata(&self, metadata: StreamMetadata) {
        self.buffer.write().metadata = Some(metadata);
    }

    /// Push a chunk to waiting followers
    pub fn push_chunk(&self, chunk: Bytes) {
        self.buffer.write().chunks.push(chunk);
    }

    /// Mark stream as complete
    pub fn complete(&self) {
        self.buffer.write().complete = true;
    }

    /// Mark stream as failed
    pub fn fail(&self, error: impl Into<String>) {
        let mut buf = self.buffer.write();
        buf.error = Some(error.into());
        buf.complete = true;
    }
}

/// Follower handle for reading streaming response
pub struct StreamingFollower {
    buffer: Arc<parking_lot::RwLock<StreamBuffer>>,
    read_position: usize,
}

impl StreamingFollower {
    /// Get metadata (blocks until available)
    pub async fn metadata(&self) -> Option<StreamMetadata> {
        loop {
            {
                let buf = self.buffer.read();
                if buf.metadata.is_some() || buf.complete {
                    return buf.metadata.clone();
                }
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Read the next available chunk
    pub async fn next_chunk(&mut self) -> Option<Result<Bytes, String>> {
        loop {
            {
                let buf = self.buffer.read();
                
                // Check for new chunks
                if self.read_position < buf.chunks.len() {
                    let chunk = buf.chunks[self.read_position].clone();
                    self.read_position += 1;
                    return Some(Ok(chunk));
                }

                // Check if complete
                if buf.complete {
                    if let Some(ref error) = buf.error {
                        return Some(Err(error.clone()));
                    }
                    return None;
                }
            }
            
            // Wait for more data
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn test_request_collapsing() {
        let collapser = RequestCollapser::new(Duration::from_secs(30));

        let key = CacheKey::new(
            crate::types::BucketName::new_unchecked("bucket"),
            crate::types::ObjectKey::new_unchecked("test"),
        );

        // First request becomes leader
        let result1 = collapser.try_acquire(&key);
        assert!(matches!(result1, CollapseResult::Leader));

        // Second request becomes follower
        let result2 = collapser.try_acquire(&key);
        assert!(matches!(result2, CollapseResult::Follower(_)));

        // Complete the request
        let entry = CacheEntry::new(
            Bytes::from("test data"),
            "text/plain",
            Duration::from_secs(3600),
        );
        collapser.complete(&key, CollapsedResponse::success(entry.clone()));

        // Follower should receive the response
        if let CollapseResult::Follower(mut rx) = result2 {
            let response = rx.recv().await.unwrap();
            assert!(response.entry.is_some());
            assert_eq!(response.entry.as_ref().unwrap().data, entry.data);
        }
    }

    #[tokio::test]
    async fn test_multiple_followers() {
        let collapser = Arc::new(RequestCollapser::new(Duration::from_secs(30)));
        let fetch_count = Arc::new(AtomicU32::new(0));

        let key = CacheKey::new(
            crate::types::BucketName::new_unchecked("bucket"),
            crate::types::ObjectKey::new_unchecked("popular"),
        );

        let mut handles = Vec::new();

        // Spawn 10 concurrent requests
        for _ in 0..10 {
            let collapser = collapser.clone();
            let key = key.clone();
            let fetch_count = fetch_count.clone();

            handles.push(tokio::spawn(async move {
                match collapser.try_acquire(&key) {
                    CollapseResult::Leader => {
                        // Simulate fetch
                        fetch_count.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(10)).await;

                        let entry = CacheEntry::new(
                            Bytes::from("shared data"),
                            "text/plain",
                            Duration::from_secs(3600),
                        );
                        collapser.complete(&key, CollapsedResponse::success(entry));
                    }
                    CollapseResult::Follower(mut rx) => {
                        let _ = rx.recv().await;
                    }
                }
            }));
        }

        // Wait for all requests
        for handle in handles {
            handle.await.unwrap();
        }

        // Only one fetch should have happened
        assert_eq!(fetch_count.load(Ordering::SeqCst), 1);
    }
}
