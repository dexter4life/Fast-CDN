//! Rate limiting using token bucket algorithm.

use dashmap::DashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::warn;

/// Rate limiter configuration
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Requests per second
    pub requests_per_second: u32,
    /// Burst capacity
    pub burst: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            requests_per_second: 1000,
            burst: 5000,
        }
    }
}

/// Token bucket for rate limiting
#[derive(Debug, Clone)]
struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
    capacity: f64,
    refill_rate: f64, // tokens per second
}

impl TokenBucket {
    fn new(capacity: u32, refill_rate: u32) -> Self {
        Self {
            tokens: capacity as f64,
            last_refill: Instant::now(),
            capacity: capacity as f64,
            refill_rate: refill_rate as f64,
        }
    }

    fn try_consume(&mut self, tokens: f64) -> bool {
        self.refill();

        if self.tokens >= tokens {
            self.tokens -= tokens;
            true
        } else {
            false
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill);
        let new_tokens = elapsed.as_secs_f64() * self.refill_rate;

        self.tokens = (self.tokens + new_tokens).min(self.capacity);
        self.last_refill = now;
    }
}

/// Rate limiter implementation
pub struct RateLimiter {
    buckets: DashMap<IpAddr, TokenBucket>,
    config: RateLimitConfig,
    cleanup_interval: Duration,
    last_cleanup: parking_lot::Mutex<Instant>,
}

impl RateLimiter {
    /// Create a new rate limiter
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            buckets: DashMap::new(),
            config,
            cleanup_interval: Duration::from_secs(60),
            last_cleanup: parking_lot::Mutex::new(Instant::now()),
        }
    }

    /// Check if a request from the given IP is allowed
    pub fn check(&self, ip: IpAddr) -> bool {
        self.maybe_cleanup();

        let mut bucket = self
            .buckets
            .entry(ip)
            .or_insert_with(|| TokenBucket::new(self.config.burst, self.config.requests_per_second));

        let allowed = bucket.try_consume(1.0);

        if !allowed {
            warn!(ip = %ip, "Rate limit exceeded");
        }

        allowed
    }

    /// Get remaining tokens for an IP
    pub fn remaining(&self, ip: &IpAddr) -> u32 {
        self.buckets
            .get(ip)
            .map(|b| b.tokens as u32)
            .unwrap_or(self.config.burst)
    }

    /// Reset rate limit for an IP
    pub fn reset(&self, ip: &IpAddr) {
        self.buckets.remove(ip);
    }

    /// Cleanup old buckets
    fn maybe_cleanup(&self) {
        let mut last_cleanup = self.last_cleanup.lock();
        if last_cleanup.elapsed() < self.cleanup_interval {
            return;
        }

        *last_cleanup = Instant::now();
        drop(last_cleanup);

        // Remove buckets that are fully refilled (inactive)
        let now = Instant::now();
        let max_idle = Duration::from_secs(300); // 5 minutes

        self.buckets.retain(|_, bucket| {
            now.duration_since(bucket.last_refill) < max_idle
        });
    }

    /// Get statistics
    pub fn stats(&self) -> RateLimitStats {
        RateLimitStats {
            tracked_ips: self.buckets.len(),
        }
    }
}

impl Clone for RateLimiter {
    fn clone(&self) -> Self {
        Self {
            buckets: self.buckets.clone(),
            config: self.config.clone(),
            cleanup_interval: self.cleanup_interval,
            last_cleanup: parking_lot::Mutex::new(*self.last_cleanup.lock()),
        }
    }
}

/// Rate limiter statistics
#[derive(Debug, Clone)]
pub struct RateLimitStats {
    pub tracked_ips: usize,
}

/// Per-path rate limiting
pub struct PathRateLimiter {
    limiters: DashMap<String, Arc<RateLimiter>>,
    default_config: RateLimitConfig,
}

impl PathRateLimiter {
    pub fn new(default_config: RateLimitConfig) -> Self {
        Self {
            limiters: DashMap::new(),
            default_config,
        }
    }

    /// Configure rate limit for a specific path pattern
    pub fn configure_path(&self, path_pattern: &str, config: RateLimitConfig) {
        self.limiters.insert(
            path_pattern.to_string(),
            Arc::new(RateLimiter::new(config)),
        );
    }

    /// Check rate limit for a path and IP
    pub fn check(&self, path: &str, ip: IpAddr) -> bool {
        // Find matching path limiter
        for entry in self.limiters.iter() {
            if path.starts_with(entry.key()) {
                return entry.value().check(ip);
            }
        }

        // Use default limiter
        let default = self.limiters.entry("*".to_string()).or_insert_with(|| {
            Arc::new(RateLimiter::new(self.default_config.clone()))
        });
        default.check(ip)
    }
}

impl Clone for PathRateLimiter {
    fn clone(&self) -> Self {
        Self {
            limiters: self.limiters.clone(),
            default_config: self.default_config.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn test_rate_limiter_allow() {
        let config = RateLimitConfig {
            requests_per_second: 10,
            burst: 5,
        };
        let limiter = RateLimiter::new(config);
        let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

        // First 5 requests should be allowed (burst)
        for _ in 0..5 {
            assert!(limiter.check(ip));
        }
    }

    #[test]
    fn test_rate_limiter_deny() {
        let config = RateLimitConfig {
            requests_per_second: 1,
            burst: 2,
        };
        let limiter = RateLimiter::new(config);
        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1));

        // Exhaust burst
        assert!(limiter.check(ip));
        assert!(limiter.check(ip));
        
        // Should be denied
        assert!(!limiter.check(ip));
    }

    #[test]
    fn test_rate_limiter_refill() {
        let config = RateLimitConfig {
            requests_per_second: 100,
            burst: 1,
        };
        let limiter = RateLimiter::new(config);
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));

        // Use the one token
        assert!(limiter.check(ip));
        assert!(!limiter.check(ip));

        // Wait for refill (10ms for 1 token at 100/sec)
        std::thread::sleep(Duration::from_millis(15));

        // Should be allowed again
        assert!(limiter.check(ip));
    }

    #[test]
    fn test_per_ip_limits() {
        let config = RateLimitConfig {
            requests_per_second: 1,
            burst: 1,
        };
        let limiter = RateLimiter::new(config);
        
        let ip1 = IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1));
        let ip2 = IpAddr::V4(Ipv4Addr::new(2, 2, 2, 2));

        // Each IP has its own bucket
        assert!(limiter.check(ip1));
        assert!(limiter.check(ip2));

        // Both should now be limited
        assert!(!limiter.check(ip1));
        assert!(!limiter.check(ip2));
    }

    #[test]
    fn test_remaining_tokens() {
        let config = RateLimitConfig {
            requests_per_second: 10,
            burst: 10,
        };
        let limiter = RateLimiter::new(config);
        let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

        assert_eq!(limiter.remaining(&ip), 10);

        limiter.check(ip);
        // Should have ~9 tokens remaining
        assert!(limiter.remaining(&ip) >= 8 && limiter.remaining(&ip) <= 10);
    }
}
