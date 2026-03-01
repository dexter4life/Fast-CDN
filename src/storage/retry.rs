//! Retry policy with exponential backoff for S3 operations.

use crate::storage::S3Error;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, warn};

/// Retry policy implementing exponential backoff with jitter
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    max_retries: u32,
    initial_delay: Duration,
    max_delay: Duration,
}

impl RetryPolicy {
    pub fn new(max_retries: u32, initial_delay: Duration, max_delay: Duration) -> Self {
        Self {
            max_retries,
            initial_delay,
            max_delay,
        }
    }

    /// Execute an operation with retry
    pub async fn execute<F, Fut, T>(&self, mut operation: F) -> Result<T, S3Error>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, S3Error>>,
    {
        let mut attempt = 0;
        let mut last_error = None;

        loop {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    last_error = Some(e);

                    // Check if error is retryable
                    if !self.is_retryable(last_error.as_ref().unwrap()) {
                        debug!(
                            error = ?last_error,
                            "Non-retryable error, failing immediately"
                        );
                        return Err(last_error.unwrap());
                    }

                    attempt += 1;
                    if attempt > self.max_retries {
                        warn!(
                            attempts = attempt,
                            max_retries = self.max_retries,
                            "Max retries exceeded"
                        );
                        return Err(last_error.unwrap());
                    }

                    // Calculate delay with exponential backoff and jitter
                    let delay = self.calculate_delay(attempt);
                    debug!(
                        attempt,
                        max_retries = self.max_retries,
                        delay_ms = delay.as_millis(),
                        "Retrying after delay"
                    );

                    sleep(delay).await;
                }
            }
        }
    }

    /// Check if an error is retryable
    fn is_retryable(&self, error: &S3Error) -> bool {
        matches!(
            error,
            S3Error::Throttled { .. }
                | S3Error::Connection(_)
                | S3Error::Timeout(_)
                | S3Error::Internal(_)
        )
    }

    /// Calculate delay with exponential backoff and jitter
    fn calculate_delay(&self, attempt: u32) -> Duration {
        // Exponential backoff: initial_delay * 2^(attempt-1)
        let base_delay = self
            .initial_delay
            .saturating_mul(1 << (attempt - 1).min(10)); // Cap at 2^10 to prevent overflow

        // Cap at max delay
        let capped_delay = base_delay.min(self.max_delay);

        // Add jitter (±25%)
        let jitter_factor = 0.75 + (rand_jitter() * 0.5);
        let jittered_delay = Duration::from_secs_f64(capped_delay.as_secs_f64() * jitter_factor);

        jittered_delay.max(Duration::from_millis(1))
    }
}

/// Generate a random jitter factor between 0 and 1
fn rand_jitter() -> f64 {
    use std::hash::{Hash, Hasher};
    use std::time::SystemTime;

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .hash(&mut hasher);
    std::thread::current().id().hash(&mut hasher);

    let hash = hasher.finish();
    (hash as f64) / (u64::MAX as f64)
}

/// Builder for retry policy
pub struct RetryPolicyBuilder {
    max_retries: u32,
    initial_delay: Duration,
    max_delay: Duration,
}

impl Default for RetryPolicyBuilder {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
        }
    }
}

impl RetryPolicyBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }

    pub fn initial_delay(mut self, delay: Duration) -> Self {
        self.initial_delay = delay;
        self
    }

    pub fn max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = delay;
        self
    }

    pub fn build(self) -> RetryPolicy {
        RetryPolicy::new(self.max_retries, self.initial_delay, self.max_delay)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_retry_success_on_first_attempt() {
        let policy = RetryPolicy::new(
            3,
            Duration::from_millis(10),
            Duration::from_millis(100),
        );

        let attempt_count = Arc::new(AtomicU32::new(0));
        let attempt_count_clone = attempt_count.clone();

        let result = policy
            .execute(|| {
                let count = attempt_count_clone.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, S3Error>("success")
                }
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(attempt_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_success_after_failures() {
        let policy = RetryPolicy::new(
            3,
            Duration::from_millis(10),
            Duration::from_millis(100),
        );

        let attempt_count = Arc::new(AtomicU32::new(0));
        let attempt_count_clone = attempt_count.clone();

        let result = policy
            .execute(|| {
                let count = attempt_count_clone.clone();
                async move {
                    let attempts = count.fetch_add(1, Ordering::SeqCst) + 1;
                    if attempts < 3 {
                        Err(S3Error::Connection("temporary failure".to_string()))
                    } else {
                        Ok("success")
                    }
                }
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(attempt_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_no_retry_on_non_retryable_error() {
        let policy = RetryPolicy::new(
            3,
            Duration::from_millis(10),
            Duration::from_millis(100),
        );

        let attempt_count = Arc::new(AtomicU32::new(0));
        let attempt_count_clone = attempt_count.clone();

        let result: Result<&str, S3Error> = policy
            .execute(|| {
                let count = attempt_count_clone.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Err(S3Error::NotFound {
                        bucket: "test".to_string(),
                        key: "key".to_string(),
                    })
                }
            })
            .await;

        assert!(result.is_err());
        assert_eq!(attempt_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_calculate_delay_exponential() {
        let policy = RetryPolicy::new(
            5,
            Duration::from_millis(100),
            Duration::from_secs(10),
        );

        // First retry should be around 100ms
        let delay1 = policy.calculate_delay(1);
        assert!(delay1.as_millis() >= 75 && delay1.as_millis() <= 125);

        // Second retry should be around 200ms
        let delay2 = policy.calculate_delay(2);
        assert!(delay2.as_millis() >= 150 && delay2.as_millis() <= 250);
    }
}
