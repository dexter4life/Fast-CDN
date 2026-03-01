//! Zero-copy streaming support with backpressure handling.

use bytes::Bytes;
use futures::Stream;
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::warn;

/// Configuration for streaming
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    /// Maximum buffer size for slow consumers
    pub max_buffer_size: usize,
    /// Chunk size for reading
    pub chunk_size: usize,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            max_buffer_size: 16 * 1024 * 1024, // 16MB
            chunk_size: 64 * 1024,             // 64KB
        }
    }
}

pin_project! {
    /// A streaming body with backpressure support
    pub struct StreamingBody<S> {
        #[pin]
        inner: S,
        buffer_size: usize,
        max_buffer_size: usize,
        bytes_sent: u64,
    }
}

impl<S> StreamingBody<S> {
    pub fn new(stream: S, config: &StreamingConfig) -> Self {
        Self {
            inner: stream,
            buffer_size: 0,
            max_buffer_size: config.max_buffer_size,
            bytes_sent: 0,
        }
    }

    pub fn bytes_sent(&self) -> u64 {
        self.bytes_sent
    }
}

impl<S, E> Stream for StreamingBody<S>
where
    S: Stream<Item = Result<Bytes, E>>,
{
    type Item = Result<Bytes, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match this.inner.poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                *this.bytes_sent += chunk.len() as u64;
                
                // Track buffer usage for metrics
                *this.buffer_size = (*this.buffer_size).saturating_add(chunk.len());
                
                // Warn if buffer is getting large (slow consumer)
                if *this.buffer_size > *this.max_buffer_size / 2 {
                    warn!(
                        buffer_size = *this.buffer_size,
                        max_buffer = *this.max_buffer_size,
                        "Slow consumer detected"
                    );
                }

                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => {
                // Consumer caught up, reset buffer tracking
                *this.buffer_size = 0;
                Poll::Pending
            }
        }
    }
}

/// Adapter to convert AsyncRead to Stream
pin_project! {
    pub struct AsyncReadStream<R> {
        #[pin]
        reader: R,
        chunk_size: usize,
        buf: Vec<u8>,
    }
}

impl<R: AsyncRead> AsyncReadStream<R> {
    pub fn new(reader: R, chunk_size: usize) -> Self {
        Self {
            reader,
            chunk_size,
            buf: vec![0u8; chunk_size],
        }
    }
}

impl<R: AsyncRead> Stream for AsyncReadStream<R> {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        let mut read_buf = tokio::io::ReadBuf::new(this.buf);
        
        match this.reader.poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(())) => {
                let filled = read_buf.filled();
                if filled.is_empty() {
                    // EOF
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Ok(Bytes::copy_from_slice(filled))))
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Rate-limited stream for slow consumer protection
pin_project! {
    pub struct RateLimitedStream<S> {
        #[pin]
        inner: S,
        bytes_per_second: u64,
        bytes_this_second: u64,
        second_start: std::time::Instant,
    }
}

impl<S> RateLimitedStream<S> {
    pub fn new(stream: S, bytes_per_second: u64) -> Self {
        Self {
            inner: stream,
            bytes_per_second,
            bytes_this_second: 0,
            second_start: std::time::Instant::now(),
        }
    }
}

impl<S, E> Stream for RateLimitedStream<S>
where
    S: Stream<Item = Result<Bytes, E>>,
{
    type Item = Result<Bytes, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        // Check if we've exceeded the rate for this second
        let now = std::time::Instant::now();
        let elapsed = now.duration_since(*this.second_start);

        if elapsed >= std::time::Duration::from_secs(1) {
            // Reset for new second
            *this.second_start = now;
            *this.bytes_this_second = 0;
        }

        if *this.bytes_this_second >= *this.bytes_per_second {
            // Schedule wakeup at the start of next second
            let waker = cx.waker().clone();
            let remaining = std::time::Duration::from_secs(1) - elapsed;
            
            tokio::spawn(async move {
                tokio::time::sleep(remaining).await;
                waker.wake();
            });

            return Poll::Pending;
        }

        match this.inner.poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                *this.bytes_this_second += chunk.len() as u64;
                Poll::Ready(Some(Ok(chunk)))
            }
            other => other,
        }
    }
}

/// Tee stream that writes to both a cache buffer and the output
pin_project! {
    pub struct TeeStream<S> {
        #[pin]
        inner: S,
        buffer: Vec<u8>,
        max_buffer: usize,
        buffering: bool,
    }
}

impl<S> TeeStream<S> {
    pub fn new(stream: S, max_buffer: usize) -> Self {
        Self {
            inner: stream,
            buffer: Vec::with_capacity(max_buffer.min(1024 * 1024)), // Pre-allocate up to 1MB
            max_buffer,
            buffering: true,
        }
    }

    /// Take the buffered data (consumes the buffer)
    pub fn take_buffer(&mut self) -> Option<Bytes> {
        if self.buffer.is_empty() || !self.buffering {
            None
        } else {
            Some(Bytes::from(std::mem::take(&mut self.buffer)))
        }
    }

    /// Check if we're still buffering
    pub fn is_buffering(&self) -> bool {
        self.buffering
    }
}

impl<S, E> Stream for TeeStream<S>
where
    S: Stream<Item = Result<Bytes, E>>,
{
    type Item = Result<Bytes, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match this.inner.poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                // Buffer if we haven't exceeded the limit
                if *this.buffering {
                    if this.buffer.len() + chunk.len() <= *this.max_buffer {
                        this.buffer.extend_from_slice(&chunk);
                    } else {
                        // Stop buffering, item too large for cache
                        *this.buffering = false;
                        this.buffer.clear();
                        this.buffer.shrink_to_fit();
                    }
                }
                Poll::Ready(Some(Ok(chunk)))
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_streaming_body() {
        let chunks: Vec<Result<Bytes, std::io::Error>> = vec![
            Ok(Bytes::from("hello ")),
            Ok(Bytes::from("world")),
        ];
        let stream = futures::stream::iter(chunks);
        
        let config = StreamingConfig::default();
        let mut body = StreamingBody::<_>::new(stream, &config);

        let mut result = Vec::new();
        while let Some(chunk) = body.next().await {
            result.extend_from_slice(&chunk.unwrap());
        }

        assert_eq!(result, b"hello world");
        assert_eq!(body.bytes_sent(), 11);
    }

    #[tokio::test]
    async fn test_tee_stream() {
        let chunks: Vec<Result<Bytes, std::io::Error>> = vec![
            Ok::<_, std::io::Error>(Bytes::from(vec![1, 2, 3])),
            Ok(Bytes::from(vec![4, 5, 6])),
        ];
        let stream = futures::stream::iter(chunks);
        
        let mut tee = TeeStream::new(stream, 1024);

        // Consume the stream
        while let Some(_) = tee.next().await {}

        // Verify buffer contains all data
        let buffer = tee.take_buffer().unwrap();
        assert_eq!(&buffer[..], &[1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn test_tee_stream_exceeds_buffer() {
        let large_chunk = Ok::<_, std::io::Error>(Bytes::from(vec![0u8; 100]));
        let stream = futures::stream::iter(vec![large_chunk]);
        
        let mut tee = TeeStream::new(stream, 50); // Buffer smaller than chunk

        // Consume the stream
        while let Some(_) = tee.next().await {}

        // Buffer should be empty because item exceeded limit
        assert!(!tee.is_buffering());
        assert!(tee.take_buffer().is_none());
    }
}
