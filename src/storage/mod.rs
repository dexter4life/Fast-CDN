//! S3 storage module with multi-cloud compatibility.
//!
//! Supports AWS S3, MinIO, Cloudflare R2, and any S3-compatible storage.

mod client;
mod sigv4;
mod retry;

pub use client::*;
pub use sigv4::*;
pub use retry::*;
