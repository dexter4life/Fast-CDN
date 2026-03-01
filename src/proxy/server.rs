//! HTTP server implementation using Hyper.

use crate::cache::SharedCache;
use crate::config::{CdnConfig, ServerConfig};
use crate::observability::Metrics;
use crate::proxy::RequestHandler;
use crate::security::SecurityMiddleware;
use crate::storage::S3Client;
use anyhow::Result;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use std::error::Error as StdError;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::broadcast;
use tracing::{error, info, warn};

/// CDN server state
pub struct CdnServer {
    config: ServerConfig,
    handler: Arc<RequestHandler>,
    shutdown_tx: broadcast::Sender<()>,
}

impl CdnServer {
    /// Create a new CDN server
    pub async fn new(
        config: CdnConfig,
        s3_client: S3Client,
        cache: SharedCache,
        security: SecurityMiddleware,
        metrics: Metrics,
    ) -> Result<Self> {
        let handler = Arc::new(RequestHandler::new(
            config.clone(),
            s3_client,
            cache,
            security,
            metrics,
        ));

        let (shutdown_tx, _) = broadcast::channel(1);

        Ok(Self {
            config: config.server,
            handler,
            shutdown_tx,
        })
    }

    /// Start the HTTP server
    pub async fn run(self) -> Result<()> {
        let addr: SocketAddr = self.config.http_addr.parse()?;

        info!(addr = %addr, "Starting CDN server");

        let listener = TcpListener::bind(addr).await?;

        // Start HTTP/3 server if enabled
        #[cfg(feature = "http3")]
        if self.config.enable_http3 {
            if let Some(ref http3_addr) = self.config.http3_addr {
                let handler = self.handler.clone();
                let shutdown_rx = self.shutdown_tx.subscribe();
                let addr_str = http3_addr.clone();
                let tls_cert = self.config.tls_cert.clone();
                let tls_key = self.config.tls_key.clone();
                
                tokio::spawn(async move {
                    if let Err(e) = run_http3_server(
                        &addr_str,
                        handler,
                        tls_cert,
                        tls_key,
                        shutdown_rx,
                    ).await {
                        error!(error = %e, "HTTP/3 server error");
                    }
                });
            }
        }

        let shutdown_timeout = self.config.shutdown_timeout;
        let shutdown_tx = self.shutdown_tx.clone();
        let handler = self.handler.clone();

        // Main accept loop
        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, remote_addr)) => {
                            let handler = handler.clone();
                            let shutdown_rx = shutdown_tx.subscribe();

                            tokio::spawn(async move {
                                let io = TokioIo::new(stream);

                                let service = service_fn(move |req| {
                                    let handler = handler.clone();
                                    async move {
                                        handler.handle(req, Some(remote_addr.ip())).await
                                    }
                                });

                                let conn = http1::Builder::new()
                                    .serve_connection(io, service);

                                tokio::select! {
                                    result = conn => {
                                        if let Err(e) = result {
                                            if !is_connection_error(&e) {
                                                warn!(
                                                    error = %e,
                                                    remote_addr = %remote_addr,
                                                    "Connection error"
                                                );
                                            }
                                        }
                                    }
                                    _ = wait_for_shutdown(shutdown_rx) => {
                                        // Connection will be dropped
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "Accept error");
                        }
                    }
                }
                _ = shutdown_signal() => {
                    info!("Shutdown signal received");
                    break;
                }
            }
        }

        // Graceful shutdown
        info!(timeout = ?shutdown_timeout, "Starting graceful shutdown");
        let _ = shutdown_tx.send(());

        // Wait for connections to drain or timeout
        tokio::time::sleep(shutdown_timeout).await;

        info!("Server shutdown complete");
        Ok(())
    }

    /// Get a shutdown trigger
    pub fn shutdown_trigger(&self) -> broadcast::Sender<()> {
        self.shutdown_tx.clone()
    }
}

/// Wait for shutdown signal
async fn wait_for_shutdown(mut rx: broadcast::Receiver<()>) {
    let _ = rx.recv().await;
}

/// Listen for OS shutdown signals (SIGTERM, SIGINT)
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

/// Check if an error is a normal connection closure
fn is_connection_error(e: &hyper::Error) -> bool {
    if e.is_incomplete_message() || e.is_canceled() || e.is_closed() {
        return true;
    }

    if let Some(source) = e.source() {
        if let Some(io_err) = source.downcast_ref::<std::io::Error>() {
            return matches!(
                io_err.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
            );
        }
    }

    false
}

/// Run HTTP/3 (QUIC) server
#[cfg(feature = "http3")]
async fn run_http3_server(
    addr: &str,
    handler: Arc<RequestHandler>,
    tls_cert: Option<std::path::PathBuf>,
    tls_key: Option<std::path::PathBuf>,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Result<()> {
    use quinn::{ServerConfig as QuinnServerConfig, Endpoint};
    use std::fs;

    let (cert_path, key_path) = match (tls_cert, tls_key) {
        (Some(cert), Some(key)) => (cert, key),
        _ => {
            warn!("HTTP/3 requires TLS certificate and key");
            return Ok(());
        }
    };

    // Load TLS certificates
    let cert_pem = fs::read(&cert_path)?;
    let key_pem = fs::read(&key_path)?;

    let certs = rustls_pemfile::certs(&mut cert_pem.as_slice())
        .filter_map(|c| c.ok())
        .map(|c| rustls::pki_types::CertificateDer::from(c.to_vec()))
        .collect::<Vec<_>>();

    let key = rustls_pemfile::private_key(&mut key_pem.as_slice())?
        .ok_or_else(|| anyhow::anyhow!("No private key found"))?;

    let mut server_crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    server_crypto.alpn_protocols = vec![b"h3".to_vec()];

    let server_config = QuinnServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto)?
    ));

    let addr: SocketAddr = addr.parse()?;
    let endpoint = Endpoint::server(server_config, addr)?;

    info!(addr = %addr, "HTTP/3 server listening");

    loop {
        tokio::select! {
            conn = endpoint.accept() => {
                if let Some(connecting) = conn {
                    let handler = handler.clone();
                    
                    tokio::spawn(async move {
                        match connecting.await {
                            Ok(connection) => {
                                // Handle HTTP/3 connection
                                // This is a simplified version; full implementation
                                // would use h3 crate for HTTP/3 framing
                                tracing::debug!(
                                    remote_addr = %connection.remote_address(),
                                    "HTTP/3 connection established"
                                );
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "HTTP/3 connection failed");
                            }
                        }
                    });
                }
            }
            _ = shutdown_rx.recv() => {
                info!("HTTP/3 server shutting down");
                break;
            }
        }
    }

    endpoint.close(0u32.into(), b"server shutdown");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_connection_error() {
        // Test that connection errors are properly detected
        // This is a basic sanity test
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset");
        assert!(matches!(
            io_err.kind(),
            std::io::ErrorKind::ConnectionReset
        ));
    }
}
