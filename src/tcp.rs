use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tracing::{error, info, warn};

use crate::config::ServiceConfig;
use crate::middleware::load_balancer::LoadBalancer;
use crate::metrics::ServiceMetrics;

pub struct TcpProxy {
    config: ServiceConfig,
    lb: Arc<LoadBalancer>,
    metrics: Arc<ServiceMetrics>,
}

impl TcpProxy {
    pub fn new(
        config: ServiceConfig,
        lb: Arc<LoadBalancer>,
        metrics: Arc<ServiceMetrics>,
    ) -> Self {
        Self {
            config,
            lb,
            metrics,
        }
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(&self.config.listen_addr).await?;

        // Setup Middleware Chain (Modular Filters)
        use crate::middleware::{MiddlewareChain, rate_limiter::RateLimiter};
        let mut middleware_chain = MiddlewareChain::new();
        if let Some(ref limit_conf) = self.config.rate_limit {
            middleware_chain.add(Arc::new(RateLimiter::new(limit_conf.max_requests, limit_conf.refill_rate)));
            info!(
                "[{}] Connection Rate Limiter enabled: max={}, refill={}/s",
                self.config.name, limit_conf.max_requests, limit_conf.refill_rate
            );
        }
        
        // Add Load Balancer as a terminal routing middleware!
        middleware_chain.add(self.lb.clone());
        let middleware_chain = Arc::new(middleware_chain);

        // Setup TLS Acceptor if configured (Dynamic Let's Encrypt ACME or Static Certificate)
        let tls_acceptor = if let Some(ref domain) = self.config.acme_domain {
            let email = self.config.acme_email.clone().unwrap_or_else(|| "admin@example.com".to_string());
            let tls_config = crate::tls::load_acme_config(domain, &email)?;
            info!(
                "[{}] SSL/TLS termination enabled via Let's Encrypt (ACME) for domain: '{}'",
                self.config.name, domain
            );
            Some(TlsAcceptor::from(tls_config))
        } else if let (Some(cert), Some(key)) = (&self.config.cert_path, &self.config.key_path) {
            let tls_config = crate::tls::load_tls_config(cert, key)?;
            info!(
                "[{}] SSL/TLS termination enabled using static cert: '{}', key: '{}'",
                self.config.name, cert, key
            );
            Some(TlsAcceptor::from(tls_config))
        } else {
            None
        };

        info!(
            "TCP Proxy service '{}' listening on {}",
            self.config.name, self.config.listen_addr
        );

        loop {
            let (client_stream, client_addr) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    error!(
                        "[{}] Failed to accept TCP connection: {}",
                        self.config.name, e
                    );
                    self.metrics.inc_errors();
                    continue;
                }
            };

            // Execute modular Middleware Filter Chain (e.g. rate limiters and load balancer!)
            let ctx = match middleware_chain.execute(client_addr.ip()) {
                Ok(context) => context,
                Err(mw_name) => {
                    warn!(
                        "[{}] TCP connection rejected by middleware '{}' from IP: {}",
                        self.config.name, mw_name, client_addr.ip()
                    );
                    self.metrics.inc_errors();
                    // Reject connection instantly by dropping client_stream
                    continue;
                }
            };

            let backend_addr = ctx.target_backend.expect("Backend selection guaranteed by LoadBalancer middleware");
            let lb = self.lb.clone();
            let metrics = self.metrics.clone();
            let service_name = self.config.name.clone();
            let acceptor = tls_acceptor.clone();

            tokio::spawn(async move {
                metrics.inc_active_connections();

                let backend_stream = match tokio::time::timeout(std::time::Duration::from_secs(10), TcpStream::connect(&backend_addr)).await {
                    Ok(Ok(stream)) => {
                        lb.record_success(&backend_addr);
                        stream
                    }
                    Ok(Err(e)) => {
                        error!(
                            "[{}] Failed to connect to backend {}: {}",
                            service_name, backend_addr, e
                        );
                        lb.record_failure(&backend_addr);
                        metrics.inc_errors();
                        metrics.dec_active_connections();
                        return;
                    }
                    Err(_) => {
                        error!(
                            "[{}] Connection to backend {} timed out",
                            service_name, backend_addr
                        );
                        lb.record_failure(&backend_addr);
                        metrics.inc_errors();
                        metrics.dec_active_connections();
                        return;
                    }
                };

                if let Some(acc) = acceptor {
                    // TLS Connection Yönlendirme (TLS Termination) with Slowloris Protection
                    match tokio::time::timeout(std::time::Duration::from_secs(10), acc.accept(client_stream)).await {
                        Ok(Ok(tls_stream)) => {
                            info!(
                                "[{}] Decrypted TLS connection from {} routing to backend {}",
                                service_name, client_addr, backend_addr
                            );
                            forward(tls_stream, backend_stream, metrics.clone(), service_name, client_addr, backend_addr).await;
                        }
                        Ok(Err(e)) => {
                            error!(
                                "[{}] TLS handshake failed for client {}: {}",
                                service_name, client_addr, e
                            );
                            metrics.inc_errors();
                        }
                        Err(_) => {
                            warn!(
                                "[{}] TLS handshake timed out (Slowloris) for client {}",
                                service_name, client_addr
                            );
                            metrics.inc_errors();
                        }
                    }
                } else {
                    // Plain TCP Forwarding
                    info!(
                        "[{}] Routing connection from {} to backend {}",
                        service_name, client_addr, backend_addr
                    );
                    forward(client_stream, backend_stream, metrics.clone(), service_name, client_addr, backend_addr).await;
                }

                metrics.dec_active_connections();
            });
        }
    }
}

async fn forward<C, S>(
    mut client: C,
    mut backend: S,
    metrics: Arc<ServiceMetrics>,
    service_name: String,
    client_addr: std::net::SocketAddr,
    backend_addr: String,
) where
    C: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    match tokio::io::copy_bidirectional(&mut client, &mut backend).await {
        Ok((rx_bytes, tx_bytes)) => {
            metrics.add_rx(rx_bytes);
            metrics.add_tx(tx_bytes);
            info!(
                "[{}] Connection closed between {} and {}. Transferred: {} rx, {} tx",
                service_name, client_addr, backend_addr, rx_bytes, tx_bytes
            );
        }
        Err(e) => {
            warn!(
                "[{}] Error in bi-directional transfer between {} and {}: {}",
                service_name, client_addr, backend_addr, e
            );
            metrics.inc_errors();
        }
    }
}
