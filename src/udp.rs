use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tokio::time::{timeout, Duration};
use tracing::{error, info, warn};

use crate::config::ServiceConfig;
use crate::middleware::load_balancer::LoadBalancer;
use crate::metrics::ServiceMetrics;

const SESSION_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_DATAGRAM_SIZE: usize = 65507;

#[derive(Clone)]
struct UdpSession {
    backend_socket: Arc<UdpSocket>,
    backend_addr: SocketAddr,
}

pub struct UdpProxy {
    config: ServiceConfig,
    lb: Arc<LoadBalancer>,
    metrics: Arc<ServiceMetrics>,
    sessions: Arc<RwLock<HashMap<SocketAddr, UdpSession>>>,
}

impl UdpProxy {
    pub fn new(
        config: ServiceConfig,
        lb: Arc<LoadBalancer>,
        metrics: Arc<ServiceMetrics>,
    ) -> Self {
        Self {
            config,
            lb,
            metrics,
            sessions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = Arc::new(UdpSocket::bind(&self.config.listen_addr).await?);
        
        // Setup Middleware Chain (Modular Filters)
        use crate::middleware::{MiddlewareChain, rate_limiter::RateLimiter};
        let mut middleware_chain = MiddlewareChain::new();
        if let Some(ref limit_conf) = self.config.rate_limit {
            middleware_chain.add(Arc::new(RateLimiter::new(limit_conf.max_requests, limit_conf.refill_rate)));
            info!(
                "[{}] UDP Rate Limiter enabled: max={}, refill={}/s",
                self.config.name, limit_conf.max_requests, limit_conf.refill_rate
            );
        }
        
        // Add Load Balancer as terminal routing middleware
        middleware_chain.add(self.lb.clone());
        let middleware_chain = Arc::new(middleware_chain);

        info!(
            "UDP Proxy service '{}' listening on {}",
            self.config.name, self.config.listen_addr
        );

        let mut buf = vec![0u8; MAX_DATAGRAM_SIZE];

        loop {
            let (len, client_addr) = match listener.recv_from(&mut buf).await {
                Ok(res) => res,
                Err(e) => {
                    error!(
                        "[{}] Failed to receive UDP packet: {}",
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
                        "[{}] UDP packet rejected by middleware '{}' from IP: {}",
                        self.config.name, mw_name, client_addr.ip()
                    );
                    self.metrics.inc_errors();
                    continue;
                }
            };

            let backend_addr_str = ctx.target_backend.expect("Backend selection guaranteed by LoadBalancer middleware");
            let data = buf[..len].to_vec();
            self.metrics.add_rx(len as u64);
            self.metrics.inc_requests();

            // Find or create session using pre-selected backend address
            let session = self.get_or_create_session(client_addr, listener.clone(), backend_addr_str).await;

            if let Some(sess) = session {
                let lb = self.lb.clone();
                let metrics = self.metrics.clone();
                let service_name = self.config.name.clone();
                tokio::spawn(async move {
                    if let Err(e) = sess.backend_socket.send_to(&data, sess.backend_addr).await {
                        error!(
                            "[{}] Failed to forward UDP packet to backend {}: {}",
                            service_name, sess.backend_addr, e
                        );
                        lb.record_failure(&sess.backend_addr.to_string());
                        metrics.inc_errors();
                    } else {
                        lb.record_success(&sess.backend_addr.to_string());
                    }
                });
            } else {
                self.metrics.inc_errors();
            }
        }
    }

    async fn get_or_create_session(
        &self,
        client_addr: SocketAddr,
        listener_socket: Arc<UdpSocket>,
        backend_addr_str: String,
    ) -> Option<UdpSession> {
        // Read lock
        {
            let map = self.sessions.read().await;
            if let Some(session) = map.get(&client_addr) {
                return Some(session.clone());
            }
        }

        // Write lock (session doesn't exist)
        let mut map = self.sessions.write().await;
        // Double check
        if let Some(session) = map.get(&client_addr) {
            return Some(session.clone());
        }

        let backend_addr: SocketAddr = match backend_addr_str.parse() {
            Ok(addr) => addr,
            Err(e) => {
                error!(
                    "[{}] Invalid backend address '{}': {}",
                    self.config.name, backend_addr_str, e
                );
                return None;
            }
        };

        // Bind local socket to communicate with backend
        let backend_socket = match UdpSocket::bind("0.0.0.0:0").await {
            Ok(sock) => Arc::new(sock),
            Err(e) => {
                error!(
                    "[{}] Failed to bind socket for UDP backend association: {}",
                    self.config.name, e
                );
                return None;
            }
        };

        let session = UdpSession {
            backend_socket: backend_socket.clone(),
            backend_addr,
        };

        map.insert(client_addr, session.clone());
        self.metrics.inc_active_connections();
        info!(
            "[{}] Created new UDP session for client {} -> backend {}",
            self.config.name, client_addr, backend_addr
        );

        // Spawn listener task for backend responses
        let sessions_clone = self.sessions.clone();
        let service_name = self.config.name.clone();
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            let mut resp_buf = vec![0u8; MAX_DATAGRAM_SIZE];
            loop {
                // Wait for response from backend with a timeout
                match timeout(SESSION_TIMEOUT, backend_socket.recv_from(&mut resp_buf)).await {
                    Ok(Ok((len, from_addr))) => {
                        if from_addr != backend_addr {
                            warn!(
                                "[{}] UDP packet received from unexpected backend address {} (expected {})",
                                service_name, from_addr, backend_addr
                            );
                            continue;
                        }

                        // Send back to client
                        if let Err(e) = listener_socket.send_to(&resp_buf[..len], client_addr).await {
                            error!(
                                "[{}] Failed to send UDP response back to client {}: {}",
                                service_name, client_addr, e
                            );
                            metrics.inc_errors();
                        } else {
                            metrics.add_tx(len as u64);
                        }
                    }
                    Ok(Err(e)) => {
                        error!(
                            "[{}] Error receiving UDP backend response for client {}: {}",
                            service_name, client_addr, e
                        );
                        metrics.inc_errors();
                        break;
                    }
                    Err(_) => {
                        // Timeout - clean up session
                        info!(
                            "[{}] UDP session idle timeout for client {}",
                            service_name, client_addr
                        );
                        break;
                    }
                }
            }

            // Remove from session map
            let mut map = sessions_clone.write().await;
            map.remove(&client_addr);
            metrics.dec_active_connections();
            info!(
                "[{}] UDP session cleaned up for client {}",
                service_name, client_addr
            );
        });

        Some(session)
    }
}
