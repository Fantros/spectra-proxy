use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use http_body_util::combinators::BoxBody;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper::http;
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::convert::Infallible;
use hyper_util::server::conn::auto;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tracing::{error, info, warn};

use crate::config::ServiceConfig;
use crate::middleware::load_balancer::LoadBalancer;
use crate::metrics::ServiceMetrics;
use crate::middleware::{MiddlewareChain, rate_limiter::RateLimiter};

type GenericBody = BoxBody<Bytes, Box<dyn std::error::Error + Send + Sync>>;

struct HttpRoute {
    path: String,
    lb: Arc<LoadBalancer>,
}

pub struct HttpProxy {
    config: ServiceConfig,
    default_lb: Arc<LoadBalancer>,
    routes: Vec<HttpRoute>,
    metrics: Arc<ServiceMetrics>,
    middleware_chain: Arc<MiddlewareChain>,
}

impl HttpProxy {
    pub fn new(
        config: ServiceConfig,
        default_lb: Arc<LoadBalancer>,
        metrics: Arc<ServiceMetrics>,
    ) -> Self {
        let mut routes = Vec::new();
        if let Some(ref http_rules) = config.http_rules
            && let Some(ref configured_routes) = http_rules.routes {
                for r in configured_routes {
                    let route_lb = Arc::new(LoadBalancer::new(
                        r.backends.clone(),
                        config.load_balance,
                    ));
                    routes.push(HttpRoute {
                        path: r.path.clone(),
                        lb: route_lb,
                    });
                }
            }

        // Sort routes by path length descending so more specific paths match first
        routes.sort_by(|a, b| b.path.len().cmp(&a.path.len()));

        // Setup Middleware Chain (Modular Filters)
        let mut middleware_chain = MiddlewareChain::new();
        if let Some(ref limit_conf) = config.rate_limit {
            middleware_chain.add(Arc::new(RateLimiter::new(limit_conf.max_requests, limit_conf.refill_rate)));
            info!(
                "[{}] HTTP L7 Rate Limiter enabled: max={}, refill={}/s",
                config.name, limit_conf.max_requests, limit_conf.refill_rate
            );
        }

        Self {
            config,
            default_lb,
            routes,
            metrics,
            middleware_chain: Arc::new(middleware_chain),
        }
    }

    /// Entry point for launching the L7 proxy engine.
    /// It concurrently runs both the TCP engine (HTTP/1.x & HTTP/2) and the secure UDP engine (HTTP/3 over QUIC).
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let proxy_state = Arc::new(self);

        // 1. Spawn TCP dynamic HTTP/1.x and HTTP/2 task
        let state_tcp = proxy_state.clone();
        let name_tcp = proxy_state.config.name.clone();
        let tcp_handle = tokio::spawn(async move {
            if let Err(e) = state_tcp.run_tcp().await {
                error!("[{}] TCP engine terminated with error: {}", name_tcp, e);
            }
        });

        // 2. Spawn UDP dynamic HTTP/3 (QUIC) task if SSL/TLS is enabled (Dynamic ACME or Static Certificate)
        if proxy_state.config.acme_domain.is_some() || (proxy_state.config.cert_path.is_some() && proxy_state.config.key_path.is_some()) {
            let state_h3 = proxy_state.clone();
            let name_h3 = proxy_state.config.name.clone();
            tokio::spawn(async move {
                if let Err(e) = state_h3.run_h3().await {
                    error!("[{}] HTTP/3 QUIC engine terminated with error: {}", name_h3, e);
                }
            });
        }

        // Keep running until the TCP handler terminates
        let _ = tcp_handle.await;
        Ok(())
    }

    /// Traditional TCP socket listener loop for HTTP/1.1 and HTTP/2
    async fn run_tcp(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(&self.config.listen_addr).await?;

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
            "HTTP/1.x & HTTP/2 (TCP) L7 Proxy service '{}' listening on {}",
            self.config.name, self.config.listen_addr
        );

        let proxy_state = Arc::new(Self {
            config: self.config.clone(),
            default_lb: self.default_lb.clone(),
            routes: self.routes.iter().map(|r| HttpRoute { path: r.path.clone(), lb: r.lb.clone() }).collect(),
            metrics: self.metrics.clone(),
            middleware_chain: self.middleware_chain.clone(),
        });

        loop {
            let (stream, client_addr) = match listener.accept().await {
                Ok(res) => res,
                Err(e) => {
                    error!(
                        "[{}] Failed to accept TCP HTTP connection: {}",
                        proxy_state.config.name, e
                    );
                    proxy_state.metrics.inc_errors();
                    continue;
                }
            };

            // Execute Connection Rate Limit Middleware Filter immediately after accept!
            if let Err(mw_name) = proxy_state.middleware_chain.execute(client_addr.ip()) {
                warn!(
                    "[{}] HTTP TCP connection rejected by middleware '{}' from IP: {}",
                    proxy_state.config.name, mw_name, client_addr.ip()
                );
                proxy_state.metrics.inc_errors();
                // Reject connection instantly by dropping socket
                continue;
            }

            let state = proxy_state.clone();
            let acceptor = tls_acceptor.clone();

            tokio::spawn(async move {
                state.metrics.inc_active_connections();

                // Create hyper auto builder to handle HTTP/1.x and HTTP/2
                let builder = auto::Builder::new(TokioExecutor::new());

                let state_clone = state.clone();
                let service = service_fn(move |req: Request<Incoming>| {
                    let state = state_clone.clone();
                    async move { state.handle_request(req, client_addr).await }
                });

                if let Some(acc) = acceptor {
                    // TLS Handshake (HTTPS termination) with Slowloris Protection
                    match tokio::time::timeout(std::time::Duration::from_secs(10), acc.accept(stream)).await {
                        Ok(Ok(tls_stream)) => {
                            let io = TokioIo::new(tls_stream);
                            if let Err(err) = tokio::time::timeout(std::time::Duration::from_secs(300), builder.serve_connection(io, service)).await {
                                warn!(
                                    "[{}] HTTPS connection timeout/error for client {}: {:?}",
                                    state.config.name, client_addr, err
                                );
                                state.metrics.inc_errors();
                            }
                        }
                        Ok(Err(e)) => {
                            error!(
                                "[{}] HTTPS TLS handshake failed for client {}: {}",
                                state.config.name, client_addr, e
                            );
                            state.metrics.inc_errors();
                        }
                        Err(_) => {
                            warn!(
                                "[{}] HTTPS TLS handshake timed out (Slowloris) for client {}",
                                state.config.name, client_addr
                            );
                            state.metrics.inc_errors();
                        }
                    }
                } else {
                    // Plain HTTP routing
                    let io = TokioIo::new(stream);
                    if let Err(err) = tokio::time::timeout(std::time::Duration::from_secs(300), builder.serve_connection(io, service)).await {
                        warn!(
                            "[{}] HTTP connection timeout/error for client {}: {:?}",
                            state.config.name, client_addr, err
                        );
                        state.metrics.inc_errors();
                    }
                }

                state.metrics.dec_active_connections();
            });
        }
    }

    /// Modern UDP socket listener loop for secure HTTP/3 (QUIC)
    async fn run_h3(
        self: Arc<Self>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Load SSL/TLS certificates (Dynamic Let's Encrypt ACME or Static Certificate)
        let tls_config = if let Some(ref domain) = self.config.acme_domain {
            let email = self.config.acme_email.clone().unwrap_or_else(|| "admin@example.com".to_string());
            crate::tls::load_acme_config(domain, &email)?
        } else if let (Some(cert), Some(key)) = (&self.config.cert_path, &self.config.key_path) {
            crate::tls::load_tls_config(cert, key)?
        } else {
            return Err("HTTP/3 engine requires certs/keys or acme_domain parameters".into());
        };
        
        // Setup Quinn Server configuration using compat conversion
        let rustls_config = (*tls_config).clone();
        let quic_crypto = quinn::crypto::rustls::QuicServerConfig::try_from(rustls_config)?;
        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_crypto));
        let mut transport = quinn::TransportConfig::default();
        transport.max_concurrent_bidi_streams(1000u32.into());
        server_config.transport_config(Arc::new(transport));

        // Quinn 0.11 server endpoint constructor handles socket binding and runtime setup instantly
        let endpoint = quinn::Endpoint::server(
            server_config,
            self.config.listen_addr.parse()?,
        )?;

        info!(
            "HTTP/3 (UDP QUIC) L7 Proxy service '{}' listening on {}",
            self.config.name, self.config.listen_addr
        );

        while let Some(incoming) = endpoint.accept().await {
            let state = self.clone();
            
            // Execute Connection Rate Limit Middleware Filter immediately on the raw UDP incoming IP!
            if let Err(mw_name) = state.middleware_chain.execute(incoming.remote_address().ip()) {
                warn!(
                    "[{}] HTTP/3 QUIC connection rejected by middleware '{}' from IP: {}",
                    state.config.name, mw_name, incoming.remote_address().ip()
                );
                state.metrics.inc_errors();
                // Reject connection immediately before starting heavy crypto handshakes
                continue;
            }

            match incoming.accept() {
                Ok(conn) => {
                    let state_clone = state.clone();
                    tokio::spawn(async move {
                        if let Err(e) = state_clone.handle_quic_connection(conn).await {
                            warn!(
                                "[{}] Error handling HTTP/3 QUIC connection: {}",
                                state.config.name, e
                            );
                            state.metrics.inc_errors();
                        }
                    });
                }
                Err(e) => {
                    warn!(
                        "[{}] Failed to accept incoming QUIC connection: {}",
                        state.config.name, e
                    );
                    state.metrics.inc_errors();
                }
            }
        }

        Ok(())
    }

    /// Handles an accepted QUIC connection and starts H3 session handshakes
    async fn handle_quic_connection(
        self: Arc<Self>,
        conn: quinn::Connecting,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.metrics.inc_active_connections();

        let connection = conn.await?;
        let mut h3_conn = h3::server::Connection::new(h3_quinn::Connection::new(connection)).await?;

        while let Some(resolver) = h3_conn.accept().await? {
            let state = self.clone();
            tokio::spawn(async move {
                let (req, stream) = match resolver.resolve_request().await {
                    Ok(res) => res,
                    Err(e) => {
                        warn!(
                            "[{}] Failed to resolve H3 request: {}",
                            state.config.name, e
                        );
                        state.metrics.inc_errors();
                        return;
                    }
                };
                if let Err(e) = state.handle_h3_request(req, stream).await {
                    warn!(
                        "[{}] Error serving HTTP/3 stream request: {}",
                        state.config.name, e
                    );
                    state.metrics.inc_errors();
                }
            });
        }

        self.metrics.dec_active_connections();
        Ok(())
    }

    /// Serves an individual HTTP/3 stream, routing requests to backend TCP hosts
    async fn handle_h3_request<S>(
        &self,
        req: http::Request<()>,
        mut stream: h3::server::RequestStream<S, bytes::Bytes>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> 
    where 
        S: h3::quic::SendStream<bytes::Bytes> + h3::quic::RecvStream,
    {
        self.metrics.inc_requests();
        let path = req.uri().path().to_string();

        // 1. Select load balancer based on path routing
        let mut selected_lb = self.default_lb.clone();
        for route in &self.routes {
            if path.starts_with(&route.path) {
                selected_lb = route.lb.clone();
                break;
            }
        }

        // 2. Select backend
        let backend_addr = match selected_lb.select() {
            Some(addr) => addr,
            None => {
                error!(
                    "[{}] No backends available for HTTP/3 request to '{}'",
                    self.config.name, path
                );
                self.metrics.inc_errors();
                
                let err_resp = self.error_response(
                    StatusCode::BAD_GATEWAY,
                    "Gateway Error: No backends configured or available.",
                );
                let (parts, mut body) = err_resp.into_parts();
                let h3_resp = Response::from_parts(parts, ());
                stream.send_response(h3_resp).await?;
                
                while let Some(chunk) = body.frame().await {
                    let frame = chunk?;
                    if let Some(data) = frame.data_ref() {
                        stream.send_data(data.clone()).await?;
                    }
                }
                stream.finish().await?;
                return Ok(());
            }
        };

        // 3. Forward request to backend using hyper-util HTTP client
        let (mut parts, _) = req.into_parts();

        // Rewrite URI to backend
        let backend_uri_string = format!("http://{}{}", backend_addr, path);
        let target_uri = backend_uri_string.parse::<hyper::Uri>()?;
        parts.uri = target_uri;

        // Apply HTTP Header Rules
        if let Some(ref http_rules) = self.config.http_rules
            && let Some(ref headers_conf) = http_rules.headers {
                // Inject custom headers
                if let Some(ref inject) = headers_conf.inject {
                    for (k, v) in inject {
                        if let Ok(name) = hyper::header::HeaderName::from_bytes(k.as_bytes())
                            && let Ok(value) = hyper::header::HeaderValue::from_str(v) {
                                parts.headers.insert(name, value);
                            }
                    }
                }

                // Remove configured headers
                if let Some(ref remove) = headers_conf.remove {
                    for k in remove {
                        if let Ok(name) = hyper::header::HeaderName::from_bytes(k.as_bytes()) {
                            parts.headers.remove(name);
                        }
                    }
                }
            }

        // Inject standard proxy headers
        let proto_val = hyper::header::HeaderValue::from_static("https"); // HTTP/3 is always secure
        parts.headers.insert(
            hyper::header::HeaderName::from_static("x-forwarded-proto"),
            proto_val,
        );
        let proxy_val = hyper::header::HeaderValue::from_static("SpectraProxy-H3");
        parts.headers.insert(
            hyper::header::HeaderName::from_static("x-proxy-by"),
            proxy_val,
        );

        let forward_req = Request::from_parts(parts, http_body_util::Empty::<bytes::Bytes>::new());

        let client = hyper_util::client::legacy::Client::builder(TokioExecutor::new())
            .build(hyper_util::client::legacy::connect::HttpConnector::new());

        info!(
            "[{}] Routing HTTP/3 request '{}' to backend '{}'",
            self.config.name, path, backend_addr
        );

        match client.request(forward_req).await {
            Ok(resp) => {
                selected_lb.record_success(&backend_addr);
                let (resp_parts, mut resp_body) = resp.into_parts();
                
                // Add Alt-Svc to advertise H3 availability
                let listen_port = self.config.listen_addr.split(':').next_back().unwrap_or("443");
                let alt_svc_val = format!("h3=\":{}\"; ma=86400", listen_port);
                let alt_val = hyper::header::HeaderValue::from_str(&alt_svc_val)?;
                let mut resp_parts = resp_parts;
                resp_parts.headers.insert(
                    hyper::header::HeaderName::from_static("alt-svc"),
                    alt_val,
                );

                let h3_resp = Response::from_parts(resp_parts, ());
                stream.send_response(h3_resp).await?;

                while let Some(chunk) = resp_body.frame().await {
                    let frame = chunk?;
                    if let Some(data) = frame.data_ref() {
                        stream.send_data(data.clone()).await?;
                    }
                }
                stream.finish().await?;
            }
            Err(e) => {
                error!(
                    "[{}] Failed to forward HTTP/3 request to backend '{}': {}",
                    self.config.name, backend_addr, e
                );
                selected_lb.record_failure(&backend_addr);
                self.metrics.inc_errors();

                let err_resp = self.error_response(
                    StatusCode::BAD_GATEWAY,
                    &format!("Bad Gateway: Request to backend failed: {}", e),
                );
                let (parts, mut body) = err_resp.into_parts();
                let h3_resp = Response::from_parts(parts, ());
                stream.send_response(h3_resp).await?;
                
                while let Some(chunk) = body.frame().await {
                    let frame = chunk?;
                    if let Some(data) = frame.data_ref() {
                        stream.send_data(data.clone()).await?;
                    }
                }
                stream.finish().await?;
            }
        }

        Ok(())
    }

    /// Plain TCP HTTP request forwarding (handles Alt-Svc header injection to dynamically advertise H3)
    async fn handle_request(
        &self,
        req: Request<Incoming>,
        client_addr: SocketAddr,
    ) -> Result<Response<GenericBody>, Infallible> {
        self.metrics.inc_requests();
        let path = req.uri().path().to_string();

        // 1. Select load balancer based on path routing
        let mut selected_lb = self.default_lb.clone();
        for route in &self.routes {
            if path.starts_with(&route.path) {
                selected_lb = route.lb.clone();
                break;
            }
        }

        // 2. Select backend
        let backend_addr = match selected_lb.select() {
            Some(addr) => addr,
            None => {
                error!(
                    "[{}] No backends available for HTTP request to '{}'",
                    self.config.name, path
                );
                self.metrics.inc_errors();
                return Ok(self.error_response(
                    StatusCode::BAD_GATEWAY,
                    "Gateway Error: No backends configured or available.",
                ));
            }
        };

        // 3. Prepare the forwarded request
        let (mut parts, body) = req.into_parts();

        // Rewrite URI to backend
        let backend_uri_string = format!("http://{}{}", backend_addr, path);
        let target_uri = match backend_uri_string.parse::<hyper::Uri>() {
            Ok(uri) => uri,
            Err(e) => {
                error!(
                    "[{}] Failed to parse backend URI '{}': {}",
                    self.config.name, backend_uri_string, e
                );
                self.metrics.inc_errors();
                return Ok(self.error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal Error: Bad backend URI format.",
                ));
            }
        };

        parts.uri = target_uri;

        // Apply HTTP Header Rules
        if let Some(ref http_rules) = self.config.http_rules
            && let Some(ref headers_conf) = http_rules.headers {
                // Inject custom headers
                if let Some(ref inject) = headers_conf.inject {
                    for (k, v) in inject {
                        if let Ok(name) = hyper::header::HeaderName::from_bytes(k.as_bytes())
                            && let Ok(value) = hyper::header::HeaderValue::from_str(v) {
                                parts.headers.insert(name, value);
                            }
                    }
                }

                // Remove configured headers
                if let Some(ref remove) = headers_conf.remove {
                    for k in remove {
                        if let Ok(name) = hyper::header::HeaderName::from_bytes(k.as_bytes()) {
                            parts.headers.remove(name);
                        }
                    }
                }
            }

        // Inject standard proxy headers
        if let Ok(client_ip_val) = hyper::header::HeaderValue::from_str(&client_addr.ip().to_string()) {
            parts.headers.insert(
                hyper::header::HeaderName::from_static("x-forwarded-for"),
                client_ip_val,
            );
        }
        let proto_val = hyper::header::HeaderValue::from_static("http");
        parts.headers.insert(
            hyper::header::HeaderName::from_static("x-forwarded-proto"),
            proto_val,
        );
        let proxy_val = hyper::header::HeaderValue::from_static("SpectraProxy");
        parts.headers.insert(
            hyper::header::HeaderName::from_static("x-proxy-by"),
            proxy_val,
        );

        let forward_req = Request::from_parts(parts, body);

        // 4. Send request using hyper-util legacy HTTP client
        let client = hyper_util::client::legacy::Client::builder(TokioExecutor::new())
            .build(hyper_util::client::legacy::connect::HttpConnector::new());

        info!(
            "[{}] Routing HTTP request '{}' to backend '{}'",
            self.config.name, path, backend_addr
        );

        let listen_port = self.config.listen_addr.split(':').next_back().unwrap_or("443");
        let alt_svc_val = format!("h3=\":{}\"; ma=86400", listen_port);

        match client.request(forward_req).await {
            Ok(resp) => {
                selected_lb.record_success(&backend_addr);
                let (mut resp_parts, resp_body) = resp.into_parts();
                
                // If SSL/TLS is enabled, advertise H3 over UDP to browsers using Alt-Svc
                if (self.config.cert_path.is_some() || self.config.acme_domain.is_some())
                    && let Ok(alt_val) = hyper::header::HeaderValue::from_str(&alt_svc_val) {
                        resp_parts.headers.insert(
                            hyper::header::HeaderName::from_static("alt-svc"),
                            alt_val,
                        );
                    }

                let boxed_body = resp_body
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
                    .boxed();

                let tx_resp = Response::from_parts(resp_parts, boxed_body);
                Ok(tx_resp)
            }
            Err(e) => {
                error!(
                    "[{}] Failed to forward HTTP request to backend '{}': {}",
                    self.config.name, backend_addr, e
                );
                selected_lb.record_failure(&backend_addr);
                self.metrics.inc_errors();
                Ok(self.error_response(
                    StatusCode::BAD_GATEWAY,
                    &format!("Bad Gateway: Request to backend failed: {}", e),
                ))
            }
        }
    }

    fn error_response(&self, status: StatusCode, msg: &str) -> Response<GenericBody> {
        let body_bytes = Bytes::from(msg.to_string());
        let boxed_body = Full::new(body_bytes)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            .boxed();

        Response::builder()
            .status(status)
            .header(hyper::header::CONTENT_TYPE, "text/plain")
            .body(boxed_body)
            .unwrap()
    }
}
