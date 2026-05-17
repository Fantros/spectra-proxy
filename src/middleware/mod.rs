use std::net::IpAddr;
use std::sync::Arc;

pub mod rate_limiter;
pub mod load_balancer;

#[derive(Debug, Clone)]
pub struct ConnectionContext {
    pub client_ip: IpAddr,
    pub target_backend: Option<String>,
}

impl ConnectionContext {
    pub fn new(client_ip: IpAddr) -> Self {
        Self {
            client_ip,
            target_backend: None,
        }
    }
}

pub trait Middleware: Send + Sync {
    /// Returns the human-readable static identifier of this middleware.
    fn name(&self) -> &'static str;
    
    /// Processes the connection context. Returns true to continue, false to reject.
    fn handle(&self, ctx: &mut ConnectionContext) -> bool;
}

pub struct MiddlewareChain {
    middlewares: Vec<Arc<dyn Middleware>>,
}

impl MiddlewareChain {
    pub fn new() -> Self {
        Self {
            middlewares: Vec::new(),
        }
    }

    pub fn add(&mut self, middleware: Arc<dyn Middleware>) {
        self.middlewares.push(middleware);
    }

    /// Evaluates the client connection through all configured middlewares.
    /// Returns Ok(ConnectionContext) if all filters pass,
    /// or Err(name) with the identifier of the middleware that rejected it.
    pub fn execute(&self, client_ip: IpAddr) -> Result<ConnectionContext, &'static str> {
        let mut ctx = ConnectionContext::new(client_ip);
        for mw in &self.middlewares {
            if !mw.handle(&mut ctx) {
                return Err(mw.name());
            }
        }
        Ok(ctx)
    }
}
