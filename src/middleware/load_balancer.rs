use crate::config::LoadBalanceMode;
use crate::middleware::{Middleware, ConnectionContext};
use rand::Rng;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;
use std::time::{Instant, Duration};
use std::sync::Mutex;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    Closed,
    Open { until: Instant },
    HalfOpen,
}

#[derive(Debug)]
pub struct BackendNode {
    pub address: String,
    pub state: Mutex<BreakerState>,
    pub consecutive_failures: std::sync::atomic::AtomicU32,
}

impl BackendNode {
    pub fn new(address: String) -> Self {
        Self {
            address,
            state: Mutex::new(BreakerState::Closed),
            consecutive_failures: std::sync::atomic::AtomicU32::new(0),
        }
    }

    /// Checks if the backend is currently available for routing (and manages state transitions dynamically).
    pub fn is_available(&self) -> bool {
        let mut guard = self.state.lock().unwrap();
        match *guard {
            BreakerState::Closed => true,
            BreakerState::HalfOpen => true,
            BreakerState::Open { until } => {
                if Instant::now() >= until {
                    // Cooldown has passed! Upgrade dynamically to Half-Open to probe backend health.
                    *guard = BreakerState::HalfOpen;
                    true
                } else {
                    false // Circuit is tripped, skip this node!
                }
            }
        }
    }

    /// Records a successful request to the backend.
    pub fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::SeqCst);
        let mut guard = self.state.lock().unwrap();
        if *guard == BreakerState::HalfOpen {
            *guard = BreakerState::Closed; // Restored to healthy closed circuit!
        }
    }

    /// Records a failed request to the backend.
    pub fn record_failure(&self, max_failures: u32, cooldown: Duration) {
        let fails = self.consecutive_failures.fetch_add(1, Ordering::SeqCst) + 1;
        if fails >= max_failures {
            let mut guard = self.state.lock().unwrap();
            *guard = BreakerState::Open {
                until: Instant::now() + cooldown,
            };
        }
    }
}

#[derive(Debug)]
pub struct LoadBalancer {
    nodes: RwLock<Vec<Arc<BackendNode>>>,
    mode: LoadBalanceMode,
    counter: AtomicUsize,
}

impl LoadBalancer {
    pub fn new(backends: Vec<String>, mode: LoadBalanceMode) -> Self {
        let nodes = backends.into_iter().map(|addr| Arc::new(BackendNode::new(addr))).collect();
        Self {
            nodes: RwLock::new(nodes),
            mode,
            counter: AtomicUsize::new(0),
        }
    }

    /// Selects a backend based on the load balancing mode, skipping tripped circuit breakers.
    pub fn select(&self) -> Option<String> {
        let nodes = self.nodes.read().unwrap();
        if nodes.is_empty() {
            return None;
        }

        let len = nodes.len();
        match self.mode {
            LoadBalanceMode::RoundRobin => {
                for _ in 0..len {
                    let idx = self.counter.fetch_add(1, Ordering::Relaxed);
                    let node = &nodes[idx % len];
                    if node.is_available() {
                        return Some(node.address.clone());
                    }
                }
                None // All backends tripped!
            }
            LoadBalanceMode::Random => {
                let mut rng = rand::thread_rng();
                let available_nodes: Vec<&Arc<BackendNode>> = nodes.iter().filter(|n| n.is_available()).collect();
                if available_nodes.is_empty() {
                    return None;
                }
                let idx = rng.gen_range(0..available_nodes.len());
                Some(available_nodes[idx].address.clone())
            }
        }
    }

    /// Records a successful request to the backend.
    pub fn record_success(&self, address: &str) {
        let nodes = self.nodes.read().unwrap();
        if let Some(node) = nodes.iter().find(|n| n.address == address) {
            node.record_success();
        }
    }

    /// Records a failed request to the backend.
    pub fn record_failure(&self, address: &str) {
        let nodes = self.nodes.read().unwrap();
        if let Some(node) = nodes.iter().find(|n| n.address == address) {
            node.record_failure(3, Duration::from_secs(10));
        }
    }

    /// Returns a copy of the current backends list.
    #[allow(dead_code)]
    pub fn get_backends(&self) -> Vec<String> {
        self.nodes.read().unwrap().iter().map(|n| n.address.clone()).collect()
    }

    /// Replaces the current backends list with a new one at runtime!
    #[allow(dead_code)]
    pub fn update_backends(&self, new_backends: Vec<String>) {
        let mut nodes = self.nodes.write().unwrap();
        *nodes = new_backends.into_iter().map(|addr| Arc::new(BackendNode::new(addr))).collect();
    }
}

impl Middleware for LoadBalancer {
    fn name(&self) -> &'static str {
        "LoadBalancer"
    }

    fn handle(&self, ctx: &mut ConnectionContext) -> bool {
        if let Some(backend) = self.select() {
            ctx.target_backend = Some(backend);
            true
        } else {
            false // No backends available! Rejects connection in pipeline.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::middleware::ConnectionContext;

    #[test]
    fn test_load_balancer_round_robin() {
        let lb = LoadBalancer::new(
            vec!["127.0.0.1:8081".to_string(), "127.0.0.1:8082".to_string()],
            LoadBalanceMode::RoundRobin,
        );

        // Should alternate between backends in round-robin fashion
        let first = lb.select().unwrap();
        let second = lb.select().unwrap();
        let third = lb.select().unwrap();

        assert_ne!(first, second);
        assert_eq!(first, third);
    }

    #[test]
    fn test_load_balancer_random() {
        let lb = LoadBalancer::new(
            vec!["127.0.0.1:8081".to_string(), "127.0.0.1:8082".to_string()],
            LoadBalanceMode::Random,
        );

        let selection = lb.select();
        assert!(selection.is_some());
        let addr = selection.unwrap();
        assert!(addr == "127.0.0.1:8081" || addr == "127.0.0.1:8082");
    }

    #[test]
    fn test_circuit_breaker_tripping_and_recovery() {
        let lb = LoadBalancer::new(
            vec!["127.0.0.1:8081".to_string()],
            LoadBalanceMode::RoundRobin,
        );

        // Initially healthy
        assert!(lb.select().is_some());

        // Trip the circuit breaker with 3 consecutive failures
        lb.record_failure("127.0.0.1:8081");
        lb.record_failure("127.0.0.1:8081");
        lb.record_failure("127.0.0.1:8081");

        // The only backend is tripped, so select should return None!
        assert!(lb.select().is_none());

        // Update the circuit breaker state to Open with a 0-duration cooldown to test recovery!
        {
            let nodes = lb.nodes.read().unwrap();
            let node = &nodes[0];
            node.record_failure(1, Duration::from_millis(0)); // Trip it with immediate expiration!
        }

        // It should now dynamically transition to Half-Open and allow selection!
        let prob_addr = lb.select();
        assert_eq!(prob_addr, Some("127.0.0.1:8081".to_string()));

        // Record a success in Half-Open state, it should transition back to Closed (healthy)!
        lb.record_success("127.0.0.1:8081");

        // Should remain healthy
        assert!(lb.select().is_some());
    }

    #[test]
    fn test_middleware_trait_integration() {
        let lb = LoadBalancer::new(
            vec!["127.0.0.1:8081".to_string()],
            LoadBalanceMode::RoundRobin,
        );

        let mut ctx = ConnectionContext::new("127.0.0.1".parse().unwrap());
        let handled = lb.handle(&mut ctx);

        assert!(handled);
        assert_eq!(ctx.target_backend, Some("127.0.0.1:8081".to_string()));
    }
}
