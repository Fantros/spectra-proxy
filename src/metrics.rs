use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::RwLock;

#[derive(Debug, Default)]
pub struct ServiceMetrics {
    pub active_connections: AtomicUsize,
    pub total_connections: AtomicU64,
    pub bytes_rx: AtomicU64,
    pub bytes_tx: AtomicU64,
    pub total_requests: AtomicU64,
    pub total_errors: AtomicU64,
}

impl ServiceMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc_active_connections(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        self.total_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_active_connections(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn add_rx(&self, bytes: u64) {
        self.bytes_rx.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn add_tx(&self, bytes: u64) {
        self.bytes_tx.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn inc_requests(&self) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_errors(&self) {
        self.total_errors.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone)]
pub struct MetricsTracker {
    services: Arc<RwLock<HashMap<String, Arc<ServiceMetrics>>>>,
}

#[derive(Serialize)]
pub struct ServiceMetricsSnapshot {
    pub active_connections: usize,
    pub total_connections: u64,
    pub bytes_rx: u64,
    pub bytes_tx: u64,
    pub total_requests: u64,
    pub total_errors: u64,
}

impl MetricsTracker {
    pub fn new(service_names: &[String]) -> Self {
        let mut map = HashMap::new();
        for name in service_names {
            map.insert(name.clone(), Arc::new(ServiceMetrics::new()));
        }
        Self {
            services: Arc::new(RwLock::new(map)),
        }
    }

    /// Fetches a service metrics handle, dynamically registering it if it doesn't exist yet (important for config hot-reload!)
    pub fn get(&self, name: &str) -> Arc<ServiceMetrics> {
        // First try reading
        {
            let map = self.services.read().unwrap();
            if let Some(metrics) = map.get(name) {
                return metrics.clone();
            }
        }

        // If not present, write-lock and register
        let mut map = self.services.write().unwrap();
        map.entry(name.to_string())
            .or_insert_with(|| Arc::new(ServiceMetrics::new()))
            .clone()
    }

    pub fn get_all_snapshots(&self) -> HashMap<String, ServiceMetricsSnapshot> {
        let mut snapshots = HashMap::new();
        let map = self.services.read().unwrap();
        for (name, metrics) in map.iter() {
            snapshots.insert(
                name.clone(),
                ServiceMetricsSnapshot {
                    active_connections: metrics.active_connections.load(Ordering::Relaxed),
                    total_connections: metrics.total_connections.load(Ordering::Relaxed),
                    bytes_rx: metrics.bytes_rx.load(Ordering::Relaxed),
                    bytes_tx: metrics.bytes_tx.load(Ordering::Relaxed),
                    total_requests: metrics.total_requests.load(Ordering::Relaxed),
                    total_errors: metrics.total_errors.load(Ordering::Relaxed),
                },
            );
        }
        snapshots
    }
}
