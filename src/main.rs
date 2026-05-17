use clap::Parser;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

mod config;
mod http;
mod metrics;
mod tcp;
mod ui;
mod udp;
mod tls;
mod middleware;

use config::Config;
use middleware::load_balancer::LoadBalancer;
use metrics::MetricsTracker;
use ui::{TuiDashboard, TuiServiceInfo};

#[derive(Parser, Debug)]
#[command(name = "Spectra Proxy", version = "0.1.0", author = "Fantros")]
struct Args {
    /// Path to the configuration TOML file
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,
    
    /// Enable logging to stdout (disables interactive TUI mode)
    #[arg(short, long)]
    log_only: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();

    // 1. Initialize Tracing Subscriber (modern logging)
    if args.log_only {
        let subscriber = FmtSubscriber::builder()
            .with_max_level(Level::INFO)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("Failed to set tracing subscriber");
        print_banner();
    } else {
        // Log to file if in interactive TUI mode to avoid corrupting Ratatui
        let file_appender = std::fs::File::create("spectra-proxy.log")?;
        let subscriber = FmtSubscriber::builder()
            .with_writer(std::sync::Mutex::new(file_appender))
            .with_max_level(Level::INFO)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("Failed to set tracing subscriber");
    }

    // 2. Write a default config if it doesn't exist
    if !args.config.exists() {
        info!("Configuration file '{}' not found. Creating a beautiful default TOML configuration...", args.config.display());
        write_default_config(&args.config)?;
    }

    // 3. Load config once to get initial service names for metrics tracker
    let config = match Config::load_from_file(&args.config) {
        Ok(cfg) => cfg,
        Err(e) => {
            error!("Failed to load configuration file '{}': {}", args.config.display(), e);
            std::process::exit(1);
        }
    };

    // 4. Initialize metrics tracker (supports dynamic registration!)
    let service_names: Vec<String> = config.services.iter().map(|s| s.name.clone()).collect();
    let metrics_tracker = MetricsTracker::new(&service_names);

    // 5. Shared thread-safe service metadata container for TUI updates
    let shared_services = Arc::new(RwLock::new(Vec::<TuiServiceInfo>::new()));

    // 6. Reload channel coordinating config reloads
    let (reload_tx, mut reload_rx) = mpsc::channel::<()>(10);

    // 7. Spawn cross-platform config file modification watcher
    let config_path_clone = args.config.clone();
    let reload_tx_clone = reload_tx.clone();
    tokio::spawn(async move {
        let mut last_modified = std::fs::metadata(&config_path_clone)
            .and_then(|m| m.modified())
            .unwrap_or_else(|_| std::time::SystemTime::now());

        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if let Ok(metadata) = std::fs::metadata(&config_path_clone)
                && let Ok(modified) = metadata.modified()
                    && modified > last_modified {
                        last_modified = modified;
                        info!("config.toml modification detected. Triggering reload...");
                        let _ = reload_tx_clone.send(()).await;
                    }
        }
    });

    // 8. Spawn Unix SIGHUP listener (equivalent to Nginx's SIGHUP reload)
    #[cfg(unix)]
    {
        let reload_tx_clone = reload_tx.clone();
        tokio::spawn(async move {
            use tokio::signal::unix::{signal, SignalKind};
            if let Ok(mut stream) = signal(SignalKind::hangup()) {
                while stream.recv().await.is_some() {
                    info!("SIGHUP signal received. Triggering configuration hot reload...");
                    let _ = reload_tx_clone.send(()).await;
                }
            }
        });
    }

    // 9. Spawn active service supervisor task
    let shared_services_clone = shared_services.clone();
    let metrics_tracker_clone = metrics_tracker.clone();
    let config_path_clone = args.config.clone();
    tokio::spawn(async move {
        let mut active_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

        // Initial launch
        spawn_services(
            &config_path_clone,
            &metrics_tracker_clone,
            &shared_services_clone,
            &mut active_handles,
        ).await;

        // Reload listening loop
        while reload_rx.recv().await.is_some() {
            info!("Hot-reloading proxy servers gracefully...");
            
            // Abort only the main socket listeners. Ongoing active client streams
            // are spawned independently and will finish naturally in the background!
            for handle in active_handles.drain(..) {
                handle.abort();
            }

            // Yield briefly to let old listener sockets unbind cleanly
            tokio::time::sleep(Duration::from_millis(150)).await;

            // Spawn new configuration
            spawn_services(
                &config_path_clone,
                &metrics_tracker_clone,
                &shared_services_clone,
                &mut active_handles,
            ).await;
        }
    });

    // 10. Start UI Dashboard or block on Ctrl+C in Log-Only Mode
    if args.log_only {
        info!("Spectra Proxy successfully initialized in Log-Only mode. Press Ctrl+C to terminate.");
        tokio::signal::ctrl_c().await?;
        info!("Shutting down Spectra Proxy gracefully. Goodbye!");
    } else {
        // Run Ratatui Dashboard (TUI blocks on main thread until 'q' is pressed)
        let dashboard = TuiDashboard::new(shared_services, metrics_tracker);
        if let Err(e) = dashboard.run() {
            eprintln!("TUI Dashboard error: {}", e);
            std::process::exit(1);
        }
    }

    Ok(())
}

/// Dynamic service spawner. Compiles service proxies from TOML config, updates 
/// the shared UI vector metadata, and registers active handles.
async fn spawn_services(
    config_path: &Path,
    metrics_tracker: &MetricsTracker,
    shared_services: &Arc<RwLock<Vec<TuiServiceInfo>>>,
    active_handles: &mut Vec<tokio::task::JoinHandle<()>>,
) {
    let config = match Config::load_from_file(config_path) {
        Ok(cfg) => cfg,
        Err(e) => {
            error!("Failed to reload config: {}", e);
            return;
        }
    };

    let mut tui_services = Vec::new();

    for service in config.services {
        let lb = Arc::new(LoadBalancer::new(
            service.backends.clone(),
            service.load_balance,
        ));
        let metrics = metrics_tracker.get(&service.name);

        let protocol_str = match service.protocol {
            config::Protocol::Tcp => {
                if service.cert_path.is_some() || service.acme_domain.is_some() {
                    "tcps".to_string()
                } else {
                    "tcp".to_string()
                }
            }
            config::Protocol::Udp => "udp".to_string(),
            config::Protocol::Http => {
                if service.cert_path.is_some() || service.acme_domain.is_some() {
                    "https".to_string()
                } else {
                    "http".to_string()
                }
            }
        };

        tui_services.push(TuiServiceInfo {
            name: service.name.clone(),
            protocol: protocol_str,
            listen_addr: service.listen_addr.clone(),
        });

        match service.protocol {
            config::Protocol::Tcp => {
                let proxy = tcp::TcpProxy::new(service, lb, metrics);
                let handle = tokio::spawn(async move {
                    if let Err(e) = proxy.run().await {
                        error!("TCP proxy service terminated with error: {}", e);
                    }
                });
                active_handles.push(handle);
            }
            config::Protocol::Udp => {
                let proxy = udp::UdpProxy::new(service, lb, metrics);
                let handle = tokio::spawn(async move {
                    if let Err(e) = proxy.run().await {
                        error!("UDP proxy service terminated with error: {}", e);
                    }
                });
                active_handles.push(handle);
            }
            config::Protocol::Http => {
                let proxy = http::HttpProxy::new(service, lb, metrics);
                let handle = tokio::spawn(async move {
                    if let Err(e) = proxy.run().await {
                        error!("HTTP proxy service terminated with error: {}", e);
                    }
                });
                active_handles.push(handle);
            }
        }
    }

    // Safely update the TUI dashboard list under write lock
    {
        let mut services_guard = shared_services.write().unwrap();
        *services_guard = tui_services;
    }
    info!("Successfully loaded configuration and spawned running proxy services.");
}

fn print_banner() {
    println!(
        r#"
  ____                  _             ____                     
 / ___| _ __   ___  ___| |_ _ __ __ _|  _ \ _ __ _____  _ _    
 \___ \| '_ \ / _ \/ __| __| '__/ _` | |_) | '__/ _ \ \/ / | | 
  ___) | |_) |  __/ (__| |_| | | (_| |  __/| | | (_) >  <| |_| 
 |____/| .__/ \___|\___|\__|_|  \__,_|_|   |_|  \___/_/\_\\__, |
       |_|                                                |___/
      -- High-Performance Multi-Protocol Reverse Proxy --
               [TCP]  •  [HTTP]  •  [UDP]
"#
    );
}

fn write_default_config(path: &Path) -> std::io::Result<()> {
    let default_toml = r#"# ==========================================
# Spectra Proxy Configuration File (TOML)
# Modern, High-Performance Multi-Protocol Proxy
# ==========================================

# List of Proxy Services

# 1. L7 HTTP/HTTPS/H3 Reverse Proxy with Path-based routing and Rate Limiting
[[services]]
name = "web-http-service"
protocol = "http"
listen_addr = "127.0.0.1:8080"
backends = ["127.0.0.1:8081", "127.0.0.1:8082"]
load_balance = "round_robin"

# --- 🛡️ Connection Rate Limiting Middleware (Nginx-style Token Bucket) ---
[services.rate_limit]
max_requests = 20.0       # Burst limit (maximum number of requests held in bucket)
refill_rate = 5.0        # Refill speed (tokens added per second)

# --- 🔒 Dynamic SSL/TLS via Let's Encrypt (ACME) ---
# When enabled, it automatically completes challenges on the same port and caches certs.
# acme_domain = "yourdomain.com"
# acme_email = "admin@yourdomain.com"

# --- 🔑 Static SSL/TLS termination (Fallback if ACME is disabled) ---
# cert_path = "cert.pem" 
# key_path = "key.pem"

[services.http_rules]
routes = [
    { path = "/api/v1", backends = ["127.0.0.1:8083"] },
    { path = "/", backends = ["127.0.0.1:8081", "127.0.0.1:8082"] }
]
[services.http_rules.headers]
inject = { "X-Proxy-Provider" = "SpectraProxy", "X-Environment" = "Production" }
remove = ["Server", "X-Powered-By"]

# 2. L4 TCP Reverse Proxy (Load-Balanced Postgres with Rate Limiting)
[[services]]
name = "database-tcp-service"
protocol = "tcp"
listen_addr = "127.0.0.1:5432"
backends = ["127.0.0.1:5433", "127.0.0.1:5434"]
load_balance = "round_robin"

[services.rate_limit]
max_requests = 100.0
refill_rate = 10.0

# 3. L4 UDP Reverse Proxy (DNS relay example)
[[services]]
name = "dns-udp-service"
protocol = "udp"
listen_addr = "127.0.0.1:5353"
backends = ["8.8.8.8:53", "1.1.1.1:53"]
load_balance = "random"
"#;
    std::fs::write(path, default_toml)
}
