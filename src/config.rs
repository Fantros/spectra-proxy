use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub services: Vec<ServiceConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServiceConfig {
    pub name: String,
    pub protocol: Protocol,
    pub listen_addr: String,
    pub backends: Vec<String>,
    #[serde(default)]
    pub load_balance: LoadBalanceMode,
    pub http_rules: Option<HttpRules>,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    pub acme_domain: Option<String>,
    pub acme_email: Option<String>,
    pub rate_limit: Option<RateLimitConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RateLimitConfig {
    pub max_requests: f64,
    pub refill_rate: f64,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum LoadBalanceMode {
    #[default]
    RoundRobin,
    Random,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    Tcp,
    Udp,
    Http,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HttpRules {
    pub routes: Option<Vec<Route>>,
    pub headers: Option<HeadersConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Route {
    pub path: String,
    pub backends: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HeadersConfig {
    pub inject: Option<HashMap<String, String>>,
    pub remove: Option<Vec<String>>,
}

impl Config {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut file = File::open(path)?;
        let mut content = String::new();
        file.read_to_string(&mut content)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }
}
