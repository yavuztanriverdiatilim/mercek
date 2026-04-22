use anyhow::Result;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub listeners: ListenerConfig,
    #[serde(default)]
    pub queue: QueueConfig,
    #[serde(default)]
    pub db: DbConfig,
    #[serde(default)]
    pub batching: BatchingConfig,
    #[serde(default)]
    pub retry: RetryConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub limits: LimitsConfig,
    #[serde(default)]
    pub log: LogConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ListenerConfig {
    #[serde(default = "default_udp_addr")]
    pub udp_addr: String,
    #[serde(default = "default_tcp_addr")]
    pub tcp_addr: String,
    #[serde(default = "default_tls_addr")]
    pub tls_addr: String,
    #[serde(default = "default_true")]
    pub enable_udp: bool,
    #[serde(default = "default_true")]
    pub enable_tcp: bool,
    #[serde(default)]
    pub enable_tls: bool,
    #[serde(default)]
    pub tls_cert_path: Option<String>,
    #[serde(default)]
    pub tls_key_path: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QueuePolicy {
    #[default]
    DropNewest,
    Backpressure,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueueConfig {
    #[serde(default = "default_queue_capacity")]
    pub ingress_capacity: usize,
    #[serde(default = "default_queue_capacity")]
    pub db_capacity: usize,
    #[serde(default)]
    pub policy: QueuePolicy,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DbConfig {
    #[serde(default = "default_db_dsn")]
    pub dsn: String,
    #[serde(default = "default_pool_max")]
    pub pool_max_size: usize,
    #[serde(default = "default_pool_timeout_ms")]
    pub pool_timeout_ms: u64,
    #[serde(default = "default_true")]
    pub auto_migrate: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BatchingConfig {
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_batch_timeout_ms")]
    pub flush_interval_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RetryConfig {
    #[serde(default = "default_retry_attempts")]
    pub attempts: usize,
    #[serde(default = "default_retry_backoff_ms")]
    pub backoff_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsConfig {
    #[serde(default = "default_metrics_addr")]
    pub bind_addr: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LimitsConfig {
    #[serde(default = "default_max_message_bytes")]
    pub max_message_bytes: usize,
    #[serde(default = "default_max_tcp_connections")]
    pub max_tcp_connections: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LogConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            listeners: ListenerConfig::default(),
            queue: QueueConfig::default(),
            db: DbConfig::default(),
            batching: BatchingConfig::default(),
            retry: RetryConfig::default(),
            metrics: MetricsConfig::default(),
            limits: LimitsConfig::default(),
            log: LogConfig::default(),
        }
    }
}

impl Default for ListenerConfig {
    fn default() -> Self {
        Self {
            udp_addr: default_udp_addr(),
            tcp_addr: default_tcp_addr(),
            tls_addr: default_tls_addr(),
            enable_udp: true,
            enable_tcp: true,
            enable_tls: false,
            tls_cert_path: None,
            tls_key_path: None,
        }
    }
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            ingress_capacity: default_queue_capacity(),
            db_capacity: default_queue_capacity(),
            policy: QueuePolicy::DropNewest,
        }
    }
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            dsn: default_db_dsn(),
            pool_max_size: default_pool_max(),
            pool_timeout_ms: default_pool_timeout_ms(),
            auto_migrate: true,
        }
    }
}

impl Default for BatchingConfig {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            flush_interval_ms: default_batch_timeout_ms(),
        }
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            attempts: default_retry_attempts(),
            backoff_ms: default_retry_backoff_ms(),
        }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            bind_addr: default_metrics_addr(),
        }
    }
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_message_bytes: default_max_message_bytes(),
            max_tcp_connections: default_max_tcp_connections(),
        }
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
        }
    }
}

impl AppConfig {
    pub fn load() -> Result<Self> {
        let settings = config::Config::builder()
            .add_source(config::Environment::with_prefix("MERCEK").separator("__"))
            .build()?;

        let mut cfg = Self::default();
        if let Ok(parsed) = settings.try_deserialize::<Self>() {
            cfg = parsed;
        }
        Ok(cfg)
    }
}

fn default_udp_addr() -> String {
    "0.0.0.0:5514".to_string()
}

fn default_tcp_addr() -> String {
    "0.0.0.0:5514".to_string()
}

fn default_tls_addr() -> String {
    "0.0.0.0:6514".to_string()
}

fn default_metrics_addr() -> String {
    "0.0.0.0:9000".to_string()
}

fn default_db_dsn() -> String {
    "host=127.0.0.1 user=postgres password=postgres dbname=postgres".to_string()
}

fn default_batch_size() -> usize {
    500
}

fn default_batch_timeout_ms() -> u64 {
    200
}

fn default_retry_attempts() -> usize {
    5
}

fn default_retry_backoff_ms() -> u64 {
    250
}

fn default_queue_capacity() -> usize {
    10_000
}

fn default_max_message_bytes() -> usize {
    64 * 1024
}

fn default_max_tcp_connections() -> usize {
    2_000
}

fn default_pool_max() -> usize {
    16
}

fn default_pool_timeout_ms() -> u64 {
    2_000
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_true() -> bool {
    true
}
