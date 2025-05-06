use crate::{bootstrap::BootstrapServerConfig, config::NodeConfig};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct BootstrapServerToml {
    pub enable: Option<bool>,
    pub batch_size: Option<usize>,
    pub max_queue: Option<usize>,
    pub threads: Option<usize>,
    pub limiter: Option<usize>,
}

impl From<&BootstrapServerToml> for BootstrapServerConfig {
    fn from(toml: &BootstrapServerToml) -> Self {
        let mut config = BootstrapServerConfig::default();

        if let Some(max_queue) = toml.max_queue {
            config.max_queue = max_queue;
        }
        if let Some(threads) = toml.threads {
            config.threads = threads;
        }
        if let Some(batch_size) = toml.batch_size {
            config.batch_size = batch_size;
        }
        if let Some(limiter) = toml.limiter {
            config.limiter = limiter;
        }
        config
    }
}

impl From<&NodeConfig> for BootstrapServerToml {
    fn from(config: &NodeConfig) -> Self {
        Self {
            enable: Some(config.enable_bootstrap_responder),
            max_queue: Some(config.bootstrap_server.max_queue),
            threads: Some(config.bootstrap_server.threads),
            batch_size: Some(config.bootstrap_server.batch_size),
            limiter: Some(config.bootstrap_server.limiter),
        }
    }
}
