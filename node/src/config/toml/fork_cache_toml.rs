use crate::config::NodeConfig;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct ForkCacheToml {
    pub max_size: Option<usize>,
    pub max_forks_per_root: Option<usize>,
}

impl From<&NodeConfig> for ForkCacheToml {
    fn from(value: &NodeConfig) -> Self {
        Self {
            max_size: Some(value.fork_cache_max_size),
            max_forks_per_root: Some(value.fork_cache_max_forks_per_root),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn convert_from_node_config() {
        let config = NodeConfig {
            fork_cache_max_size: 123,
            fork_cache_max_forks_per_root: 11,
            ..NodeConfig::new_test_instance()
        };

        let fork_cache = ForkCacheToml::from(&config);

        assert_eq!(fork_cache.max_forks_per_root, Some(11));
        assert_eq!(fork_cache.max_size, Some(123));
    }
}
