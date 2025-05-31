use super::GlobalConfig;
use crate::block_processing::{BacklogScanConfig, ProcessQueueConfig};
use rsnano_network::bandwidth_limiter::BandwidthLimiterConfig;

impl From<&GlobalConfig> for ProcessQueueConfig {
    fn from(value: &GlobalConfig) -> Self {
        value.node_config.block_processor.clone()
    }
}

impl From<&GlobalConfig> for BacklogScanConfig {
    fn from(value: &GlobalConfig) -> Self {
        value.node_config.backlog_scan.clone()
    }
}

impl From<&GlobalConfig> for BandwidthLimiterConfig {
    fn from(value: &GlobalConfig) -> Self {
        Self {
            generic_limit: value.node_config.bandwidth_limit,
            generic_burst_ratio: value.node_config.bandwidth_limit_burst_ratio,
            bootstrap_limit: value.node_config.bootstrap_bandwidth_limit,
            bootstrap_burst_ratio: value.node_config.bootstrap_bandwidth_burst_ratio,
        }
    }
}
