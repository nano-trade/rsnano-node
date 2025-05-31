use super::GlobalConfig;
use crate::block_processing::{BacklogScanConfig, BlockProcessorConfig};
use rsnano_network::bandwidth_limiter::BandwidthLimiterConfig;
use std::time::Duration;

impl From<&GlobalConfig> for BlockProcessorConfig {
    fn from(value: &GlobalConfig) -> Self {
        let config = &value.node_config.block_processor;
        Self {
            queue: config.queue.clone(),
            batch_max_time: Duration::from_millis(
                value.node_config.block_processor_batch_max_time_ms as u64,
            ),
            full_size: value.flags.block_processor_full_size,
        }
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
