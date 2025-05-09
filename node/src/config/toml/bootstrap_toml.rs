use crate::bootstrap::{state::CandidateAccountsConfig, BootstrapConfig};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Deserialize, Serialize)]
pub struct BootstrapToml {
    pub enable: Option<bool>,
    pub enable_priorities: Option<bool>,
    pub enable_dependency_walker: Option<bool>,
    pub enable_frontier_scan: Option<bool>,
    pub block_processor_threshold: Option<usize>,
    pub database_rate_limit: Option<usize>,
    pub frontier_rate_limit: Option<usize>,
    pub database_warmup_ratio: Option<usize>,
    pub max_pull_count: Option<u8>,
    pub channel_limit: Option<usize>,
    pub rate_limit: Option<usize>,
    pub throttle_coefficient: Option<usize>,
    pub throttle_wait: Option<u64>,
    pub request_timeout: Option<u64>,
    pub max_requests: Option<usize>,
    pub optimistic_request_percentage: Option<u8>,
    pub account_sets: Option<AccountSetsToml>,
    pub frontier_scan: Option<FrontierScanToml>,
}

impl From<&BootstrapConfig> for BootstrapToml {
    fn from(config: &BootstrapConfig) -> Self {
        Self {
            enable: Some(config.enable),
            enable_priorities: Some(config.enable_priorities),
            enable_dependency_walker: Some(config.enable_dependency_walker),
            enable_frontier_scan: Some(config.enable_frontier_scan),
            channel_limit: Some(config.channel_limit),
            rate_limit: Some(config.rate_limit),
            database_rate_limit: Some(config.database_rate_limit),
            frontier_rate_limit: Some(config.frontier_rate_limit),
            database_warmup_ratio: Some(config.database_warmup_ratio),
            max_pull_count: Some(config.max_pull_count),
            request_timeout: Some(config.request_timeout.as_millis() as u64),
            throttle_coefficient: Some(config.throttle_coefficient),
            throttle_wait: Some(config.throttle_wait.as_millis() as u64),
            block_processor_threshold: Some(config.block_processor_theshold),
            max_requests: Some(config.max_requests),
            optimistic_request_percentage: Some(config.optimistic_request_percentage),
            account_sets: Some((&config.candidate_accounts).into()),
            frontier_scan: Some(config.into()),
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct AccountSetsToml {
    pub blocking_max: Option<usize>,
    pub blocking_decay: Option<usize>,
    pub consideration_count: Option<usize>,
    pub cooldown: Option<u64>,
    pub priorities_max: Option<usize>,
}

impl Default for AccountSetsToml {
    fn default() -> Self {
        let config = CandidateAccountsConfig::default();
        Self {
            consideration_count: Some(config.consideration_count),
            priorities_max: Some(config.priorities_max),
            blocking_max: Some(config.blocking_max),
            blocking_decay: Some(config.blocking_decay.as_secs() as usize),
            cooldown: Some(config.cooldown.as_millis() as u64),
        }
    }
}

impl From<&CandidateAccountsConfig> for AccountSetsToml {
    fn from(value: &CandidateAccountsConfig) -> Self {
        Self {
            consideration_count: Some(value.consideration_count),
            priorities_max: Some(value.priorities_max),
            blocking_max: Some(value.blocking_max),
            cooldown: Some(value.cooldown.as_millis() as u64),
            blocking_decay: Some(value.blocking_decay.as_secs() as usize),
        }
    }
}

impl From<&AccountSetsToml> for CandidateAccountsConfig {
    fn from(toml: &AccountSetsToml) -> Self {
        let mut config = CandidateAccountsConfig::default();

        if let Some(blocking_max) = toml.blocking_max {
            config.blocking_max = blocking_max;
        }
        if let Some(consideration_count) = toml.consideration_count {
            config.consideration_count = consideration_count;
        }
        if let Some(priorities_max) = toml.priorities_max {
            config.priorities_max = priorities_max;
        }
        if let Some(cooldown) = &toml.cooldown {
            config.cooldown = Duration::from_millis(*cooldown);
        }
        if let Some(decay) = &toml.blocking_decay {
            config.blocking_decay = Duration::from_secs(*decay as u64);
        }
        config
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct FrontierScanToml {
    pub head_parallelism: Option<usize>,
    pub consideration_count: Option<usize>,
    pub candidates: Option<usize>,
    pub cooldown: Option<u64>,
    pub max_pending: Option<usize>,
}

impl From<&BootstrapConfig> for FrontierScanToml {
    fn from(value: &BootstrapConfig) -> Self {
        Self {
            head_parallelism: Some(value.frontier_scan.parallelism),
            consideration_count: Some(value.frontier_scan.consideration_count),
            candidates: Some(value.frontier_scan.candidates),
            cooldown: Some(value.frontier_scan.cooldown.as_millis() as u64),
            max_pending: Some(value.max_pending_frontier_responses),
        }
    }
}

impl BootstrapConfig {
    pub(crate) fn merge_toml(&mut self, toml: &FrontierScanToml) {
        if let Some(i) = toml.head_parallelism {
            self.frontier_scan.parallelism = i;
        }
        if let Some(i) = toml.consideration_count {
            self.frontier_scan.consideration_count = i;
        }
        if let Some(i) = toml.candidates {
            self.frontier_scan.candidates = i;
        }
        if let Some(i) = toml.cooldown {
            self.frontier_scan.cooldown = Duration::from_millis(i);
        }
        if let Some(i) = toml.max_pending {
            self.max_pending_frontier_responses = i;
        }
    }
}
