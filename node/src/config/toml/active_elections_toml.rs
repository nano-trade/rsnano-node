use crate::config::NodeConfig;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct ActiveElectionsToml {
    pub confirmation_cache: Option<usize>,
    pub confirmation_history_size: Option<usize>,
    pub hinted_limit_percentage: Option<usize>,
    pub optimistic_limit_percentage: Option<usize>,
    pub size: Option<usize>,
}

impl From<&NodeConfig> for ActiveElectionsToml {
    fn from(config: &NodeConfig) -> Self {
        Self {
            size: Some(config.active_elections.max_elections),
            hinted_limit_percentage: Some(config.hinted_scheduler.hinted_limit_percentage),
            optimistic_limit_percentage: Some(
                config.optimistic_scheduler.optimistic_limit_percentage,
            ),
            confirmation_history_size: Some(config.confirmation_history_size),
            confirmation_cache: Some(config.active_elections.confirmation_cache),
        }
    }
}
