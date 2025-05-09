use crate::config::NodeConfig;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct VoteRebroadcasterToml {
    pub enable: Option<bool>,
    pub max_queue: Option<usize>,
    pub max_history: Option<usize>,
    pub max_representatives: Option<usize>,
    pub rebroadcast_threshold: Option<u64>,
}

impl From<&NodeConfig> for VoteRebroadcasterToml {
    fn from(value: &NodeConfig) -> Self {
        Self {
            enable: Some(value.enable_vote_rebroadcast),
            max_queue: Some(value.vote_rebroadcaster_max_queue),
            max_history: Some(value.rebroadcast_history.max_blocks_per_rep),
            max_representatives: Some(value.rebroadcast_history.max_representatives),
            rebroadcast_threshold: Some(
                value.rebroadcast_history.rebroadcast_min_gap.as_millis() as u64
            ),
        }
    }
}
