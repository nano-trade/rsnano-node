use super::BlockContext;
use rsnano_core::utils::FairQueue;
use rsnano_ledger::BlockSource;
use rsnano_network::ChannelId;
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

#[derive(Clone, Debug, PartialEq)]
pub struct BlockProcessorQueueConfig {
    // Maximum number of blocks to queue from network peers
    pub max_peer_queue: usize,

    // Maximum number of blocks to queue from system components (local RPC, bootstrap)
    pub max_system_queue: usize,

    // Higher priority gets processed more frequently
    pub priority_live: usize,
    pub priority_bootstrap: usize,
    pub priority_local: usize,
    pub priority_system: usize,
}

impl Default for BlockProcessorQueueConfig {
    fn default() -> Self {
        Self {
            max_peer_queue: 128,
            max_system_queue: 16 * 1024,
            priority_live: 1,
            priority_bootstrap: 8,
            priority_local: 16,
            priority_system: 32,
        }
    }
}

pub(crate) struct BlockProcessorQueue(FairQueue<(BlockSource, ChannelId), Arc<BlockContext>>);

impl BlockProcessorQueue {
    pub fn new(config: BlockProcessorQueueConfig) -> Self {
        let config_l = config.clone();
        let max_size_query = move |origin: &(BlockSource, ChannelId)| match origin.0 {
            BlockSource::Live | BlockSource::LiveOriginator => config_l.max_peer_queue,
            _ => config_l.max_system_queue,
        };

        let config_l = config.clone();
        let priority_query = move |origin: &(BlockSource, ChannelId)| match origin.0 {
            BlockSource::Live | BlockSource::LiveOriginator => config.priority_live,
            BlockSource::Bootstrap | BlockSource::BootstrapLegacy | BlockSource::Unchecked => {
                config_l.priority_bootstrap
            }
            BlockSource::Local => config_l.priority_local,
            BlockSource::Election | BlockSource::Forced | BlockSource::Unknown => {
                config.priority_system
            }
        };

        Self(FairQueue::new(max_size_query, priority_query))
    }
}

impl Deref for BlockProcessorQueue {
    type Target = FairQueue<(BlockSource, ChannelId), Arc<BlockContext>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for BlockProcessorQueue {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
