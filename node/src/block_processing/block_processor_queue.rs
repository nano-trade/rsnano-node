use super::{BlockContext, BlockProcessorConfig};
use rsnano_core::utils::FairQueue;
use rsnano_ledger::BlockSource;
use rsnano_network::ChannelId;
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

pub(crate) struct BlockProcessorQueue(FairQueue<(BlockSource, ChannelId), Arc<BlockContext>>);

impl BlockProcessorQueue {
    pub fn new(config: BlockProcessorConfig) -> Self {
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
