use super::BlockContext;
use rsnano_core::{
    utils::{ContainerInfo, FairQueue, FairQueueInfo},
    Block,
};
use rsnano_ledger::BlockSource;
use rsnano_network::ChannelId;
use std::{
    collections::VecDeque,
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

    pub fn next_batch(&mut self, max_count: usize) -> VecDeque<Arc<BlockContext>> {
        let mut results = VecDeque::new();
        while !self.is_empty() && results.len() < max_count {
            results.push_back(self.next());
        }
        results
    }

    fn next(&mut self) -> Arc<BlockContext> {
        if !self.0.is_empty() {
            let ((source, _), request) = self.0.next().unwrap();
            assert!(source != BlockSource::Forced || request.source == BlockSource::Forced);
            return request;
        }

        panic!("next() called when no blocks are ready");
    }

    pub fn remove(&mut self, source: BlockSource, channel_id: ChannelId) {
        self.0.remove(&(source, channel_id));
    }

    pub fn queue_len(&self, source: BlockSource) -> usize {
        self.0
            .sum_queue_len((source, ChannelId::MIN)..=(source, ChannelId::MAX))
    }

    pub fn info(&self) -> FairQueueInfo<BlockSource> {
        self.compacted_info(|(source, _)| *source)
    }

    pub fn container_info(&self) -> ContainerInfo {
        ContainerInfo::builder()
            .leaf("blocks", self.0.len(), size_of::<Arc<Block>>())
            .leaf(
                "forced",
                self.0
                    .queue_len(&(BlockSource::Forced, ChannelId::LOOPBACK)),
                size_of::<Arc<Block>>(),
            )
            .node("queue", self.0.container_info())
            .finish()
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
