use std::{
    collections::VecDeque,
    sync::{Arc, Condvar, Mutex},
};

use strum::IntoEnumIterator;

use rsnano_core::{
    utils::{ContainerInfo, FairQueueInfo},
    BlockHash,
};
use rsnano_ledger::BlockSource;
use rsnano_network::{ChannelId, DeadChannelCleanupStep};

use super::{
    process_queue::{ProcessQueue, ProcessQueueConfig},
    BlockContext,
};

pub(crate) enum BlockProcessorAction {
    RollBack(RollbackRequest),
    Process(VecDeque<Arc<BlockContext>>),
}

pub struct RollbackRequest {
    pub targets: Vec<BlockHash>,
    pub max_rollbacks: usize,
    pub result: Arc<RollbackResult>,
}

pub struct RollbackResult {
    pub rolled_back: Mutex<Option<Vec<BlockHash>>>,
    pub done: Condvar,
}

impl RollbackResult {
    pub fn new() -> Self {
        Self {
            rolled_back: Mutex::new(None),
            done: Condvar::new(),
        }
    }
}

pub(crate) struct BlockProcessorQueue {
    queue: Mutex<BlockProcessorQueueImpl>,
    condition: Condvar,
}

impl BlockProcessorQueue {
    pub fn new(config: ProcessQueueConfig) -> Self {
        Self {
            queue: Mutex::new(BlockProcessorQueueImpl::new(config)),
            condition: Condvar::new(),
        }
    }

    pub fn stop(&self) {
        {
            let mut queue = self.queue.lock().unwrap();
            queue.stopped = true;

            for req in queue.rollback_queue.drain(..) {
                *req.result.rolled_back.lock().unwrap() = Some(Vec::new());
                req.result.done.notify_all();
            }
        }
        self.condition.notify_all();
    }

    pub fn set_cooldown(&self, cool_down: bool) {
        self.queue.lock().unwrap().cool_down = cool_down;
        self.condition.notify_all();
    }

    pub fn is_cooling_down(&self) -> bool {
        self.queue.lock().unwrap().cool_down
    }

    pub fn pop_blocking(&self) -> Option<BlockProcessorAction> {
        let mut queue = self.queue.lock().unwrap();

        loop {
            if queue.stopped {
                return None;
            }

            if !queue.cool_down {
                if let Some(request) = queue.rollback_queue.pop_front() {
                    return Some(BlockProcessorAction::RollBack(request));
                }

                let batch = queue.process_queue.next_batch();
                if !batch.is_empty() {
                    return Some(BlockProcessorAction::Process(batch));
                }
            }

            queue = self
                .condition
                .wait_while(queue, |i| i.should_wait())
                .unwrap();
        }
    }

    // TODO: Remove and replace all checks with calls to size (block_source)
    pub fn total_queue_len(&self) -> usize {
        self.queue.lock().unwrap().process_queue.len()
    }

    pub fn queue_len(&self, source: BlockSource) -> usize {
        self.queue.lock().unwrap().process_queue.source_len(source)
    }

    pub fn push(&self, context: Arc<BlockContext>, channel_id: ChannelId) -> bool {
        let added;
        {
            let mut guard = self.queue.lock().unwrap();
            if guard.stopped {
                return false;
            }

            added = guard
                .process_queue
                .push(context.source, channel_id, context);
        }
        if added {
            self.condition.notify_all();
        }
        added
    }

    pub fn roll_back(&self, request: RollbackRequest) -> bool {
        {
            let mut guard = self.queue.lock().unwrap();
            if guard.stopped {
                return false;
            }

            guard.rollback_queue.push_back(request);
        }
        self.condition.notify_all();
        true
    }

    pub fn info(&self) -> FairQueueInfo<BlockSource> {
        self.queue.lock().unwrap().process_queue.info()
    }

    pub fn container_info(&self) -> ContainerInfo {
        self.queue.lock().unwrap().process_queue.container_info()
    }
}

impl Default for BlockProcessorQueue {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

impl DeadChannelCleanupStep for BlockProcessorQueue {
    fn clean_up_dead_channels(&self, dead_channel_ids: &[ChannelId]) {
        let mut guard = self.queue.lock().unwrap();
        for channel_id in dead_channel_ids {
            let iter = BlockSource::iter();
            for source in iter {
                guard.process_queue.remove(source, *channel_id)
            }
        }
    }
}

struct BlockProcessorQueueImpl {
    process_queue: ProcessQueue,
    rollback_queue: VecDeque<RollbackRequest>,
    stopped: bool,
    cool_down: bool,
}

impl BlockProcessorQueueImpl {
    pub fn new(config: ProcessQueueConfig) -> Self {
        Self {
            process_queue: ProcessQueue::new(config),
            rollback_queue: VecDeque::new(),
            stopped: false,
            cool_down: false,
        }
    }

    pub fn should_wait(&self) -> bool {
        if self.stopped {
            return false;
        }

        if self.cool_down {
            return true;
        }

        self.process_queue.is_empty() && self.rollback_queue.is_empty()
    }
}
