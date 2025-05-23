use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};

use rsnano_stats::{StatsCollection, StatsSource};

use super::{process_queue::ProcessQueue, BlockContext, BlockProcessorConfig, RollbackRequest};

pub(crate) enum BlockProcessorAction {
    RollBack(RollbackRequest),
    Process(VecDeque<Arc<BlockContext>>),
    Wait,
}

pub(crate) struct BlockProcessorQueue {
    pub process_queue: ProcessQueue,
    pub rollback_queue: VecDeque<RollbackRequest>,
    last_log: Option<Instant>,
    pub stopped: bool,
    pub cool_down: bool,
    config: BlockProcessorConfig,
    cooldown_count: u64,
}

impl BlockProcessorQueue {
    pub fn new(config: BlockProcessorConfig) -> Self {
        Self {
            process_queue: ProcessQueue::new(config.queue.clone()),
            rollback_queue: VecDeque::new(),
            last_log: None,
            stopped: false,
            cool_down: false,
            config: config.clone(),
            cooldown_count: 0,
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

    pub fn can_log(&self) -> bool {
        if let Some(last) = &self.last_log {
            last.elapsed() >= Duration::from_secs(15)
        } else {
            true
        }
    }

    pub fn logged(&mut self) {
        self.last_log = Some(Instant::now());
    }

    pub fn pop(&mut self) -> Option<BlockProcessorAction> {
        if self.stopped {
            return None;
        }

        if self.cool_down {
            // It's possible that ledger processing happens faster than the
            // notifications can be processed by other components, cooldown here
            self.cooldown_count += 1;
            return Some(BlockProcessorAction::Wait);
        }

        if let Some(request) = self.rollback_queue.pop_front() {
            return Some(BlockProcessorAction::RollBack(request));
        }

        let batch = self.process_queue.next_batch(self.config.batch_size);
        if !batch.is_empty() {
            return Some(BlockProcessorAction::Process(batch));
        }

        Some(BlockProcessorAction::Wait)
    }
}

impl StatsSource for BlockProcessorQueue {
    fn collect_stats(&self, result: &mut StatsCollection) {
        result.insert("block_processor", "cooldown", self.cooldown_count);
    }
}
