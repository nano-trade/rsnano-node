use std::{
    sync::{Arc, Mutex},
    thread::JoinHandle,
};

use rsnano_core::BlockHash;
use rsnano_ledger::Ledger;
use rsnano_stats::{StatsCollection, StatsSource};

use super::{
    block_batch_processor::BlockBatchProcessorStats, BlockProcessorAction, BlockProcessorQueue,
    UncheckedMap,
};
use crate::block_processing::{
    block_batch_processor::BlockBatchProcessor, block_batch_rollback::BlockBatchRollback,
};

pub struct BlockProcessor {
    thread: Mutex<Option<JoinHandle<()>>>,
    queue: Arc<BlockProcessorQueue>,
    ledger: Arc<Ledger>,
    unchecked: Arc<UncheckedMap>,
    process_stats: Arc<BlockBatchProcessorStats>,
    can_roll_back: Mutex<Option<Box<dyn Fn(&BlockHash) -> bool + Send + Sync>>>,
}

impl BlockProcessor {
    pub(crate) fn new(
        queue: Arc<BlockProcessorQueue>,
        ledger: Arc<Ledger>,
        unchecked_map: Arc<UncheckedMap>,
    ) -> Self {
        Self {
            queue,
            ledger,
            unchecked: unchecked_map,
            process_stats: Arc::new(BlockBatchProcessorStats::default()),
            can_roll_back: Mutex::new(None),
            thread: Mutex::new(None),
        }
    }

    // Give other components a chance to veto a rollback
    pub fn can_rolling_back(&mut self, f: impl Fn(&BlockHash) -> bool + Send + Sync + 'static) {
        *self.can_roll_back.lock().unwrap() = Some(Box::new(f));
    }

    pub fn start(&self) {
        debug_assert!(self.thread.lock().unwrap().is_none());
        let queue = self.queue.clone();
        let mut rollback = self.create_rollback_processor();
        let process = self.create_block_batch_processor();

        *self.thread.lock().unwrap() = Some(
            std::thread::Builder::new()
                .name("Blck processing".to_string())
                .spawn(move || {
                    while let Some(action) = queue.pop_blocking() {
                        match action {
                            BlockProcessorAction::RollBack(request) => {
                                rollback.roll_back(request);
                            }
                            BlockProcessorAction::Process(blocks) => {
                                process.process_blocks(blocks);
                            }
                        }
                    }
                })
                .unwrap(),
        );
    }

    fn create_block_batch_processor(&self) -> BlockBatchProcessor {
        BlockBatchProcessor {
            ledger: self.ledger.clone(),
            unchecked: self.unchecked.clone(),
            stats: self.process_stats.clone(),
        }
    }

    fn create_rollback_processor(&self) -> BlockBatchRollback {
        BlockBatchRollback {
            ledger: self.ledger.clone(),
            can_roll_back: self
                .can_roll_back
                .lock()
                .unwrap()
                .take()
                .unwrap_or_else(|| Box::new(|_| true)),
        }
    }

    pub fn stop(&self) {
        self.queue.stop();
        let join_handle = self.thread.lock().unwrap().take();
        if let Some(join_handle) = join_handle {
            join_handle.join().unwrap();
        }
    }
}

impl Drop for BlockProcessor {
    fn drop(&mut self) {
        // Thread must be stopped before destruction
        debug_assert!(self.thread.lock().unwrap().is_none());
    }
}

impl StatsSource for BlockProcessor {
    fn collect_stats(&self, result: &mut StatsCollection) {
        self.process_stats.collect_stats(result);
    }
}
