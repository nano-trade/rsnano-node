use std::{
    sync::{Arc, Mutex},
    thread::JoinHandle,
};

use rsnano_core::BlockHash;
use rsnano_ledger::Ledger;
use rsnano_stats::{Stats, StatsCollection, StatsSource};

use super::{BlockProcessorAction, BlockProcessorQueue, UncheckedMap};
use crate::block_processing::{
    block_batch_processor::BlockBatchProcessor, block_batch_rollback::BlockBatchRollback,
};

pub struct BlockProcessor {
    thread: Mutex<Option<JoinHandle<()>>>,
    queue: Arc<BlockProcessorQueue>,
    ledger: Arc<Ledger>,
    unchecked: Arc<UncheckedMap>,
    stats: Arc<Stats>,
    can_roll_back: Mutex<Option<Box<dyn Fn(&BlockHash) -> bool + Send + Sync>>>,
}

impl BlockProcessor {
    pub(crate) fn new(
        queue: Arc<BlockProcessorQueue>,
        ledger: Arc<Ledger>,
        unchecked_map: Arc<UncheckedMap>,
        stats: Arc<Stats>,
    ) -> Self {
        Self {
            queue,
            ledger,
            unchecked: unchecked_map,
            stats,
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

        let mut rollback = BlockBatchRollback {
            ledger: self.ledger.clone(),
            can_roll_back: self
                .can_roll_back
                .lock()
                .unwrap()
                .take()
                .unwrap_or_else(|| Box::new(|_| true)),
        };

        let process = BlockBatchProcessor {
            ledger: self.ledger.clone(),
            unchecked: self.unchecked.clone(),
            stats: self.stats.clone(),
        };

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
    fn collect_stats(&self, _result: &mut StatsCollection) {
        // TODO
    }
}
