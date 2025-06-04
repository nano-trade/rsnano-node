use std::{
    sync::{Arc, Mutex},
    thread::JoinHandle,
};

use rsnano_ledger::Ledger;
use rsnano_stats::{StatsCollection, StatsSource};

use super::{
    backlog_waiter::BacklogWaiter, block_batch_processor::BlockBatchProcessorStats,
    BlockProcessorQueue, UncheckedMap,
};
use crate::block_processing::block_batch_processor::BlockBatchProcessor;

pub struct BlockProcessor {
    thread: Mutex<Option<JoinHandle<()>>>,
    queue: Arc<BlockProcessorQueue>,
    ledger: Arc<Ledger>,
    unchecked: Arc<UncheckedMap>,
    process_stats: Arc<BlockBatchProcessorStats>,
    backlog_waiter: Arc<BacklogWaiter>,
}

impl BlockProcessor {
    pub(crate) fn new(
        queue: Arc<BlockProcessorQueue>,
        ledger: Arc<Ledger>,
        unchecked_map: Arc<UncheckedMap>,
        backlog_waiter: Arc<BacklogWaiter>,
    ) -> Self {
        Self {
            queue,
            ledger,
            unchecked: unchecked_map,
            process_stats: Arc::new(BlockBatchProcessorStats::default()),
            thread: Mutex::new(None),
            backlog_waiter,
        }
    }

    pub fn start(&self) {
        debug_assert!(self.thread.lock().unwrap().is_none());
        let mut processor_loop = self.create_loop();

        *self.thread.lock().unwrap() = Some(
            std::thread::Builder::new()
                .name("Blck processing".to_string())
                .spawn(move || {
                    processor_loop.run();
                })
                .unwrap(),
        );
    }

    fn create_loop(&self) -> BlockProcessorLoop {
        BlockProcessorLoop {
            queue: self.queue.clone(),
            process: self.create_block_batch_processor(),
            backlog_waiter: self.backlog_waiter.clone(),
        }
    }

    fn create_block_batch_processor(&self) -> BlockBatchProcessor {
        BlockBatchProcessor {
            ledger: self.ledger.clone(),
            unchecked: self.unchecked.clone(),
            stats: self.process_stats.clone(),
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
        self.stop();
    }
}

impl StatsSource for BlockProcessor {
    fn collect_stats(&self, result: &mut StatsCollection) {
        self.process_stats.collect_stats(result);
    }
}

struct BlockProcessorLoop {
    queue: Arc<BlockProcessorQueue>,
    process: BlockBatchProcessor,
    backlog_waiter: Arc<BacklogWaiter>,
}

impl BlockProcessorLoop {
    fn run(&mut self) {
        while let Some(blocks) = self.queue.pop_blocking() {
            self.backlog_waiter.wait_for_backlog();

            if self.queue.stopped() {
                break;
            }

            self.process.process_blocks(blocks);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_processing::BlockContext;

    #[test]
    fn wait_for_backlog() {
        let queue = Arc::new(BlockProcessorQueue::new_null_with(vec![
            BlockContext::new_test_instance().into(),
        ]));
        let process = BlockBatchProcessor::new_null();
        let backlog_waiter = Arc::new(BacklogWaiter::new_null());

        let mut processor_loop = BlockProcessorLoop {
            queue,
            process,
            backlog_waiter: backlog_waiter.clone(),
        };

        processor_loop.run();

        assert_eq!(backlog_waiter.call_count(), 1);
    }
}
