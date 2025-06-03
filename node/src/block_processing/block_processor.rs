use std::{
    cmp::min,
    sync::{Arc, Mutex},
    thread::JoinHandle,
    time::Duration,
};

use rsnano_ledger::Ledger;
use rsnano_stats::{StatsCollection, StatsSource};

use super::{block_batch_processor::BlockBatchProcessorStats, BlockProcessorQueue, UncheckedMap};
use crate::block_processing::block_batch_processor::BlockBatchProcessor;

pub struct BlockProcessor {
    thread: Mutex<Option<JoinHandle<()>>>,
    queue: Arc<BlockProcessorQueue>,
    ledger: Arc<Ledger>,
    unchecked: Arc<UncheckedMap>,
    process_stats: Arc<BlockBatchProcessorStats>,
    max_backlog: u64,
}

impl BlockProcessor {
    pub(crate) fn new(
        queue: Arc<BlockProcessorQueue>,
        ledger: Arc<Ledger>,
        unchecked_map: Arc<UncheckedMap>,
        max_backlog: u64,
    ) -> Self {
        Self {
            queue,
            ledger,
            unchecked: unchecked_map,
            process_stats: Arc::new(BlockBatchProcessorStats::default()),
            thread: Mutex::new(None),
            max_backlog,
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
            ledger: self.ledger.clone(),
            max_backlog: self.max_backlog,
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
    max_backlog: u64,
    ledger: Arc<Ledger>,
}

impl BlockProcessorLoop {
    const BACKLOG_THRESHOLD: f64 = 1.5;

    fn run(&mut self) {
        while let Some(blocks) = self.queue.pop_blocking() {
            self.wait_for_bounded_backlog();

            if self.queue.stopped() {
                break;
            }

            self.process.process_blocks(blocks);
        }
    }

    fn wait_for_bounded_backlog(&self) {
        let backlog_factor = self.backlog_factor();

        if backlog_factor < 1.0 {
            return;
        }

        let throttle_wait = Duration::from_secs(1); // TODO use formula from nano_node

        // TODO logging

        self.queue.wait(throttle_wait);
    }

    fn backlog_factor(&self) -> f64 {
        let backlog_count = self.ledger.backlog_count();
        if self.max_backlog == 0 || backlog_count <= self.max_backlog {
            return 0.0;
        }

        let max_with_threshold = self.max_backlog as f64 * Self::BACKLOG_THRESHOLD;
        let factor = backlog_count as f64 / max_with_threshold;
        factor
    }
}
