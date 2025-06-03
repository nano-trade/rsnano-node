use std::{
    sync::{Arc, Mutex},
    thread::JoinHandle,
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
            thread: Mutex::new(None),
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
}

impl BlockProcessorLoop {
    fn run(&mut self) {
        while let Some(blocks) = self.queue.pop_blocking() {
            self.process.process_blocks(blocks);
        }
    }
}
