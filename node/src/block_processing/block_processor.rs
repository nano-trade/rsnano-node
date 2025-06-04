use std::{
    cmp::min,
    sync::{
        atomic::{AtomicU64, Ordering::Relaxed},
        Arc, Mutex,
    },
    thread::JoinHandle,
    time::Duration,
};

use tracing::warn;

use rsnano_ledger::Ledger;
use rsnano_nullable_clock::{SteadyClock, Timestamp};
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
    cooldown_count: Arc<AtomicU64>,
    clock: Arc<SteadyClock>,
}

impl BlockProcessor {
    pub(crate) fn new(
        queue: Arc<BlockProcessorQueue>,
        ledger: Arc<Ledger>,
        unchecked_map: Arc<UncheckedMap>,
        clock: Arc<SteadyClock>,
        max_backlog: u64,
    ) -> Self {
        Self {
            queue,
            ledger,
            unchecked: unchecked_map,
            process_stats: Arc::new(BlockBatchProcessorStats::default()),
            thread: Mutex::new(None),
            max_backlog,
            cooldown_count: Arc::new(AtomicU64::new(0)),
            clock,
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
            cooldown_count: self.cooldown_count.clone(),
            last_log: None,
            clock: self.clock.clone(),
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

        result.insert(
            "block_processor",
            "cooldown_backlog",
            self.cooldown_count.load(Relaxed),
        );
    }
}

struct BlockProcessorLoop {
    queue: Arc<BlockProcessorQueue>,
    process: BlockBatchProcessor,
    max_backlog: u64,
    ledger: Arc<Ledger>,
    cooldown_count: Arc<AtomicU64>,
    last_log: Option<Timestamp>,
    clock: Arc<SteadyClock>,
}

impl BlockProcessorLoop {
    fn run(&mut self) {
        while let Some(blocks) = self.queue.pop_blocking() {
            self.wait_for_bounded_backlog();

            if self.queue.stopped() {
                break;
            }

            self.process.process_blocks(blocks);
        }
    }

    fn wait_for_bounded_backlog(&mut self) {
        let backlog_count = self.ledger.backlog_count();
        let throttle_wait = throttle_wait(backlog_count, self.max_backlog);
        if throttle_wait.is_zero() {
            return;
        }

        let now = self.clock.now();
        if self.should_log(now) {
            warn!(
                throttle_ms = throttle_wait.as_millis(),
                backlog_size = backlog_count,
                "Backlog exceeded. Throttling!"
            );
            self.last_log = Some(now);
        }

        self.cooldown_count.fetch_add(1, Relaxed);
        self.queue.wait(throttle_wait);
    }

    fn should_log(&self, now: Timestamp) -> bool {
        match self.last_log {
            Some(i) => i.elapsed(now) >= Duration::from_secs(15),
            None => true,
        }
    }
}

fn throttle_wait(backlog_count: u64, max_backlog: u64) -> Duration {
    const BACKLOG_THROTTLE_MS: u64 = 100;
    const BACKLOG_THROTTLE_MAX_MS: u64 = 1000;

    let backlog_factor = backlog_factor(backlog_count, max_backlog);

    if backlog_factor < 1.0 {
        return Duration::ZERO;
    }

    // This uses a power of approximately 3.32, which gives ~1x at 1.0 and ~10x at 2.0
    let scaling = backlog_factor.powf(3.32);
    let throttle_wait_ms = min(
        (BACKLOG_THROTTLE_MS as f64 * scaling) as u64,
        BACKLOG_THROTTLE_MAX_MS,
    );

    Duration::from_millis(throttle_wait_ms)
}

fn backlog_factor(backlog_count: u64, max_backlog: u64) -> f64 {
    const BACKLOG_THRESHOLD: f64 = 1.5;

    if max_backlog == 0 || backlog_count <= max_backlog {
        return 0.0;
    }

    let max_with_threshold = max_backlog as f64 * BACKLOG_THRESHOLD;
    let factor = backlog_count as f64 / max_with_threshold;
    factor
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backlog_factor() {
        fn assert_factor(backlog_count: u64, max_backlog: u64, expected: f64) {
            let factor = backlog_factor(backlog_count, max_backlog);
            let delta = (expected - factor).abs();
            assert!(delta < 0.01, "expected: {}, actual: {}", expected, factor);
        }

        assert_factor(0, 0, 0.0);
        assert_factor(1000, 0, 0.0);
        assert_factor(0, 1000, 0.0);
        assert_factor(999, 1000, 0.0);
        assert_factor(1000, 1000, 0.0);
        assert_factor(1001, 1000, 0.667);
        assert_factor(1250, 1000, 0.833);
        assert_factor(1500, 1000, 1.0);
        assert_factor(2000, 1000, 1.333);
        assert_factor(3000, 1000, 2.0);
    }

    #[test]
    fn test_throttle_wait() {
        fn assert_throttle(backlog_count: u64, max_backlog: u64, expected_ms: u64) {
            let throttle = throttle_wait(backlog_count, max_backlog);
            assert_eq!(throttle, Duration::from_millis(expected_ms));
        }

        assert_throttle(0, 0, 0);
        assert_throttle(1000, 1000, 0);
        assert_throttle(1499, 1000, 0);
        assert_throttle(1500, 1000, 100);
        assert_throttle(2000, 1000, 259);
        assert_throttle(2500, 1000, 545);
        assert_throttle(3000, 1000, 998);
        assert_throttle(10000, 1000, 1000);
    }
}
