use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use tracing::debug;

use rsnano_core::{BlockHash, BlockType, Epoch, UncheckedInfo};
use rsnano_ledger::{BlockError, Ledger};
use rsnano_stats::{DetailType, StatType, Stats, StatsCollection, StatsSource};

use super::{
    BlockContext, BlockProcessorAction, BlockProcessorQueue, RollbackRequest, UncheckedMap,
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

struct BlockBatchRollback {
    ledger: Arc<Ledger>,
    can_roll_back: Box<dyn Fn(&BlockHash) -> bool + Send + Sync>,
}

impl BlockBatchRollback {
    fn roll_back(&mut self, request: RollbackRequest) {
        let mut results = self.ledger.roll_back_batch(
            &request.targets,
            request.max_rollbacks,
            &self.can_roll_back,
        );

        let mut processed_hashes = Vec::new();
        for result in results.drain(..) {
            if !result.rolled_back.is_empty() {
                for h in &result.rolled_back {
                    processed_hashes.push(h.hash());
                }
            } else {
                processed_hashes.push(result.target_hash);
            }
        }

        *request.result.rolled_back.lock().unwrap() = Some(processed_hashes);
        request.result.done.notify_all();
    }
}

struct BlockBatchProcessor {
    ledger: Arc<Ledger>,
    unchecked: Arc<UncheckedMap>,
    stats: Arc<Stats>,
}

impl BlockBatchProcessor {
    fn process_blocks(&self, mut batch: VecDeque<Arc<BlockContext>>) {
        let timer = Instant::now();

        let mut result = self
            .ledger
            .process_batch(batch.iter().map(|c| (&c.block, c.source)));

        if result.processed.len() > 0 && timer.elapsed() > Duration::from_millis(100) {
            debug!(
                "Processed {} blocks in {} ms",
                result.processed.len(),
                timer.elapsed().as_millis(),
            );
        }

        assert_eq!(result.processed.len(), batch.len());
        let mut result: Vec<(Result<(), BlockError>, Arc<BlockContext>)> = result
            .processed
            .drain(..)
            .zip(batch.drain(..))
            .map(|((status, saved_block), block_ctx)| {
                if saved_block.is_some() {
                    *block_ctx.saved_block.lock().unwrap() = saved_block;
                }

                (status, block_ctx)
            })
            .collect();

        for (status, block_ctx) in &result {
            match status {
                Ok(()) => {
                    self.stats
                        .inc(StatType::BlockProcessorResult, DetailType::Progress);
                }
                Err(e) => {
                    self.stats.inc(StatType::BlockProcessorResult, (*e).into());
                }
            }

            self.stats
                .inc(StatType::BlockProcessorSource, block_ctx.source.into());

            let hash = &block_ctx.block.hash();
            let block = &block_ctx.block;
            let saved_block = block_ctx.saved_block.lock().unwrap().clone();

            match status {
                Ok(()) => {
                    self.unchecked.trigger(&hash.into());

                    /*
                     * For send blocks check epoch open unchecked (gap pending).
                     * For state blocks check only send subtype and only if block epoch is not last epoch.
                     * If epoch is last, then pending entry shouldn't trigger same epoch open block for destination account.
                     * */
                    let block = saved_block.unwrap();
                    if block.block_type() == BlockType::LegacySend
                        || block.block_type() == BlockType::State
                            && block.is_send()
                            && block.epoch() < Epoch::MAX
                    {
                        self.unchecked.trigger(&block.destination_or_link().into());
                    }
                }
                Err(BlockError::GapPrevious) => {
                    self.unchecked
                        .put(block.previous().into(), UncheckedInfo::new(block.clone()));
                    self.stats.inc(StatType::Ledger, DetailType::GapPrevious);
                }
                Err(BlockError::GapSource) => {
                    self.unchecked.put(
                        block
                            .source_field()
                            .unwrap_or(block.link_field().unwrap_or_default().into())
                            .into(),
                        UncheckedInfo::new(block.clone()),
                    );
                    self.stats.inc(StatType::Ledger, DetailType::GapSource);
                }
                Err(BlockError::GapEpochOpenPending) => {
                    // Specific unchecked key starting with epoch open block account public key
                    self.unchecked.put(
                        block.account_field().unwrap().into(),
                        UncheckedInfo::new(block.clone()),
                    );
                    self.stats.inc(StatType::Ledger, DetailType::GapSource);
                }
                Err(BlockError::Old) => {
                    self.stats.inc(StatType::Ledger, DetailType::Old);
                }
                // These are unexpected and indicate erroneous/malicious behavior, log debug info to highlight the issue
                Err(BlockError::BadSignature) => {
                    debug!("Block signature is invalid: {}", hash)
                }
                Err(BlockError::NegativeSpend) => {
                    debug!("Block spends negative amount: {}", hash)
                }
                Err(BlockError::Unreceivable) => {
                    debug!("Block is unreceivable: {}", hash)
                }
                Err(BlockError::Fork) => {
                    self.stats.inc(StatType::Ledger, DetailType::Fork);
                    debug!("Block is a fork: {}", hash)
                }
                Err(BlockError::OpenedBurnAccount) => {
                    debug!("Block opens burn account: {}", hash)
                }
                Err(BlockError::BalanceMismatch) => {
                    debug!("Block balance mismatch: {}", hash)
                }
                Err(BlockError::RepresentativeMismatch) => {
                    debug!("Block representative mismatch: {}", hash)
                }
                Err(BlockError::BlockPosition) => {
                    debug!("Block is in incorrect position: {}", hash)
                }
                Err(BlockError::InsufficientWork) => {
                    debug!("Block has insufficient work: {}", hash)
                }
            }
        }

        // Set results for futures when not holding the lock
        for (res, context) in result.iter_mut() {
            if let Some(cb) = &context.callback {
                cb(*res);
            }
            context.set_result(*res);
        }
    }
}
