use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};

use tracing::debug;

use rsnano_core::{BlockType, Epoch, UncheckedInfo};
use rsnano_ledger::{BlockError, Ledger};
use rsnano_stats::{DetailType, StatType, Stats};

use super::{BlockContext, UncheckedMap};

pub(crate) struct BlockBatchProcessor {
    pub ledger: Arc<Ledger>,
    pub unchecked: Arc<UncheckedMap>,
    pub stats: Arc<Stats>,
}

impl BlockBatchProcessor {
    pub(crate) fn process_blocks(&self, mut batch: VecDeque<Arc<BlockContext>>) {
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
