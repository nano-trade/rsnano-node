use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, RwLock},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use tracing::{debug, error};

use rsnano_core::{Block, BlockHash, BlockType, Epoch, Networks, SavedBlock, UncheckedInfo};
use rsnano_ledger::{BlockError, BlockSource, Ledger};
use rsnano_network::ChannelId;
use rsnano_stats::{DetailType, StatType, Stats, StatsCollection, StatsSource};
use rsnano_work::WorkThresholds;

use super::{
    process_queue::ProcessQueueConfig, BlockContext, BlockProcessorAction, BlockProcessorCallback,
    BlockProcessorQueue, RollbackRequest, UncheckedMap,
};

#[derive(Clone, Debug, PartialEq)]
pub struct BlockProcessorConfig {
    pub queue: ProcessQueueConfig,

    pub batch_max_time: Duration,
    pub full_size: usize,
    pub work_thresholds: WorkThresholds,
}

impl BlockProcessorConfig {
    pub const DEFAULT_BATCH_SIZE: usize = 0;
    pub const DEFAULT_FULL_SIZE: usize = 65536;

    pub fn new(work_thresholds: WorkThresholds) -> Self {
        Self {
            work_thresholds,
            queue: Default::default(),
            batch_max_time: Duration::from_millis(500),
            full_size: Self::DEFAULT_FULL_SIZE,
        }
    }

    pub fn new_for(network: Networks) -> Self {
        Self::new(WorkThresholds::default_for(network))
    }
}

pub struct BlockProcessor {
    thread: Mutex<Option<JoinHandle<()>>>,
    pub(crate) processor_loop: Arc<BlockProcessorLoop>,
}

impl BlockProcessor {
    pub(crate) fn new(
        queue: Arc<BlockProcessorQueue>,
        config: BlockProcessorConfig,
        ledger: Arc<Ledger>,
        unchecked_map: Arc<UncheckedMap>,
        stats: Arc<Stats>,
    ) -> Self {
        Self {
            processor_loop: Arc::new(BlockProcessorLoop {
                queue,
                ledger,
                unchecked: unchecked_map,
                config,
                stats,
                can_roll_back: RwLock::new(Box::new(|_| true)),
            }),
            thread: Mutex::new(None),
        }
    }

    pub fn new_test_instance(ledger: Arc<Ledger>) -> Self {
        BlockProcessor::new(
            Arc::new(BlockProcessorQueue::default()),
            BlockProcessorConfig::new_for(Networks::NanoDevNetwork),
            ledger,
            Arc::new(UncheckedMap::default()),
            Arc::new(Stats::default()),
        )
    }

    pub fn new_null() -> Self {
        Self::new_test_instance(Arc::new(Ledger::new_null()))
    }

    // Give other components a chance to veto a rollback
    pub fn on_rolling_back(&self, f: impl Fn(&BlockHash) -> bool + Send + Sync + 'static) {
        *self.processor_loop.can_roll_back.write().unwrap() = Box::new(f);
    }

    pub fn start(&self) {
        debug_assert!(self.thread.lock().unwrap().is_none());
        let processor_loop = Arc::clone(&self.processor_loop);
        *self.thread.lock().unwrap() = Some(
            std::thread::Builder::new()
                .name("Blck processing".to_string())
                .spawn(move || {
                    processor_loop.run();
                })
                .unwrap(),
        );
    }

    pub fn stop(&self) {
        self.processor_loop.queue.stop();
        let join_handle = self.thread.lock().unwrap().take();
        if let Some(join_handle) = join_handle {
            join_handle.join().unwrap();
        }
    }

    pub fn add(&self, block: Block, source: BlockSource, channel_id: ChannelId) -> bool {
        self.processor_loop.add(block, source, channel_id, None)
    }

    pub fn add_blocking(
        &self,
        block: Arc<Block>,
        source: BlockSource,
    ) -> anyhow::Result<Result<SavedBlock, BlockError>> {
        self.processor_loop.add_blocking(block, source)
    }

    pub fn process_active(&self, block: Block) {
        self.processor_loop.process_active(block);
    }

    pub fn force(&self, block: Block) {
        self.processor_loop.force(block);
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

pub(crate) struct BlockProcessorLoop {
    queue: Arc<BlockProcessorQueue>,
    ledger: Arc<Ledger>,
    unchecked: Arc<UncheckedMap>,
    config: BlockProcessorConfig,
    stats: Arc<Stats>,
    can_roll_back: RwLock<Box<dyn Fn(&BlockHash) -> bool + Send + Sync>>,
}

impl BlockProcessorLoop {
    fn run(&self) {
        while let Some(action) = self.queue.pop_blocking() {
            self.process(action);
        }
    }

    fn process(&self, action: BlockProcessorAction) {
        match action {
            BlockProcessorAction::RollBack(request) => {
                self.process_rollback(request);
            }
            BlockProcessorAction::Process(batch) => {
                self.process_batch(batch);
            }
        }
    }

    pub fn process_active(&self, block: Block) {
        self.add(block, BlockSource::Live, ChannelId::LOOPBACK, None);
    }

    pub fn add(
        &self,
        block: Block,
        source: BlockSource,
        channel_id: ChannelId,
        callback: Option<BlockProcessorCallback>,
    ) -> bool {
        if !self.config.work_thresholds.validate_entry_block(&block) {
            self.stats
                .inc(StatType::BlockProcessor, DetailType::InsufficientWork);
            return false; // Not added
        }

        let context = Arc::new(BlockContext::new(block, source, callback));
        self.queue.push(context, channel_id)
    }

    pub fn add_blocking(
        &self,
        block: Arc<Block>,
        source: BlockSource,
    ) -> anyhow::Result<Result<SavedBlock, BlockError>> {
        self.stats
            .inc(StatType::BlockProcessor, DetailType::ProcessBlocking);
        debug!(
            "Processing block (blocking): {} (source: {:?})",
            block.hash(),
            source
        );

        let hash = block.hash();
        let ctx = Arc::new(BlockContext::new(block.as_ref().clone(), source, None));
        let waiter = ctx.get_waiter();
        self.queue.push(ctx.clone(), ChannelId::LOOPBACK);

        match waiter.wait_result() {
            Some(Ok(())) => Ok(Ok(ctx.saved_block.lock().unwrap().clone().unwrap())),
            Some(Err(e)) => Ok(Err(e)),
            None => {
                self.stats
                    .inc(StatType::BlockProcessor, DetailType::ProcessBlockingTimeout);
                error!("Block dropped when processing: {}", hash);
                Err(anyhow!("Block dropped when processing"))
            }
        }
    }

    pub fn force(&self, block: Block) {
        self.stats.inc(StatType::BlockProcessor, DetailType::Force);
        debug!("Forcing block: {}", block.hash());
        let ctx = Arc::new(BlockContext::new(block, BlockSource::Forced, None));
        self.queue.push(ctx, ChannelId::LOOPBACK);
    }

    fn process_rollback(&self, request: RollbackRequest) {
        let can_roll_back = self.can_roll_back.read().unwrap();
        let mut results =
            self.ledger
                .roll_back_batch(&request.targets, request.max_rollbacks, &*can_roll_back);

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

    fn process_batch(&self, mut batch: VecDeque<Arc<BlockContext>>) {
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
