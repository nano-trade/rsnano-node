use std::{
    collections::VecDeque,
    mem::size_of,
    sync::{Arc, Condvar, Mutex, MutexGuard, RwLock},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use strum::IntoEnumIterator;
use tracing::{debug, error, info};

use rsnano_core::{
    utils::{ContainerInfo, ContainerInfoProvider, FairQueue, FairQueueInfo},
    Block, BlockHash, BlockType, Epoch, Networks, SavedBlock, UncheckedInfo,
};
use rsnano_ledger::{BlockError, BlockSource, Ledger, LedgerSet};
use rsnano_network::{ChannelId, DeadChannelCleanupStep};
use rsnano_stats::{DetailType, StatType, Stats};
use rsnano_work::WorkThresholds;

use super::{BlockContext, BlockProcessorCallback, UncheckedMap};

#[derive(Clone, Debug, PartialEq)]
pub struct BlockProcessorConfig {
    // Maximum number of blocks to queue from network peers
    pub max_peer_queue: usize,
    //
    // Maximum number of blocks to queue from system components (local RPC, bootstrap)
    pub max_system_queue: usize,

    // Higher priority gets processed more frequently
    pub priority_live: usize,
    pub priority_bootstrap: usize,
    pub priority_local: usize,
    pub priority_system: usize,
    pub batch_max_time: Duration,
    pub full_size: usize,
    pub batch_size: usize,
    pub work_thresholds: WorkThresholds,
}

impl BlockProcessorConfig {
    pub const DEFAULT_BATCH_SIZE: usize = 0;
    pub const DEFAULT_FULL_SIZE: usize = 65536;

    pub fn new(work_thresholds: WorkThresholds) -> Self {
        Self {
            work_thresholds,
            max_peer_queue: 128,
            max_system_queue: 16 * 1024,
            priority_live: 1,
            priority_bootstrap: 8,
            priority_local: 16,
            priority_system: 32,
            batch_max_time: Duration::from_millis(500),
            full_size: Self::DEFAULT_FULL_SIZE,
            batch_size: 256,
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
        config: BlockProcessorConfig,
        ledger: Arc<Ledger>,
        unchecked_map: Arc<UncheckedMap>,
        stats: Arc<Stats>,
    ) -> Self {
        let config_l = config.clone();
        let max_size_query = move |origin: &(BlockSource, ChannelId)| match origin.0 {
            BlockSource::Live | BlockSource::LiveOriginator => config_l.max_peer_queue,
            _ => config_l.max_system_queue,
        };

        let config_l = config.clone();
        let priority_query = move |origin: &(BlockSource, ChannelId)| match origin.0 {
            BlockSource::Live | BlockSource::LiveOriginator => config.priority_live,
            BlockSource::Bootstrap | BlockSource::BootstrapLegacy | BlockSource::Unchecked => {
                config_l.priority_bootstrap
            }
            BlockSource::Local => config_l.priority_local,
            BlockSource::Election | BlockSource::Forced | BlockSource::Unknown => {
                config.priority_system
            }
        };

        Self {
            processor_loop: Arc::new(BlockProcessorLoop {
                mutex: Mutex::new(BlockProcessorImpl {
                    add_queue: FairQueue::new(max_size_query, priority_query),
                    rollback_queue: VecDeque::new(),
                    last_log: None,
                    stopped: false,
                    cool_down: false,
                }),
                condition: Condvar::new(),
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
        self.processor_loop.mutex.lock().unwrap().stopped = true;
        self.processor_loop.condition.notify_all();
        let join_handle = self.thread.lock().unwrap().take();
        if let Some(join_handle) = join_handle {
            join_handle.join().unwrap();
        }
        let mut guard = self.processor_loop.mutex.lock().unwrap();
        for req in guard.rollback_queue.drain(..) {
            *req.result.rolled_back.lock().unwrap() = Some(Vec::new());
            req.result.done.notify_all();
        }
    }

    pub fn set_cooldown(&self, cool_down: bool) {
        self.processor_loop.mutex.lock().unwrap().cool_down = cool_down;
    }

    pub fn total_queue_len(&self) -> usize {
        self.processor_loop.total_queue_len()
    }

    pub fn queue_len(&self, source: BlockSource) -> usize {
        self.processor_loop.queue_len(source)
    }

    pub fn add(&self, block: Block, source: BlockSource, channel_id: ChannelId) -> bool {
        self.processor_loop.add(block, source, channel_id, None)
    }

    pub fn add_with_callback(
        &self,
        block: Block,
        source: BlockSource,
        channel_id: ChannelId,
        callback: BlockProcessorCallback,
    ) -> bool {
        self.processor_loop
            .add(block, source, channel_id, Some(callback))
    }

    pub fn add_blocking(
        &self,
        block: Arc<Block>,
        source: BlockSource,
    ) -> anyhow::Result<Result<SavedBlock, BlockError>> {
        self.processor_loop.add_blocking(block, source)
    }

    pub fn roll_back_blocking(
        &self,
        targets: Vec<BlockHash>,
        max_rollbacks: usize,
    ) -> Vec<BlockHash> {
        self.processor_loop
            .roll_back_blocking(targets, max_rollbacks)
    }

    pub fn process_active(&self, block: Block) {
        self.processor_loop.process_active(block);
    }

    pub fn force(&self, block: Block) {
        self.processor_loop.force(block);
    }

    pub fn is_cooling_down(&self) -> bool {
        self.processor_loop.mutex.lock().unwrap().cool_down
    }

    pub fn reprocess_election_winner(&self, winner: &Block) {
        // In some edge cases block might get rolled back while the election
        // is confirming, reprocess it to ensure it's present in the ledger
        if !self
            .processor_loop
            .ledger
            .any()
            .block_exists(&winner.hash())
        {
            self.add(
                winner.clone().into(),
                BlockSource::Election,
                ChannelId::LOOPBACK,
            );
        }
    }

    pub fn info(&self) -> FairQueueInfo<BlockSource> {
        self.processor_loop.info()
    }
}

impl Drop for BlockProcessor {
    fn drop(&mut self) {
        // Thread must be stopped before destruction
        debug_assert!(self.thread.lock().unwrap().is_none());
    }
}

impl ContainerInfoProvider for BlockProcessor {
    fn container_info(&self) -> ContainerInfo {
        self.processor_loop.container_info()
    }
}

pub(crate) struct BlockProcessorLoop {
    mutex: Mutex<BlockProcessorImpl>,
    condition: Condvar,
    ledger: Arc<Ledger>,
    unchecked: Arc<UncheckedMap>,
    config: BlockProcessorConfig,
    stats: Arc<Stats>,
    can_roll_back: RwLock<Box<dyn Fn(&BlockHash) -> bool + Send + Sync>>,
}

impl BlockProcessorLoop {
    fn run(&self) {
        let mut guard = self.mutex.lock().unwrap();
        while !guard.stopped {
            if !guard.add_queue.is_empty() || !guard.rollback_queue.is_empty() {
                while guard.cool_down && !guard.stopped {
                    drop(guard);
                    // It's possible that ledger processing happens faster than the
                    // notifications can be processed by other components, cooldown here
                    self.stats
                        .inc(StatType::BlockProcessor, DetailType::Cooldown);
                    std::thread::sleep(Duration::from_millis(50));
                    guard = self.mutex.lock().unwrap();
                }
                if guard.stopped {
                    return;
                }
            }

            if !guard.rollback_queue.is_empty() {
                let request = guard.rollback_queue.pop_front().unwrap();
                drop(guard);
                self.process_rollback(request);
                guard = self.mutex.lock().unwrap();
            } else if !guard.add_queue.is_empty() {
                if guard.should_log() {
                    info!(
                        "{} blocks (+ {} forced) in processing_queue",
                        guard.add_queue.len(),
                        guard
                            .add_queue
                            .queue_len(&(BlockSource::Forced, ChannelId::LOOPBACK))
                    );
                }

                self.process_batch(guard);

                guard = self.mutex.lock().unwrap();
            } else {
                guard = self
                    .condition
                    .wait_while(guard, |i| {
                        !i.stopped && i.add_queue.is_empty() && i.rollback_queue.is_empty()
                    })
                    .unwrap();
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

        self.stats
            .inc(StatType::BlockProcessor, DetailType::Process);
        debug!(
            "Processing block (async): {} (source: {:?} channel id: {})",
            block.hash(),
            source,
            channel_id
        );

        self.add_impl(
            Arc::new(BlockContext::new(block, source, callback)),
            channel_id,
        )
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
        self.add_impl(ctx.clone(), ChannelId::LOOPBACK);

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

    fn roll_back_blocking(&self, targets: Vec<BlockHash>, max_rollbacks: usize) -> Vec<BlockHash> {
        let result = Arc::new(RollbackResult::new());
        {
            let mut guard = self.mutex.lock().unwrap();
            if guard.stopped {
                return Vec::new();
            }

            let request = RollbackRequest {
                targets,
                max_rollbacks,
                result: result.clone(),
            };
            guard.rollback_queue.push_back(request);
        }
        self.condition.notify_all();

        let mut guard = result.rolled_back.lock().unwrap();
        guard = result.done.wait_while(guard, |i| i.is_none()).unwrap();
        guard.take().unwrap()
    }

    pub fn force(&self, block: Block) {
        self.stats.inc(StatType::BlockProcessor, DetailType::Force);
        debug!("Forcing block: {}", block.hash());
        let ctx = Arc::new(BlockContext::new(block, BlockSource::Forced, None));
        self.add_impl(ctx, ChannelId::LOOPBACK);
    }

    // TODO: Remove and replace all checks with calls to size (block_source)
    pub fn total_queue_len(&self) -> usize {
        self.mutex.lock().unwrap().add_queue.len()
    }

    pub fn queue_len(&self, source: BlockSource) -> usize {
        self.mutex
            .lock()
            .unwrap()
            .add_queue
            .sum_queue_len((source, ChannelId::MIN)..=(source, ChannelId::MAX))
    }

    fn add_impl(&self, context: Arc<BlockContext>, channel_id: ChannelId) -> bool {
        let source = context.source;
        let added;
        {
            let mut guard = self.mutex.lock().unwrap();
            added = guard.add_queue.push((source, channel_id), context);
        }
        if added {
            self.condition.notify_all();
        } else {
            self.stats
                .inc(StatType::BlockProcessor, DetailType::Overfill);
            self.stats
                .inc(StatType::BlockProcessorOverfill, source.into());
        }
        added
    }

    fn next_batch(
        &self,
        data: &mut BlockProcessorImpl,
        max_count: usize,
    ) -> VecDeque<Arc<BlockContext>> {
        let mut results = VecDeque::new();
        while !data.add_queue.is_empty() && results.len() < max_count {
            results.push_back(data.next());
        }
        results
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

    fn process_batch(&self, mut guard: MutexGuard<BlockProcessorImpl>) {
        let mut batch = self.next_batch(&mut guard, self.config.batch_size);
        drop(guard);
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

    pub fn info(&self) -> FairQueueInfo<BlockSource> {
        self.mutex.lock().unwrap().info()
    }

    pub fn container_info(&self) -> ContainerInfo {
        let guard = self.mutex.lock().unwrap();
        ContainerInfo::builder()
            .leaf("blocks", guard.add_queue.len(), size_of::<Arc<Block>>())
            .leaf(
                "forced",
                guard
                    .add_queue
                    .queue_len(&(BlockSource::Forced, ChannelId::LOOPBACK)),
                size_of::<Arc<Block>>(),
            )
            .node("queue", guard.add_queue.container_info())
            .finish()
    }
}

struct BlockProcessorImpl {
    add_queue: FairQueue<(BlockSource, ChannelId), Arc<BlockContext>>,
    rollback_queue: VecDeque<RollbackRequest>,
    last_log: Option<Instant>,
    stopped: bool,
    cool_down: bool,
}

impl BlockProcessorImpl {
    fn next(&mut self) -> Arc<BlockContext> {
        debug_assert!(!self.add_queue.is_empty()); // This should be checked before calling next
        if !self.add_queue.is_empty() {
            let ((source, _), request) = self.add_queue.next().unwrap();
            assert!(source != BlockSource::Forced || request.source == BlockSource::Forced);
            return request;
        }

        panic!("next() called when no blocks are ready");
    }

    pub fn should_log(&mut self) -> bool {
        if let Some(last) = &self.last_log {
            if last.elapsed() >= Duration::from_secs(15) {
                self.last_log = Some(Instant::now());
                return true;
            }
        }

        false
    }

    pub fn info(&self) -> FairQueueInfo<BlockSource> {
        self.add_queue.compacted_info(|(source, _)| *source)
    }
}

pub(crate) struct BlockProcessorCleanup(Arc<BlockProcessorLoop>);

impl BlockProcessorCleanup {
    pub fn new(processor_loop: Arc<BlockProcessorLoop>) -> Self {
        Self(processor_loop)
    }
}

impl DeadChannelCleanupStep for BlockProcessorCleanup {
    fn clean_up_dead_channels(&self, dead_channel_ids: &[ChannelId]) {
        let mut guard = self.0.mutex.lock().unwrap();
        for channel_id in dead_channel_ids {
            let iter = BlockSource::iter();
            for source in iter {
                guard.add_queue.remove(&(source, *channel_id))
            }
        }
    }
}

pub struct RollbackRequest {
    targets: Vec<BlockHash>,
    max_rollbacks: usize,
    result: Arc<RollbackResult>,
}

struct RollbackResult {
    rolled_back: Mutex<Option<Vec<BlockHash>>>,
    done: Condvar,
}

impl RollbackResult {
    fn new() -> Self {
        Self {
            rolled_back: Mutex::new(None),
            done: Condvar::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_stats::Direction;

    #[test]
    fn insufficient_work() {
        let config = BlockProcessorConfig::new(WorkThresholds::new_stub());
        let ledger = Arc::new(Ledger::new_null());
        let unchecked = Arc::new(UncheckedMap::default());
        let stats = Arc::new(Stats::default());
        let block_processor = BlockProcessor::new(config, ledger, unchecked, stats.clone());

        let mut block = Block::new_test_instance();
        block.set_work(3.into());

        block_processor.add(block, BlockSource::Live, ChannelId::LOOPBACK);

        assert_eq!(
            stats.count(
                StatType::BlockProcessor,
                DetailType::InsufficientWork,
                Direction::In
            ),
            1
        );

        assert_eq!(block_processor.total_queue_len(), 0);
    }
}
