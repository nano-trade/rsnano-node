use std::{
    collections::{HashSet, VecDeque},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use rsnano_core::{utils::ContainerInfo, BlockHash, SavedBlock};
use rsnano_ledger::{BlockStatus, CementingObserver, Ledger};
use rsnano_stats::{DetailType, StatType, Stats};

use super::ordered_entries::{Entry, OrderedEntries};
use crate::{
    block_processing::BlockContext,
    consensus::ElectionData,
    utils::{ThreadPool, ThreadPoolImpl},
};

#[derive(Clone, Debug, PartialEq)]
pub struct ConfirmingSetConfig {
    pub batch_size: usize,
    /// Maximum number of dependent blocks to be stored in memory during processing
    pub max_blocks: usize,
    pub max_queued_notifications: usize,

    /// Maximum number of failed blocks to wait for requeuing
    pub max_deferred: usize,
    /// Max age of deferred blocks before they are dropped
    pub deferred_age_cutoff: Duration,
}

impl Default for ConfirmingSetConfig {
    fn default() -> Self {
        Self {
            batch_size: 256,
            max_blocks: 128 * 128,
            max_queued_notifications: 8,
            max_deferred: 16 * 1024,
            deferred_age_cutoff: Duration::from_secs(15 * 60),
        }
    }
}

/// Set of blocks to be durably confirmed
pub struct ConfirmingSet {
    thread: Arc<ConfirmingSetThread>,
    join_handle: Mutex<Option<JoinHandle<()>>>,
}

impl ConfirmingSet {
    pub fn new(config: ConfirmingSetConfig, ledger: Arc<Ledger>, stats: Arc<Stats>) -> Self {
        Self {
            join_handle: Mutex::new(None),
            thread: Arc::new(ConfirmingSetThread {
                mutex: Mutex::new(ConfirmingSetImpl {
                    set: OrderedEntries::default(),
                    deferred: OrderedEntries::default(),
                    current: HashSet::new(),
                    stats: stats.clone(),
                    config: config.clone(),
                }),
                stopped: AtomicBool::new(false),
                condition: Condvar::new(),
                ledger,
                stats,
                config,
                observers: Arc::new(Mutex::new(Observers::default())),
                workers: ThreadPoolImpl::create(1, "Conf notif"),
            }),
        }
    }

    pub(crate) fn on_batch_cemented(&self, callback: BatchCementedCallback) {
        self.thread
            .observers
            .lock()
            .unwrap()
            .batch_cemented
            .push(callback);
    }

    pub fn on_cemented(&self, callback: BlockCallback) {
        self.thread
            .observers
            .lock()
            .unwrap()
            .cemented
            .push(callback);
    }

    pub fn on_already_cemented(&self, callback: AlreadyCementedCallback) {
        self.thread
            .observers
            .lock()
            .unwrap()
            .already_cemented
            .push(callback);
    }

    pub fn on_cementing_failed(&self, callback: impl FnMut(&BlockHash) + Send + 'static) {
        self.thread
            .observers
            .lock()
            .unwrap()
            .cementing_failed
            .push(Box::new(callback));
    }

    /// Adds a block to the set of blocks to be confirmed
    pub fn add(&self, hash: BlockHash) {
        self.add_with_election(hash, None)
    }

    pub fn add_with_election(&self, hash: BlockHash, election: Option<Arc<Mutex<ElectionData>>>) {
        self.thread.add(hash, election);
    }

    pub fn start(&self) {
        debug_assert!(self.join_handle.lock().unwrap().is_none());

        let thread = Arc::clone(&self.thread);
        *self.join_handle.lock().unwrap() = Some(
            std::thread::Builder::new()
                .name("Conf height".to_string())
                .spawn(move || thread.run())
                .unwrap(),
        );
    }

    pub fn stop(&self) {
        self.thread.stop();
        let handle = self.join_handle.lock().unwrap().take();
        if let Some(handle) = handle {
            handle.join().unwrap();
        }
        self.thread.workers.stop();
    }

    /// Added blocks will remain in this set until after ledger has them marked as confirmed.
    pub fn contains(&self, hash: &BlockHash) -> bool {
        self.thread.contains(hash)
    }

    pub fn len(&self) -> usize {
        self.thread.len()
    }

    pub fn info(&self) -> ConfirmingSetInfo {
        let guard = self.thread.mutex.lock().unwrap();
        ConfirmingSetInfo {
            size: guard.set.len(),
            max_size: self.thread.config.max_blocks,
        }
    }

    /// Requeue blocks that failed to cement immediately due to missing ledger blocks
    pub fn requeue_blocks(&self, batch: &[(BlockStatus, Arc<BlockContext>)]) {
        let mut should_notify = false;
        {
            let mut guard = self.thread.mutex.lock().unwrap();
            for (_, context) in batch {
                if let Some(entry) = guard.deferred.remove(&context.block.hash()) {
                    self.thread
                        .stats
                        .inc(StatType::ConfirmingSet, DetailType::Requeued);
                    guard.set.push_back(entry);
                    should_notify = true;
                }
            }
        }

        if should_notify {
            self.thread.condition.notify_all();
        }
    }

    pub fn container_info(&self) -> ContainerInfo {
        let guard = self.thread.mutex.lock().unwrap();
        [
            ("set", guard.set.len(), 0),
            ("deferred", guard.deferred.len(), 0),
        ]
        .into()
    }
}

#[derive(Default)]
pub struct ConfirmingSetInfo {
    pub size: usize,
    pub max_size: usize,
}

impl Drop for ConfirmingSet {
    fn drop(&mut self) {
        self.stop();
    }
}

struct ConfirmingSetThread {
    mutex: Mutex<ConfirmingSetImpl>,
    stopped: AtomicBool,
    condition: Condvar,
    ledger: Arc<Ledger>,
    stats: Arc<Stats>,
    config: ConfirmingSetConfig,
    workers: ThreadPoolImpl,
    observers: Arc<Mutex<Observers>>,
}

impl ConfirmingSetThread {
    fn stop(&self) {
        {
            let _guard = self.mutex.lock().unwrap();
            self.stopped.store(true, Ordering::SeqCst);
        }
        self.condition.notify_all();
    }

    fn add(&self, hash: BlockHash, election: Option<Arc<Mutex<ElectionData>>>) {
        let added = {
            let mut guard = self.mutex.lock().unwrap();
            guard.set.push_back(Entry {
                hash,
                election,
                timestamp: Instant::now(),
            })
        };

        if added {
            self.condition.notify_all();
            self.stats.inc(StatType::ConfirmingSet, DetailType::Insert);
        } else {
            self.stats
                .inc(StatType::ConfirmingSet, DetailType::Duplicate);
        }
    }

    fn contains(&self, hash: &BlockHash) -> bool {
        let guard = self.mutex.lock().unwrap();
        guard.set.contains(hash) || guard.deferred.contains(hash) || guard.current.contains(hash)
    }

    fn len(&self) -> usize {
        // Do not report deferred blocks, as they are not currently being processed (and might never be requeued)
        let guard = self.mutex.lock().unwrap();
        guard.set.len() + guard.current.len()
    }

    fn run(&self) {
        let mut guard = self.mutex.lock().unwrap();
        while !self.stopped.load(Ordering::SeqCst) {
            self.stats.inc(StatType::ConfirmingSet, DetailType::Loop);
            let evicted = guard.cleanup();

            // Notify about evicted blocks so that other components can perform necessary cleanup
            if !evicted.is_empty() {
                drop(guard);
                {
                    let mut observers = self.observers.lock().unwrap();
                    for entry in evicted {
                        observers.notify_cementing_failed(&entry.hash);
                    }
                }
                guard = self.mutex.lock().unwrap();
            }

            if !guard.set.is_empty() {
                let batch = guard.next_batch(self.config.batch_size);

                // Keep track of the blocks we're currently cementing, so that the .contains (...) check is accurate
                debug_assert!(guard.current.is_empty());
                for entry in &batch {
                    guard.current.insert(entry.hash);
                }

                drop(guard);

                self.run_batch(batch);
                guard = self.mutex.lock().unwrap();
            } else {
                guard = self
                    .condition
                    .wait_while(guard, |i| {
                        i.set.is_empty() && !self.stopped.load(Ordering::SeqCst)
                    })
                    .unwrap();
            }
        }
    }

    fn notify(&self, cemented: &mut VecDeque<CementingContext>) {
        let mut batch = VecDeque::new();
        std::mem::swap(&mut batch, cemented);

        let mut guard = self.mutex.lock().unwrap();

        // It's possible that ledger cementing happens faster than the notifications can be processed by other components, cooldown here
        while self.workers.num_queued_tasks() >= self.config.max_queued_notifications {
            self.stats
                .inc(StatType::ConfirmingSet, DetailType::Cooldown);
            guard = self
                .condition
                .wait_timeout_while(guard, Duration::from_millis(100), |_| {
                    !self.stopped.load(Ordering::SeqCst)
                })
                .unwrap()
                .0;
            if self.stopped.load(Ordering::Relaxed) {
                return;
            }
        }

        let observers = self.observers.clone();
        let stats = self.stats.clone();
        self.workers.post(Box::new(move || {
            stats.inc(StatType::ConfirmingSet, DetailType::Notify);
            observers.lock().unwrap().notify_batch(batch);
        }));
    }

    fn run_batch(&self, mut batch: VecDeque<Entry>) {
        let mut notifier = CementedNotifier::new(self);
        let b = batch.drain(..).map(|i| (i.hash, i));
        self.ledger
            .confirm_batch(b, &self.stopped, self.config.max_blocks, &mut notifier);

        self.notify(&mut notifier.cemented);

        {
            let mut guard = self.observers.lock().unwrap();
            for callback in &mut guard.already_cemented {
                callback(&notifier.already_cemented)
            }
        }

        // Clear current set only after the transaction is committed
        self.mutex.lock().unwrap().current.clear();
    }
}

struct ConfirmingSetImpl {
    /// Blocks that are ready to be cemented
    set: OrderedEntries,
    /// Blocks that could not be cemented immediately (e.g. waiting for rollbacks to complete)
    deferred: OrderedEntries,
    /// Blocks that are being cemented in the current batch
    current: HashSet<BlockHash>,

    stats: Arc<Stats>,
    config: ConfirmingSetConfig,
}

impl ConfirmingSetImpl {
    fn next_batch(&mut self, max_count: usize) -> VecDeque<Entry> {
        let mut results = VecDeque::new();
        // TODO: use extract_if once it is stablized
        while let Some(entry) = self.set.pop_front() {
            if results.len() >= max_count {
                break;
            }
            results.push_back(entry);
        }
        results
    }

    fn cleanup(&mut self) -> Vec<Entry> {
        let mut evicted = Vec::new();

        let cutoff = Instant::now() - self.config.deferred_age_cutoff;
        let should_evict = |entry: &Entry| entry.timestamp < cutoff;

        // Iterate in sequenced (insertion) order
        loop {
            let Some(entry) = self.deferred.front() else {
                break;
            };

            if should_evict(entry) || self.deferred.len() > self.config.max_deferred {
                self.stats.inc(StatType::ConfirmingSet, DetailType::Evicted);
                let entry = self.deferred.pop_front().unwrap();
                evicted.push(entry);
            } else {
                // Entries are sequenced, so we can stop here and avoid unnecessary iteration
                break;
            }
        }
        evicted
    }
}

type BlockCallback = Box<dyn FnMut(&SavedBlock) + Send>;

/// block + confirmation root
type BatchCementedCallback = Box<dyn FnMut(&VecDeque<CementingContext>) + Send>;
type AlreadyCementedCallback = Box<dyn FnMut(&VecDeque<BlockHash>) + Send>;

#[derive(Default)]
struct Observers {
    cemented: Vec<BlockCallback>,
    batch_cemented: Vec<BatchCementedCallback>,
    already_cemented: Vec<AlreadyCementedCallback>,
    cementing_failed: Vec<Box<dyn FnMut(&BlockHash) + Send>>,
}

impl Observers {
    fn notify_batch(&mut self, cemented: VecDeque<CementingContext>) {
        for context in &cemented {
            for observer in &mut self.cemented {
                observer(&context.block);
            }
        }

        for observer in &mut self.batch_cemented {
            observer(&cemented);
        }
    }

    fn notify_cementing_failed(&mut self, hash: &BlockHash) {
        for observer in &mut self.cementing_failed {
            observer(hash);
        }
    }
}

pub struct CementingContext {
    pub block: SavedBlock,
    pub confirmation_root: BlockHash,
    pub election: Option<Arc<Mutex<ElectionData>>>,
}

struct CementedNotifier<'a> {
    confirming_set: &'a ConfirmingSetThread,
    cemented: VecDeque<CementingContext>,
    already_cemented: VecDeque<BlockHash>,
}

impl<'a> CementedNotifier<'a> {
    fn new(confirming_set: &'a ConfirmingSetThread) -> Self {
        Self {
            confirming_set,
            cemented: Default::default(),
            already_cemented: Default::default(),
        }
    }
}

impl<'a> CementingObserver<Entry> for CementedNotifier<'a> {
    fn cemented(&mut self, block: SavedBlock, root: &BlockHash, context: &Entry) {
        self.cemented.push_back(CementingContext {
            block,
            confirmation_root: *root,
            election: context.election.clone(),
        });
    }

    fn already_cemented(&mut self, hash: &BlockHash) {
        self.already_cemented.push_back(*hash);
    }

    fn max_blocks_reached(&mut self) {
        self.confirming_set.notify(&mut self.cemented);
    }

    fn cementing_failed(&mut self, _hash: &BlockHash, context: Entry) {
        self.confirming_set
            .mutex
            .lock()
            .unwrap()
            .deferred
            .push_back(context);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_core::{ConfirmationHeightInfo, SavedAccountChain};
    use std::time::Duration;

    #[test]
    fn add_exists() {
        let ledger = Arc::new(Ledger::new_null());
        let confirming_set =
            ConfirmingSet::new(Default::default(), ledger, Arc::new(Stats::default()));
        let hash = BlockHash::from(1);
        confirming_set.add(hash);
        assert!(confirming_set.contains(&hash));
    }

    #[test]
    fn process_one() {
        let mut chain = SavedAccountChain::genesis();
        let block_hash = chain.add_state().hash();
        let ledger = Arc::new(
            Ledger::new_null_builder()
                .blocks(chain.blocks())
                .confirmation_height(
                    &chain.account(),
                    &ConfirmationHeightInfo {
                        height: 1,
                        frontier: chain.open(),
                    },
                )
                .finish(),
        );
        let confirming_set =
            ConfirmingSet::new(Default::default(), ledger, Arc::new(Stats::default()));
        confirming_set.start();
        let count = Arc::new(Mutex::new(0));
        let condition = Arc::new(Condvar::new());
        let count_clone = Arc::clone(&count);
        let condition_clone = Arc::clone(&condition);
        confirming_set.on_cemented(Box::new(move |_block| {
            {
                *count_clone.lock().unwrap() += 1;
            }
            condition_clone.notify_all();
        }));

        confirming_set.add(block_hash);

        let guard = count.lock().unwrap();
        let result = condition
            .wait_timeout_while(guard, Duration::from_secs(5), |i| *i < 1)
            .unwrap()
            .1;
        assert_eq!(result.timed_out(), false);
    }
}
