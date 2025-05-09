use std::{
    sync::{atomic::Ordering, Arc, Condvar, Mutex, RwLock},
    thread::JoinHandle,
    time::Duration,
};

use tracing::trace;

use rsnano_core::{
    utils::{BlockPriority, ContainerInfo},
    Account, AccountInfo, Amount, BlockHash, ConfirmationHeightInfo, QualifiedRoot, SavedBlock,
};
use rsnano_ledger::{AnySet, ConfirmedSet};
use rsnano_stats::{DetailType, StatType, Stats, StatsCollection, StatsSource};

use super::{bucket_stats::BucketStats, Bucket, Bucketing, PriorityBucketConfig};
use crate::consensus::{ActiveElectionsContainer, AecInsertError};
use rsnano_nullable_clock::SteadyClock;
use rsnano_output_tracker::{OutputListenerMt, OutputTrackerMt};

pub struct PriorityScheduler {
    stopped: Mutex<bool>,
    condition: Condvar,
    stats: Arc<Stats>,
    bucketing: Bucketing,
    buckets: Mutex<Vec<Bucket>>,
    thread: Mutex<Option<JoinHandle<()>>>,
    cleanup_thread: Mutex<Option<JoinHandle<()>>>,
    bucket_stats: BucketStats,
    clock: Arc<SteadyClock>,
    active_elections: Arc<RwLock<ActiveElectionsContainer>>,
    activate_successors_listener: OutputListenerMt<SavedBlock>,
}

impl PriorityScheduler {
    pub(crate) fn new(
        config: PriorityBucketConfig,
        stats: Arc<Stats>,
        active_elections: Arc<RwLock<ActiveElectionsContainer>>,
        clock: Arc<SteadyClock>,
    ) -> Self {
        let bucketing = Bucketing::default();
        let mut buckets = Vec::with_capacity(bucketing.bucket_count());
        for _ in 0..bucketing.bucket_count() {
            buckets.push(Bucket::new(config.clone()))
        }

        Self {
            thread: Mutex::new(None),
            cleanup_thread: Mutex::new(None),
            stopped: Mutex::new(false),
            condition: Condvar::new(),
            buckets: Mutex::new(buckets),
            bucketing,
            stats,
            bucket_stats: BucketStats::default(),
            clock,
            active_elections,
            activate_successors_listener: Default::default(),
        }
    }

    pub fn track_activate_successors(&self) -> Arc<OutputTrackerMt<SavedBlock>> {
        self.activate_successors_listener.track()
    }

    pub fn bucketing(&self) -> &Bucketing {
        &self.bucketing
    }

    pub fn stop(&self) {
        *self.stopped.lock().unwrap() = true;
        self.condition.notify_all();
        let handle = self.thread.lock().unwrap().take();
        if let Some(handle) = handle {
            handle.join().unwrap();
        }
        let handle = self.cleanup_thread.lock().unwrap().take();
        if let Some(handle) = handle {
            handle.join().unwrap();
        }
    }

    pub fn notify(&self) {
        self.condition.notify_all();
    }

    pub fn contains(&self, hash: &BlockHash) -> bool {
        self.buckets
            .lock()
            .unwrap()
            .iter()
            .any(|b| b.contains(hash))
    }

    pub fn activate(&self, any: &impl AnySet, account: &Account) -> bool {
        debug_assert!(!account.is_zero());
        if let Some(account_info) = any.get_account(account) {
            let conf_info = any.confirmed().get_conf_info(account).unwrap_or_default();

            if conf_info.height < account_info.block_count {
                return self.activate_with_info(any, account, &account_info, &conf_info);
            }
        };

        self.stats
            .inc(StatType::ElectionScheduler, DetailType::ActivateSkip);
        false // Not activated
    }

    pub fn activate_with_info(
        &self,
        any: &impl AnySet,
        account: &Account,
        account_info: &AccountInfo,
        conf_info: &ConfirmationHeightInfo,
    ) -> bool {
        debug_assert!(conf_info.frontier != account_info.head);

        let hash = match conf_info.height {
            0 => account_info.open_block,
            _ => any.block_successor(&conf_info.frontier).unwrap(),
        };

        let Some(block) = any.get_block(&hash) else {
            // Not activated
            return false;
        };

        if !any.dependents_confirmed(&block) {
            self.stats
                .inc(StatType::ElectionScheduler, DetailType::ActivateFailed);
            return false; // Not activated
        }

        let priority = any.block_priority(&block);

        let added = {
            let mut buckets = self.buckets.lock().unwrap();
            self.find_bucket(&mut buckets, priority.balance)
                .push(priority, block.into())
        };

        if added {
            self.stats
                .inc(StatType::ElectionScheduler, DetailType::Activated);
            trace!(
                account = account.encode_account(),
                time = %account_info.modified,
                priority_balance = ?priority.balance,
                priority_timestamp = ?priority.time,
                "block activated"
            );
            self.condition.notify_all();
        } else {
            self.stats
                .inc(StatType::ElectionScheduler, DetailType::ActivateFull);
        }

        true // Activated
    }

    fn find_bucket<'a, 'b>(
        &'a self,
        buckets: &'b mut [Bucket],
        priority: Amount,
    ) -> &'b mut Bucket {
        let index = self.bucketing.bucket_index(priority);
        &mut buckets[index]
    }

    pub fn len(&self) -> usize {
        self.buckets.lock().unwrap().iter().map(|b| b.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn predicate(&self) -> bool {
        let vacancy = self.active_elections.read().unwrap().vacancy();
        self.buckets
            .lock()
            .unwrap()
            .iter()
            .any(|b| b.available(vacancy))
    }

    fn run(&self) {
        let mut stopped = self.stopped.lock().unwrap();
        while !*stopped {
            stopped = self
                .condition
                .wait_while(stopped, |s| !*s && !self.predicate())
                .unwrap();

            if !*stopped {
                drop(stopped);
                self.run_one();
                stopped = self.stopped.lock().unwrap();
            }
        }
    }

    fn run_one(&self) {
        self.stats
            .inc(StatType::ElectionScheduler, DetailType::Loop);

        let now = self.clock.now();
        let mut buckets = self.buckets.lock().unwrap();
        for bucket in buckets.iter_mut() {
            let aec_vacancy = self.active_elections.read().unwrap().vacancy();
            if bucket.available(aec_vacancy) {
                if let Some(insert_req) = bucket.activate() {
                    let root = insert_req.block.qualified_root();

                    let result = self
                        .active_elections
                        .write()
                        .unwrap()
                        .insert(insert_req, now);

                    if result.is_err() {
                        bucket.remove_election(&root);
                    }

                    match result {
                        Ok(()) => {
                            self.bucket_stats
                                .activate_success
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        Err(AecInsertError::Duplicate) => {
                            self.bucket_stats
                                .activate_failed_duplicate
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        Err(AecInsertError::RecentlyConfirmed) => {
                            self.bucket_stats
                                .activate_failed_confirmed
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        Err(AecInsertError::Stopped) => {}
                    }
                }
            }
        }
    }

    fn run_cleanup(&self) {
        let mut stopped = self.stopped.lock().unwrap();
        while !*stopped {
            stopped = self
                .condition
                .wait_timeout_while(stopped, Duration::from_secs(1), |s| !*s)
                .unwrap()
                .0;

            if !*stopped {
                drop(stopped);
                self.stats
                    .inc(StatType::ElectionScheduler, DetailType::Cleanup);
                let mut buckets = self.buckets.lock().unwrap();
                let mut aec = self.active_elections.write().unwrap();
                for bucket in buckets.iter_mut() {
                    if let Some(root) = bucket.election_to_cancel(aec.vacancy()) {
                        aec.cancel(&root);
                        self.bucket_stats.cancelled.fetch_add(1, Ordering::Relaxed);
                    }
                }

                stopped = self.stopped.lock().unwrap();
            }
        }
    }

    pub fn activate_successors(&self, any: &impl AnySet, block: &SavedBlock) -> bool {
        if self.activate_successors_listener.is_tracked() {
            self.activate_successors_listener.emit(block.clone());
        }
        self.activate(any, &block.account()) | self.activate_destination_account(any, &block)
    }

    fn activate_destination_account(&self, any: &impl AnySet, block: &SavedBlock) -> bool {
        if let Some(destination) = block.destination() {
            if block.is_send() && !destination.is_zero() && destination != block.account() {
                return self.activate(any, &destination);
            }
        }
        false
    }

    pub fn remove_election(&self, priority: BlockPriority, root: &QualifiedRoot) {
        let mut buckets = self.buckets.lock().unwrap();
        self.find_bucket(&mut buckets, priority.balance)
            .remove_election(root)
    }

    pub fn container_info(&self) -> ContainerInfo {
        let mut bucket_infos = ContainerInfo::builder();
        let mut election_infos = ContainerInfo::builder();

        for (id, bucket) in self.buckets.lock().unwrap().iter().enumerate() {
            bucket_infos = bucket_infos.leaf(id.to_string(), bucket.len(), 0);
            election_infos = election_infos.leaf(id.to_string(), bucket.election_count(), 0);
        }

        ContainerInfo::builder()
            .node("blocks", bucket_infos.finish())
            .node("elections", election_infos.finish())
            .finish()
    }
}

impl Drop for PriorityScheduler {
    fn drop(&mut self) {
        // Thread must be stopped before destruction
        debug_assert!(self.thread.lock().unwrap().is_none());
        debug_assert!(self.cleanup_thread.lock().unwrap().is_none());
    }
}

pub trait PrioritySchedulerExt {
    fn start(&self);
}

impl PrioritySchedulerExt for Arc<PriorityScheduler> {
    fn start(&self) {
        debug_assert!(self.thread.lock().unwrap().is_none());
        debug_assert!(self.cleanup_thread.lock().unwrap().is_none());

        let self_l = Arc::clone(&self);
        *self.thread.lock().unwrap() = Some(
            std::thread::Builder::new()
                .name("Sched Priority".to_string())
                .spawn(Box::new(move || {
                    self_l.run();
                }))
                .unwrap(),
        );

        let self_l = Arc::clone(&self);
        *self.cleanup_thread.lock().unwrap() = Some(
            std::thread::Builder::new()
                .name("Sched Priority Clean".to_string())
                .spawn(Box::new(move || {
                    self_l.run_cleanup();
                }))
                .unwrap(),
        );
    }
}

impl StatsSource for PriorityScheduler {
    fn collect_stats(&self, result: &mut StatsCollection) {
        self.bucket_stats.collect_stats(result);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_core::PrivateKey;
    use rsnano_ledger::{Ledger, LedgerInserter};

    #[test]
    fn can_track_successor_activation() {
        let scheduler = create_test_scheduler();
        let block = SavedBlock::new_test_instance();
        let ledger = Ledger::new_null();
        let tracker = scheduler.track_activate_successors();

        scheduler.activate_successors(&ledger.any(), &block);

        let output = tracker.output();
        assert_eq!(output, [block]);
    }

    #[test]
    fn activate_successors() {
        let scheduler = create_test_scheduler();

        let ledger = Ledger::new_null();
        let inserter = LedgerInserter::new(&ledger);
        let destination = PrivateKey::from(1);
        let send1 = inserter.genesis().send(&destination, 100);
        let send2 = inserter.genesis().send(Account::from(2), 100);
        let open = inserter.account(&destination).receive(send1.hash());

        ledger.confirm(send1.hash());
        scheduler.activate_successors(&ledger.any(), &send1);
        scheduler.run_one();

        let aec = scheduler.active_elections.read().unwrap();
        assert!(aec.is_active_hash(&send2.hash()));
        assert!(aec.is_active_hash(&open.hash()));
    }

    fn create_test_scheduler() -> PriorityScheduler {
        let config = PriorityBucketConfig::default();
        let stats = Arc::new(Stats::default());
        let active_elections = Arc::new(RwLock::new(ActiveElectionsContainer::default()));
        let clock = Arc::new(SteadyClock::new_null());
        PriorityScheduler::new(config, stats, active_elections, clock)
    }
}
