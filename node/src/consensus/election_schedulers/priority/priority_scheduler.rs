use std::{
    sync::{Arc, Condvar, Mutex},
    thread::JoinHandle,
    time::Duration,
};

use tracing::trace;

use rsnano_core::{
    utils::ContainerInfo, Account, AccountInfo, Amount, BlockHash, ConfirmationHeightInfo,
    SavedBlock,
};
use rsnano_ledger::{AnySet, ConfirmedSet};
use rsnano_stats::{DetailType, StatType, Stats, StatsCollection, StatsSource};

use super::{Bucket, BucketExt, BucketStats, Bucketing, PriorityBucketConfig};
use crate::consensus::ActiveElections;

pub struct PriorityScheduler {
    stopped: Mutex<bool>,
    condition: Condvar,
    stats: Arc<Stats>,
    bucketing: Bucketing,
    buckets: Vec<Arc<Bucket>>,
    thread: Mutex<Option<JoinHandle<()>>>,
    cleanup_thread: Mutex<Option<JoinHandle<()>>>,
    bucket_stats: Arc<BucketStats>,
}

impl PriorityScheduler {
    pub(crate) fn new(
        config: PriorityBucketConfig,
        stats: Arc<Stats>,
        active: Arc<ActiveElections>,
    ) -> Self {
        let bucketing = Bucketing::default();
        let mut buckets = Vec::with_capacity(bucketing.bucket_count());
        let bucket_stats = Arc::new(BucketStats::default());
        for _ in 0..bucketing.bucket_count() {
            buckets.push(Arc::new(Bucket::new(
                config.clone(),
                active.clone(),
                bucket_stats.clone(),
            )))
        }

        Self {
            thread: Mutex::new(None),
            cleanup_thread: Mutex::new(None),
            stopped: Mutex::new(false),
            condition: Condvar::new(),
            buckets,
            bucketing,
            stats,
            bucket_stats,
        }
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
        self.buckets.iter().any(|b| b.contains(hash))
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

        let (priority_balance, priority_timestamp) = any.block_priority(&block);

        let added = self
            .find_bucket(priority_balance)
            .push(priority_timestamp, block.into());

        if added {
            self.stats
                .inc(StatType::ElectionScheduler, DetailType::Activated);
            trace!(
                account = account.encode_account(),
                time = %account_info.modified,
                priority_balance = ?priority_balance,
                priority_timestamp = ?priority_timestamp,
                "block activated"
            );
            self.condition.notify_all();
        } else {
            self.stats
                .inc(StatType::ElectionScheduler, DetailType::ActivateFull);
        }

        true // Activated
    }

    fn find_bucket(&self, priority: Amount) -> &Bucket {
        let index = self.bucketing.bucket_index(priority);
        &self.buckets[index]
    }

    pub fn len(&self) -> usize {
        self.buckets.iter().map(|b| b.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn predicate(&self) -> bool {
        self.buckets.iter().any(|b| b.available())
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
                self.stats
                    .inc(StatType::ElectionScheduler, DetailType::Loop);

                for bucket in &self.buckets {
                    if bucket.available() {
                        bucket.activate();
                    }
                }

                stopped = self.stopped.lock().unwrap();
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
                for bucket in &self.buckets {
                    bucket.update();
                }

                stopped = self.stopped.lock().unwrap();
            }
        }
    }

    pub fn activate_successors(&self, any: &impl AnySet, block: &SavedBlock) -> bool {
        let mut result = self.activate(any, &block.account());

        // Start or vote for the next unconfirmed block in the destination account
        if let Some(destination) = block.destination() {
            if block.is_send() && !destination.is_zero() && destination != block.account() {
                result |= self.activate(any, &destination);
            }
        }
        result
    }

    pub fn container_info(&self) -> ContainerInfo {
        let mut bucket_infos = ContainerInfo::builder();
        let mut election_infos = ContainerInfo::builder();

        for (id, bucket) in self.buckets.iter().enumerate() {
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
