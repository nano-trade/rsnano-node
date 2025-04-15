use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex, RwLock,
};

use super::{
    bucket_elections::{BucketElection, BucketElections},
    ordered_blocks::{BlockEntry, OrderedBlocks},
};
use crate::consensus::{ActiveElectionsContainer, AecInsertError, AecInsertRequest};
use rsnano_core::{
    utils::{BlockPriority, TimePriority},
    Block, BlockHash, QualifiedRoot, SavedBlock,
};
use rsnano_nullable_clock::SteadyClock;
use rsnano_stats::{StatsCollection, StatsSource};

#[derive(Clone, Debug, PartialEq)]
pub struct PriorityBucketConfig {
    /// Maximum number of blocks to sort by priority per bucket.
    pub max_blocks: usize,

    /// Number of guaranteed slots per bucket available for election activation.
    pub reserved_elections: usize,

    /// Maximum number of slots per bucket available for election activation if the active election count is below the configured limit. (node.active_elections.size)
    pub max_elections: usize,
}

impl Default for PriorityBucketConfig {
    fn default() -> Self {
        Self {
            max_blocks: 1024 * 8,
            reserved_elections: 100,
            max_elections: 150,
        }
    }
}

/// A struct which holds an ordered set of blocks to be scheduled, ordered by their block arrival time
/// TODO: This combines both block ordering and election management, which makes the class harder to test. The functionality should be split.
pub struct Bucket {
    config: PriorityBucketConfig,
    active_elections: Arc<RwLock<ActiveElectionsContainer>>,
    data: Mutex<BucketData>,
    clock: Arc<SteadyClock>,
}

impl Bucket {
    pub fn new(
        config: PriorityBucketConfig,
        active_elections: Arc<RwLock<ActiveElectionsContainer>>,
        stats: Arc<BucketStats>,
        clock: Arc<SteadyClock>,
    ) -> Self {
        Self {
            config,
            active_elections,
            data: Mutex::new(BucketData {
                queue: Default::default(),
                elections: Default::default(),
                stats,
            }),
            clock,
        }
    }

    pub fn contains(&self, hash: &BlockHash) -> bool {
        self.data.lock().unwrap().queue.contains(hash)
    }

    pub fn available(&self) -> bool {
        let candidate_prio: TimePriority;
        let highest_election: TimePriority;
        let election_count: usize;

        {
            let guard = self.data.lock().unwrap();
            let Some(highest) = guard.queue.highest_prio() else {
                return false;
            };

            candidate_prio = highest.priority.time;
            election_count = guard.elections.len();
            highest_election = guard.elections.highest_priority();
        }

        if election_count < self.config.reserved_elections
            || election_count < self.config.max_elections
        {
            self.active_elections.read().unwrap().vacancy() > 0
        } else if election_count > 0 {
            // Compare to equal to drain duplicates
            if candidate_prio >= highest_election {
                // Bound number of reprioritizations
                election_count < self.config.max_elections * 2
            } else {
                false
            }
        } else {
            false
        }
    }

    fn election_overfill(&self, data: &BucketData) -> bool {
        if data.elections.len() < self.config.reserved_elections {
            false
        } else if data.elections.len() < self.config.max_elections {
            self.active_elections.read().unwrap().vacancy() < 0
        } else {
            true
        }
    }

    pub fn update(&self) {
        let mut guard = self.data.lock().unwrap();
        if self.election_overfill(&guard) {
            guard.cancel_election_with_lowest_prio(&self.active_elections);
        }
    }

    pub fn push(&self, priority: BlockPriority, block: SavedBlock) -> bool {
        let hash = block.hash();
        let mut guard = self.data.lock().unwrap();
        let inserted = guard.queue.insert(BlockEntry::new(block, priority));
        if guard.queue.len() > self.config.max_blocks {
            if let Some(removed) = guard.queue.pop_lowest_prio() {
                inserted && !(removed.priority == priority && removed.block.hash() == hash)
            } else {
                inserted
            }
        } else {
            inserted
        }
    }

    pub fn len(&self) -> usize {
        self.data.lock().unwrap().queue.len()
    }

    pub fn election_count(&self) -> usize {
        self.data.lock().unwrap().elections.len()
    }

    pub fn blocks(&self) -> Vec<Block> {
        let guard = self.data.lock().unwrap();
        guard.queue.iter().map(|i| i.block.clone().into()).collect()
    }

    pub fn remove_election(&self, root: &QualifiedRoot) {
        self.data.lock().unwrap().elections.erase(root);
    }

    pub fn activate(&self) -> bool {
        let mut guard = self.data.lock().unwrap();

        let Some(top) = guard.queue.pop_highest_prio() else {
            return false; // Not activated;
        };

        let block = top.block;
        let priority = top.priority;
        let root = block.qualified_root();

        let now = self.clock.now();
        let result = self
            .active_elections
            .write()
            .unwrap()
            .insert(AecInsertRequest::new_priority(block, priority), now);

        match result {
            Ok(()) => {
                guard.elections.insert(BucketElection {
                    root,
                    priority: priority.time,
                });
                guard.stats.activate_success.fetch_add(1, Ordering::Relaxed);
            }
            Err(AecInsertError::Duplicate) => {
                guard
                    .stats
                    .activate_failed_duplicate
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(AecInsertError::RecentlyConfirmed) => {
                guard
                    .stats
                    .activate_failed_confirmed
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(AecInsertError::Stopped) => {}
        }

        result.is_ok()
    }
}

struct BucketData {
    queue: OrderedBlocks,
    elections: BucketElections,
    stats: Arc<BucketStats>,
}

impl BucketData {
    fn cancel_election_with_lowest_prio(
        &mut self,
        active_elections: &RwLock<ActiveElectionsContainer>,
    ) {
        if let Some(entry) = self.elections.entry_with_highest_priority() {
            active_elections.write().unwrap().cancel(&entry.root);
            self.stats.cancelled.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[derive(Default)]
pub struct BucketStats {
    cancelled: AtomicUsize,
    activate_success: AtomicUsize,
    activate_failed_duplicate: AtomicUsize,
    activate_failed_confirmed: AtomicUsize,
}

const STATS_KEY: &'static str = "election_bucket";

impl StatsSource for BucketStats {
    fn collect_stats(&self, result: &mut StatsCollection) {
        result.insert(
            STATS_KEY,
            "cancel_lowest",
            self.cancelled.load(Ordering::Relaxed),
        );
        result.insert(
            STATS_KEY,
            "activate_success",
            self.activate_success.load(Ordering::Relaxed),
        );
        result.insert(
            STATS_KEY,
            "activate_failed_duplicate",
            self.activate_failed_duplicate.load(Ordering::Relaxed),
        );
        result.insert(
            STATS_KEY,
            "activate_failed_confirmed",
            self.activate_failed_confirmed.load(Ordering::Relaxed),
        );
    }
}
