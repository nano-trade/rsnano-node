use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use super::{
    bucket_elections::{BucketElection, BucketElections},
    ordered_blocks::{BlockEntry, OrderedBlocks},
};
use crate::consensus::{election::ElectionBehavior, ActiveElections, AecInsertError};
use rsnano_core::{utils::UnixTimestamp, Block, BlockHash, QualifiedRoot, SavedBlock};
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
    active_elections: Arc<ActiveElections>,
    data: Mutex<BucketData>,
}

impl Bucket {
    pub fn new(
        config: PriorityBucketConfig,
        active_elections: Arc<ActiveElections>,
        stats: Arc<BucketStats>,
    ) -> Self {
        Self {
            config,
            active_elections,
            data: Mutex::new(BucketData {
                queue: Default::default(),
                elections: Default::default(),
                stats,
            }),
        }
    }

    pub fn contains(&self, hash: &BlockHash) -> bool {
        self.data.lock().unwrap().queue.contains(hash)
    }

    pub fn available(&self) -> bool {
        let candidate: UnixTimestamp;
        let election_count: usize;
        let lowest: UnixTimestamp;

        {
            let guard = self.data.lock().unwrap();
            let Some(first) = guard.queue.first() else {
                return false;
            };

            candidate = first.time;
            election_count = guard.elections.len();
            lowest = guard.elections.lowest_priority();
        }

        if election_count < self.config.reserved_elections
            || election_count < self.config.max_elections
        {
            self.active_elections.read().vacancy() > 0
        } else if election_count > 0 {
            // Compare to equal to drain duplicates
            if candidate <= lowest {
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
            self.active_elections.read().vacancy() < 0
        } else {
            true
        }
    }

    pub fn update(&self) {
        let mut guard = self.data.lock().unwrap();
        if self.election_overfill(&guard) {
            guard.cancel_lowest_election(&self.active_elections);
        }
    }

    pub fn push(&self, time: UnixTimestamp, block: SavedBlock) -> bool {
        let hash = block.hash();
        let mut guard = self.data.lock().unwrap();
        let inserted = guard.queue.insert(BlockEntry { time, block });
        if guard.queue.len() > self.config.max_blocks {
            if let Some(removed) = guard.queue.pop_last() {
                inserted && !(removed.time == time && removed.block.hash() == hash)
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
}

pub(crate) trait BucketExt {
    fn activate(&self) -> bool;
}

impl BucketExt for Arc<Bucket> {
    fn activate(&self) -> bool {
        let block: SavedBlock;
        let priority: UnixTimestamp;

        {
            let mut guard = self.data.lock().unwrap();

            let Some(top) = guard.queue.pop_first() else {
                return false; // Not activated;
            };

            block = top.block;
            priority = top.time;

            guard.elections.insert(BucketElection {
                root: block.qualified_root(),
                priority,
            });
        }

        let self_w = Arc::downgrade(self);
        let erase_callback = Box::new(move |root: &QualifiedRoot| {
            let Some(self_l) = self_w.upgrade() else {
                return;
            };
            let mut guard = self_l.data.lock().unwrap();
            guard.elections.erase(root);
        });

        let root = block.qualified_root();

        let result =
            self.active_elections
                .insert(block, ElectionBehavior::Priority, Some(erase_callback));

        let mut guard = self.data.lock().unwrap();
        if result.is_err() {
            guard.elections.erase(&root);
        }
        match result {
            Ok(_) => {
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
    fn cancel_lowest_election(&mut self, active_elections: &ActiveElections) {
        if let Some(entry) = self.elections.entry_with_lowest_priority() {
            active_elections.cancel(&entry.root);
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
