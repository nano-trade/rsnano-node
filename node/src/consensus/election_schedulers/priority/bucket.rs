use super::{
    bucket_elections::{BucketElection, BucketElections},
    ordered_blocks::{BlockEntry, OrderedBlocks},
};
use crate::consensus::AecInsertRequest;
use rsnano_core::{utils::BlockPriority, BlockHash, QualifiedRoot, SavedBlock};

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
    queue: OrderedBlocks,
    elections: BucketElections,
}

impl Bucket {
    pub fn new(config: PriorityBucketConfig) -> Self {
        Self {
            config,
            queue: Default::default(),
            elections: Default::default(),
        }
    }

    pub fn contains(&self, hash: &BlockHash) -> bool {
        self.queue.contains(hash)
    }

    pub fn available(&self, aec_vacancy: i64) -> bool {
        let Some(highest) = self.queue.highest_prio() else {
            // No blocks enqueued
            return false;
        };

        let candidate_prio = highest.priority.time;
        let active_elections = self.elections.len();
        let highest_election = self.elections.highest_priority();

        if self.election_slots_available(active_elections) {
            aec_vacancy > 0
        } else if active_elections > 0 {
            // Compare to equal to drain duplicates
            if candidate_prio >= highest_election {
                // Bound number of reprioritizations
                active_elections < self.config.max_elections * 2
            } else {
                false
            }
        } else {
            false
        }
    }

    fn election_slots_available(&self, started_elections: usize) -> bool {
        started_elections < self.config.reserved_elections
            || started_elections < self.config.max_elections
    }

    fn election_overfill(&self, aec_vacancy: i64) -> bool {
        if self.elections.len() < self.config.reserved_elections {
            false
        } else if self.elections.len() < self.config.max_elections {
            aec_vacancy < 0
        } else {
            true
        }
    }

    pub fn election_to_cancel(&self, aec_vacancy: i64) -> Option<QualifiedRoot> {
        if self.election_overfill(aec_vacancy) {
            self.cancel_election_with_lowest_prio()
        } else {
            None
        }
    }

    pub fn push(&mut self, priority: BlockPriority, block: SavedBlock) -> bool {
        let hash = block.hash();
        let inserted = self.queue.insert(BlockEntry::new(block, priority));
        if self.queue.len() > self.config.max_blocks {
            if let Some(removed) = self.queue.pop_lowest_prio() {
                inserted && !(removed.priority == priority && removed.block.hash() == hash)
            } else {
                inserted
            }
        } else {
            inserted
        }
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn election_count(&self) -> usize {
        self.elections.len()
    }

    pub fn blocks(&self) -> impl Iterator<Item = &SavedBlock> {
        self.queue.iter().map(|i| &i.block)
    }

    pub fn remove_election(&mut self, root: &QualifiedRoot) {
        self.elections.erase(root);
    }

    pub fn activate(&mut self) -> Option<AecInsertRequest> {
        let Some(top) = self.queue.pop_highest_prio() else {
            return None; // Not activated;
        };

        let block = top.block;
        let priority = top.priority;
        let root = block.qualified_root();

        self.elections.insert(BucketElection {
            root,
            priority: priority.time,
        });

        Some(AecInsertRequest::new_priority(block, priority))
    }

    fn cancel_election_with_lowest_prio(&self) -> Option<QualifiedRoot> {
        if let Some(entry) = self.elections.entry_with_highest_priority() {
            Some(entry.root.clone())
        } else {
            None
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_core::{utils::TimePriority, Amount};

    #[test]
    fn construction() {
        let fixture = create_fixture();
        let bucket = &fixture.bucket;

        assert_eq!(bucket.len(), 0);
        assert_eq!(bucket.contains(&BlockHash::from(1)), false);
        assert_eq!(bucket.available(123), false);
    }

    #[test]
    fn insert_one() {
        let mut fixture = create_fixture();
        let bucket = &mut fixture.bucket;
        let block = SavedBlock::new_test_instance();

        assert!(bucket.push(test_priority(1000), block.clone()));

        assert_eq!(bucket.len(), 1);
        assert_eq!(bucket.contains(&block.hash()), true);
        assert_eq!(bucket.available(123), true);
    }

    #[test]
    fn insert_duplicate() {
        let mut fixture = create_fixture();
        let bucket = &mut fixture.bucket;
        let block = SavedBlock::new_test_instance();

        assert_eq!(bucket.push(test_priority(1000), block.clone()), true);
        assert_eq!(bucket.push(test_priority(1000), block), false);
        assert_eq!(bucket.len(), 1);
    }

    #[test]
    fn insert_many() {
        let mut fixture = create_fixture();
        let bucket = &mut fixture.bucket;
        let block0 = SavedBlock::new_test_instance_with_key(1);
        let block1 = SavedBlock::new_test_instance_with_key(2);
        let block2 = SavedBlock::new_test_instance_with_key(3);
        let block3 = SavedBlock::new_test_instance_with_key(4);
        assert!(bucket.push(test_priority(2000), block0.clone()));
        assert!(bucket.push(test_priority(1001), block1.clone()));
        assert!(bucket.push(test_priority(1000), block2.clone()));
        assert!(bucket.push(test_priority(900), block3.clone()));

        assert_eq!(bucket.len(), 4);
        let blocks: Vec<_> = bucket.blocks().cloned().collect();
        assert_eq!(blocks.len(), 4);
        // Ensure correct order
        assert_eq!(blocks[0], block3);
        assert_eq!(blocks[1], block2);
        assert_eq!(blocks[2], block1);
        assert_eq!(blocks[3], block0);
    }

    #[test]
    fn max_blocks() {
        let mut fixture = create_fixture_with(FixtureArgs {
            config: PriorityBucketConfig {
                max_blocks: 2,
                ..Default::default()
            },
        });
        let bucket = &mut fixture.bucket;

        let block0 = SavedBlock::new_test_instance_with_key(1);
        let block1 = SavedBlock::new_test_instance_with_key(2);
        let block2 = SavedBlock::new_test_instance_with_key(3);
        let block3 = SavedBlock::new_test_instance_with_key(4);

        assert_eq!(bucket.push(test_priority(2000), block0.clone()), true);
        assert_eq!(bucket.push(test_priority(900), block1.clone()), true);
        assert_eq!(bucket.push(test_priority(3000), block2.clone()), false);
        assert_eq!(bucket.push(test_priority(1001), block3.clone()), true); // Evicts 2000
        assert_eq!(bucket.contains(&block0.hash()), false);
        assert_eq!(bucket.push(test_priority(1000), block0.clone()), true); // Evicts 1001
        assert_eq!(bucket.contains(&block3.hash()), false);

        assert_eq!(bucket.len(), 2);
        let blocks: Vec<_> = bucket.blocks().cloned().collect();
        // Ensure correct order
        assert_eq!(blocks[0], block1);
        assert_eq!(blocks[1], block0);
    }

    #[derive(Default)]
    struct FixtureArgs {
        config: PriorityBucketConfig,
    }

    struct Fixture {
        bucket: Bucket,
    }

    fn create_fixture() -> Fixture {
        create_fixture_with(FixtureArgs::default())
    }

    fn create_fixture_with(args: FixtureArgs) -> Fixture {
        let bucket = Bucket::new(args.config);

        Fixture { bucket }
    }

    fn test_priority(time_prio: u64) -> BlockPriority {
        BlockPriority::new(Amount::nano(1), TimePriority::new(time_prio))
    }
}
