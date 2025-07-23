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

    // TODO remove
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
    block_queue: OrderedBlocks,
    elections: BucketElections,
}

impl Bucket {
    pub fn new(config: PriorityBucketConfig) -> Self {
        Self {
            config,
            block_queue: Default::default(),
            elections: Default::default(),
        }
    }

    pub fn contains(&self, hash: &BlockHash) -> bool {
        self.block_queue.contains(hash)
    }

    pub fn len(&self) -> usize {
        self.block_queue.len()
    }

    pub fn election_count(&self) -> usize {
        self.elections.len()
    }

    pub fn blocks(&self) -> impl Iterator<Item = &SavedBlock> {
        self.block_queue.iter().map(|i| &i.block)
    }

    pub fn insert(
        &mut self,
        priority: BlockPriority,
        block: SavedBlock,
    ) -> Result<BlockEviction, BucketInsertError> {
        let hash = block.hash();
        let inserted = self.block_queue.insert(BlockEntry::new(block, priority));
        if !inserted {
            return Err(BucketInsertError::Duplicate);
        }

        if self.block_queue.len() > self.config.max_blocks {
            let removed = self.block_queue.pop_lowest_prio().unwrap();
            if removed.block.hash() == hash {
                return Err(BucketInsertError::PriorityTooLow);
            }
            Ok(BlockEviction::Evicted)
        } else {
            Ok(BlockEviction::None)
        }
    }

    pub fn available(&self, aec_vacancy: i64) -> bool {
        let Some(highest_block) = self.block_queue.highest_prio() else {
            // No blocks enqueued
            return false;
        };

        let candidate_prio = highest_block.priority.time;
        let active_elections = self.elections.len();
        let lowest_election = self.elections.lowest_priority();
        let can_reprioritize = candidate_prio > lowest_election;

        if can_reprioritize {
            return true;
        }

        if active_elections >= self.config.reserved_elections {
            return false;
        }

        aec_vacancy > 0 // cooldown check. TODO: check for cooldown explicitly
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

    pub fn activate(&mut self, aec_vacancy: i64) -> Option<AecInsertRequest> {
        if !self.available(aec_vacancy) {
            return None;
        }

        let Some(top) = self.block_queue.pop_highest_prio() else {
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

    pub fn election_to_cancel(&self, aec_vacancy: i64) -> Option<QualifiedRoot> {
        if self.election_overfill(aec_vacancy) {
            self.root_of_lowest_prio_election()
        } else {
            None
        }
    }

    fn root_of_lowest_prio_election(&self) -> Option<QualifiedRoot> {
        self.elections
            .entry_with_lowest_priority()
            .map(|i| i.root.clone())
    }

    pub fn remove_election(&mut self, root: &QualifiedRoot) {
        self.elections.erase(root);
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum BlockEviction {
    /// The block was inserted WITHOUT removing another block
    None,
    /// The block was inserted and a block with lower priority got removed
    Evicted,
}

#[derive(PartialEq, Eq, Debug)]
pub enum BucketInsertError {
    /// The block was already in the bucket
    Duplicate,
    /// The bucket was full and the blocks priority was too low to replace another block
    PriorityTooLow,
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

        assert_eq!(
            bucket.insert(test_priority(1000), block.clone()),
            Ok(BlockEviction::None)
        );

        assert_eq!(bucket.len(), 1);
        assert_eq!(bucket.contains(&block.hash()), true);
        assert_eq!(bucket.available(123), true);
    }

    #[test]
    fn insert_duplicate() {
        let mut fixture = create_fixture();
        let bucket = &mut fixture.bucket;
        let block = SavedBlock::new_test_instance();

        assert_eq!(
            bucket.insert(test_priority(1000), block.clone()),
            Ok(BlockEviction::None)
        );
        assert_eq!(
            bucket.insert(test_priority(1000), block),
            Err(BucketInsertError::Duplicate)
        );
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
        assert_eq!(
            bucket.insert(test_priority(2000), block0.clone()),
            Ok(BlockEviction::None)
        );
        assert_eq!(
            bucket.insert(test_priority(1001), block1.clone()),
            Ok(BlockEviction::None)
        );
        assert_eq!(
            bucket.insert(test_priority(1000), block2.clone()),
            Ok(BlockEviction::None)
        );
        assert_eq!(
            bucket.insert(test_priority(900), block3.clone()),
            Ok(BlockEviction::None)
        );

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

        assert_eq!(
            bucket.insert(test_priority(2000), block0.clone()),
            Ok(BlockEviction::None)
        );
        assert_eq!(
            bucket.insert(test_priority(900), block1.clone()),
            Ok(BlockEviction::None)
        );
        assert_eq!(
            bucket.insert(test_priority(3000), block2.clone()),
            Err(BucketInsertError::PriorityTooLow)
        );
        assert_eq!(
            bucket.insert(test_priority(1001), block3.clone()),
            Ok(BlockEviction::Evicted)
        ); // Evicts 2000
        assert_eq!(bucket.contains(&block0.hash()), false);
        assert_eq!(
            bucket.insert(test_priority(1000), block0.clone()),
            Ok(BlockEviction::Evicted)
        ); // Evicts 1001
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
