use std::collections::{HashMap, VecDeque};

use bounded_vec_deque::BoundedVecDeque;
use rsnano_core::{
    utils::{ContainerInfo, ContainerInfoProvider},
    Block, BlockHash, QualifiedRoot,
};
use rsnano_stats::{StatsCollection, StatsSource};

pub(crate) struct ForkCache {
    forks: HashMap<QualifiedRoot, Entry>,
    sequential: VecDeque<QualifiedRoot>,
    empty: Entry,
    max_len: usize,
    max_forks_per_root: usize,
    inserted: u64,
}

impl ForkCache {
    pub const DEFAULT_MAX_FORKS_PER_ROOT: usize = 10;
    pub const DEFAULT_MAX_LEN: usize = 1024 * 16;

    pub(crate) fn new() -> Self {
        Self::with_max_len(Self::DEFAULT_MAX_LEN)
    }

    pub fn with_max_len(max_len: usize) -> Self {
        Self::with(max_len, Self::DEFAULT_MAX_FORKS_PER_ROOT)
    }

    pub fn with(max_len: usize, max_forks_per_root: usize) -> Self {
        Self {
            forks: HashMap::new(),
            sequential: VecDeque::new(),
            empty: Entry::new(0),
            max_len,
            max_forks_per_root,
            inserted: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.forks.len()
    }

    #[allow(dead_code)]
    pub fn max_len(&self) -> usize {
        self.max_len
    }

    pub fn add(&mut self, fork: Block) {
        let forks = self
            .forks
            .entry(fork.qualified_root())
            .or_insert_with(|| Entry::new(self.max_forks_per_root));

        if forks.contains(&fork.hash()) {
            return;
        }

        if forks.is_empty() {
            self.sequential.push_back(fork.qualified_root());
        }

        forks.add(fork);
        self.inserted += 1;

        if self.forks.len() > self.max_len {
            let root = self.sequential.pop_front().unwrap();
            self.forks.remove(&root);
        }
    }

    #[allow(dead_code)]
    pub fn contains(&self, root: &QualifiedRoot) -> bool {
        self.forks.contains_key(root)
    }

    pub fn get_forks(&self, root: &QualifiedRoot) -> impl Iterator<Item = &Block> {
        self.forks.get(root).unwrap_or(&self.empty).iter()
    }
}

impl Default for ForkCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ContainerInfoProvider for ForkCache {
    fn container_info(&self) -> ContainerInfo {
        [("fork_cache", self.len(), 0)].into()
    }
}

struct Entry {
    blocks: BoundedVecDeque<Block>,
}

impl Entry {
    fn new(max_forks: usize) -> Self {
        Self {
            blocks: BoundedVecDeque::new(max_forks),
        }
    }

    fn add(&mut self, block: Block) {
        self.blocks.push_back(block);
    }

    fn contains(&self, hash: &BlockHash) -> bool {
        self.iter().any(|i| i.hash() == *hash)
    }

    fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    fn iter(&self) -> impl Iterator<Item = &Block> {
        self.blocks.iter()
    }
}

impl StatsSource for ForkCache {
    fn collect_stats(&self, result: &mut StatsCollection) {
        result.insert("fork_cache", "insert", self.inserted);
    }
}

#[cfg(test)]
mod tests {
    use rsnano_core::{Amount, BlockHash, StateBlockArgs};

    use super::*;

    #[test]
    fn empty() {
        let cache = ForkCache::default();
        assert_eq!(cache.len(), 0);
        assert_eq!(ForkCache::DEFAULT_MAX_LEN, 1024 * 16);
        assert_eq!(cache.max_len(), ForkCache::DEFAULT_MAX_LEN);
        assert_forks(&cache, &QualifiedRoot::new_test_instance(), &[]);
        assert!(!cache.contains(&QualifiedRoot::new_test_instance()))
    }

    #[test]
    fn add_one_fork() {
        let mut cache = ForkCache::default();
        let fork = Block::new_test_instance();

        cache.add(fork.clone());

        assert_eq!(cache.len(), 1);
        assert!(cache.contains(&fork.qualified_root()));
        assert_forks(&cache, &fork.qualified_root(), &[fork]);
    }

    #[test]
    fn add_two_forks_for_two_different_roots() {
        let mut cache = ForkCache::default();

        let fork1 = create_block(BlockHash::from(1), Amount::from(1));
        let fork2 = create_block(BlockHash::from(2), Amount::from(1));

        cache.add(fork1.clone());
        cache.add(fork2.clone());

        assert_eq!(cache.len(), 2);
        assert_forks(&cache, &fork1.qualified_root(), &[fork1]);
        assert_forks(&cache, &fork2.qualified_root(), &[fork2]);
    }

    #[test]
    fn add_two_forks_for_the_same_root() {
        let mut cache = ForkCache::default();

        let fork1 = create_block(BlockHash::from(1), Amount::from(2));
        let fork2 = create_block(BlockHash::from(1), Amount::from(3));

        cache.add(fork1.clone());
        cache.add(fork2.clone());

        assert_eq!(cache.len(), 1);
        assert_forks(&cache, &fork1.qualified_root(), &[fork1, fork2]);
    }

    #[test]
    fn ignore_duplicate_forks() {
        let mut cache = ForkCache::default();

        let fork = create_block(BlockHash::from(1), Amount::from(2));

        cache.add(fork.clone());
        cache.add(fork.clone());

        assert_forks(&cache, &fork.qualified_root(), &[fork]);
    }

    #[test]
    fn limit_forks_per_root() {
        let mut cache = ForkCache::with(1024, 5);

        let fork1 = create_block(BlockHash::from(1), Amount::from(2));
        let fork2 = create_block(BlockHash::from(1), Amount::from(3));
        let fork3 = create_block(BlockHash::from(1), Amount::from(4));
        let fork4 = create_block(BlockHash::from(1), Amount::from(5));
        let fork5 = create_block(BlockHash::from(1), Amount::from(6));
        let fork6 = create_block(BlockHash::from(1), Amount::from(7));

        cache.add(fork1.clone());
        cache.add(fork2.clone());
        cache.add(fork3.clone());
        cache.add(fork4.clone());
        cache.add(fork5.clone());
        cache.add(fork6.clone());

        assert_forks(
            &cache,
            &fork1.qualified_root(),
            &[fork2, fork3, fork4, fork5, fork6],
        );
    }

    #[test]
    fn limit_cache_size() {
        let mut cache = ForkCache::with_max_len(3);
        assert_eq!(cache.max_len(), 3);

        let fork1 = create_block(BlockHash::from(1), Amount::from(1));
        let fork2 = create_block(BlockHash::from(2), Amount::from(1));
        let fork3 = create_block(BlockHash::from(3), Amount::from(1));
        let fork4 = create_block(BlockHash::from(4), Amount::from(1));

        cache.add(fork1.clone());
        cache.add(fork2.clone());
        cache.add(fork3.clone());
        cache.add(fork4.clone());

        assert_eq!(cache.len(), 3);
        assert_eq!(cache.contains(&fork1.qualified_root()), false);
        assert_eq!(cache.contains(&fork4.qualified_root()), true);
    }

    #[test]
    fn stats() {
        let mut cache = ForkCache::with_max_len(3);
        assert_eq!(cache.max_len(), 3);

        let fork1 = create_block(BlockHash::from(1), Amount::from(1));
        let fork2 = create_block(BlockHash::from(2), Amount::from(1));
        let fork3 = create_block(BlockHash::from(3), Amount::from(1));
        let fork4 = create_block(BlockHash::from(4), Amount::from(1));

        cache.add(fork1.clone());
        cache.add(fork2.clone());
        cache.add(fork3.clone());
        cache.add(fork4.clone());
        cache.add(fork4.clone());
        cache.add(fork4.clone());

        let mut stats = StatsCollection::new();
        cache.collect_stats(&mut stats);
        assert_eq!(stats.get("fork_cache", "insert"), 4);
    }

    fn create_block(previous: BlockHash, balance: Amount) -> Block {
        StateBlockArgs {
            previous,
            balance,
            ..StateBlockArgs::new_test_instance()
        }
        .into()
    }

    fn assert_forks(cache: &ForkCache, root: &QualifiedRoot, expected: &[Block]) {
        let forks: Vec<Block> = cache.get_forks(root).cloned().collect();
        assert_eq!(forks, expected);
    }
}
