use std::collections::{HashMap, VecDeque};

use rsnano_core::{Block, QualifiedRoot};

pub(crate) struct ForkCache {
    forks: HashMap<QualifiedRoot, VecDeque<Block>>,
    sequential: VecDeque<QualifiedRoot>,
    empty: VecDeque<Block>,
    max_len: usize,
}

impl ForkCache {
    const MAX_FORKS_PER_ROOT: usize = 5;

    pub(crate) fn new() -> Self {
        Self::with_max_len(1024)
    }

    pub fn with_max_len(max_len: usize) -> Self {
        Self {
            forks: HashMap::new(),
            sequential: VecDeque::new(),
            empty: VecDeque::new(),
            max_len,
        }
    }

    pub fn len(&self) -> usize {
        self.forks.len()
    }

    pub fn max_len(&self) -> usize {
        self.max_len
    }

    pub fn add(&mut self, fork: Block) {
        let forks = self.forks.entry(fork.qualified_root()).or_default();

        if forks.is_empty() {
            self.sequential.push_back(fork.qualified_root());
        }
        forks.push_back(fork);

        if forks.len() > Self::MAX_FORKS_PER_ROOT {
            forks.pop_front();
        }

        if self.forks.len() > self.max_len {
            let root = self.sequential.pop_front().unwrap();
            self.forks.remove(&root);
        }
    }

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

#[cfg(test)]
mod tests {
    use rsnano_core::{Amount, BlockHash, StateBlockArgs};

    use super::*;

    #[test]
    fn empty() {
        let cache = ForkCache::default();
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.max_len(), 1024);
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
    fn limit_to_5_forks_per_root() {
        let mut cache = ForkCache::default();

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
