use std::collections::{HashMap, VecDeque};

use rsnano_core::{Block, QualifiedRoot};

pub(crate) struct ForkCache {
    forks: HashMap<QualifiedRoot, VecDeque<Block>>,
    empty: VecDeque<Block>,
}

impl ForkCache {
    pub(crate) fn new() -> Self {
        Self {
            forks: HashMap::new(),
            empty: VecDeque::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.forks.len()
    }

    pub fn add(&mut self, fork: Block) {
        let forks = self.forks.entry(fork.qualified_root()).or_default();

        forks.push_back(fork);

        if forks.len() > 5 {
            forks.pop_front();
        }
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
        assert_forks(&cache, &QualifiedRoot::new_test_instance(), &[]);
    }

    #[test]
    fn add_one_fork() {
        let mut cache = ForkCache::default();
        let fork = Block::new_test_instance();

        cache.add(fork.clone());

        assert_eq!(cache.len(), 1);
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
