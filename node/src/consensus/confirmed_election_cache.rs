use std::collections::{HashMap, VecDeque};

use rsnano_core::BlockHash;

use super::ConfirmedElection;

pub(crate) struct ConfirmedElectionsCache {
    max_len: usize,
    elections: HashMap<BlockHash, ConfirmedElection>,
    sequential: VecDeque<BlockHash>,
}

impl ConfirmedElectionsCache {
    const DEFAULT_MAX_LEN: usize = 4096;

    pub fn with_max_len(max_len: usize) -> Self {
        Self {
            max_len,
            elections: HashMap::new(),
            sequential: VecDeque::new(),
        }
    }

    #[allow(dead_code)]
    pub fn max_len(&self) -> usize {
        self.max_len
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.sequential.len()
    }

    pub fn get(&self, winner_hash: &BlockHash) -> Option<&ConfirmedElection> {
        self.elections.get(winner_hash)
    }

    pub fn insert(&mut self, election: ConfirmedElection) {
        let winner_hash = election.winner.hash();
        let old = self.elections.insert(winner_hash, election);
        if old.is_some() {
            return;
        }
        self.sequential.push_back(winner_hash);
        if self.sequential.len() > self.max_len {
            let winner = self.sequential.pop_front().unwrap();
            self.elections.remove(&winner);
        }
    }
}

impl Default for ConfirmedElectionsCache {
    fn default() -> Self {
        Self::with_max_len(Self::DEFAULT_MAX_LEN)
    }
}

#[cfg(test)]
mod tests {
    use rsnano_core::{PrivateKey, SavedBlock};

    use super::*;

    #[test]
    fn empty() {
        let cache = ConfirmedElectionsCache::default();
        assert_eq!(cache.max_len(), 4096);
        assert_eq!(cache.len(), 0);
        assert!(cache.get(&BlockHash::from(123)).is_none());
    }

    #[test]
    fn insert_one() {
        let mut cache = ConfirmedElectionsCache::default();
        let (winner, election) = create_election(1);

        cache.insert(election);

        assert_eq!(cache.len(), 1);
        let result = cache.get(&winner).unwrap();
        assert_eq!(result.winner.hash(), winner);
    }

    #[test]
    fn insert_multiple() {
        let mut cache = ConfirmedElectionsCache::default();
        let (winner1, election1) = create_election(1);
        let (winner2, election2) = create_election(2);

        cache.insert(election1);
        cache.insert(election2);

        assert_eq!(cache.len(), 2);

        let result1 = cache.get(&winner1).unwrap();
        assert_eq!(result1.winner.hash(), winner1);
        let result2 = cache.get(&winner2).unwrap();
        assert_eq!(result2.winner.hash(), winner2);
    }

    #[test]
    fn create_with_custom_max_len() {
        let cache = ConfirmedElectionsCache::with_max_len(2);
        assert_eq!(cache.max_len(), 2);
    }

    #[test]
    fn when_max_len_reached_should_discard_oldest_entry() {
        let mut cache = ConfirmedElectionsCache::with_max_len(2);
        let (winner1, election1) = create_election(1);
        let (winner2, election2) = create_election(2);
        let (winner3, election3) = create_election(3);

        cache.insert(election1);
        cache.insert(election2);
        cache.insert(election3);

        assert_eq!(cache.len(), 2);
        assert!(cache.get(&winner3).is_some());
        assert!(cache.get(&winner2).is_some());
        assert!(cache.get(&winner1).is_none());
    }

    #[test]
    fn ignore_duplicates() {
        let mut cache = ConfirmedElectionsCache::default();
        let (_, election) = create_election(1);
        cache.insert(election.clone());
        cache.insert(election);
        assert_eq!(cache.len(), 1);
    }

    fn create_election(key: impl Into<PrivateKey>) -> (BlockHash, ConfirmedElection) {
        let block = SavedBlock::new_test_instance_with_key(key);
        let winner_hash = block.hash();
        (winner_hash, ConfirmedElection::new(block))
    }
}
