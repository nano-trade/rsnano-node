use std::collections::{HashMap, VecDeque};

use rsnano_core::BlockHash;
use rsnano_nullable_clock::Timestamp;

use crate::consensus::VoteType;

/// Keeps track of when votes were sent;
pub(crate) struct LastSentVotes {
    max_len: usize,
    entries: HashMap<(BlockHash, VoteType), Timestamp>,
    sequential: VecDeque<(BlockHash, VoteType, Timestamp)>,
}

impl LastSentVotes {
    pub(crate) fn new() -> Self {
        Self::with_max_len(1024 * 32)
    }

    pub fn with_max_len(max_len: usize) -> Self {
        Self {
            max_len,
            entries: HashMap::new(),
            sequential: VecDeque::new(),
        }
    }

    pub fn max_len(&self) -> usize {
        self.max_len
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn insert(&mut self, hash: BlockHash, vote_type: VoteType, now: Timestamp) {
        self.entries.insert((hash, vote_type), now);
        self.sequential.push_back((hash, vote_type, now));
        while self.entries.len() > self.max_len() {
            let (hash, vote_type, timestamp) = self.sequential.pop_front().unwrap();
            let key = (hash, vote_type);
            if self.entries.get(&key) == Some(&timestamp) {
                self.entries.remove(&key);
            }
        }
    }

    pub fn get(&self, hash: BlockHash, vote_type: VoteType) -> Option<Timestamp> {
        self.entries.get(&(hash, vote_type)).cloned()
    }
}

impl Default for LastSentVotes {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rsnano_core::BlockHash;
    use rsnano_nullable_clock::Timestamp;

    use crate::consensus::VoteType;

    use super::*;

    #[test]
    fn empty() {
        let last_votes = LastSentVotes::default();
        assert_eq!(last_votes.len(), 0);
        assert_eq!(last_votes.get(BlockHash::from(1), VoteType::NonFinal), None);
        assert_eq!(last_votes.max_len(), 32768);
    }

    #[test]
    fn insert() {
        let mut last_votes = LastSentVotes::default();
        let hash = BlockHash::from(1);
        let now = Timestamp::new_test_instance();

        last_votes.insert(hash, VoteType::NonFinal, now);

        assert_eq!(last_votes.len(), 1);
        assert_eq!(last_votes.get(hash, VoteType::NonFinal), Some(now));
    }

    #[test]
    fn insert_replaces_previous_value() {
        let mut last_votes = LastSentVotes::default();
        let hash = BlockHash::from(1);
        let past = Timestamp::new_test_instance();
        let now = Timestamp::new_test_instance() + Duration::from_secs(60);

        last_votes.insert(hash, VoteType::NonFinal, past);
        last_votes.insert(hash, VoteType::NonFinal, now);

        assert_eq!(last_votes.len(), 1);
        assert_eq!(last_votes.get(hash, VoteType::NonFinal), Some(now));
    }

    #[test]
    fn insert_differentiates_vote_type() {
        let mut last_votes = LastSentVotes::default();
        let hash = BlockHash::from(1);
        let now = Timestamp::new_test_instance();
        let later = Timestamp::new_test_instance() + Duration::from_secs(60);

        last_votes.insert(hash, VoteType::NonFinal, now);
        last_votes.insert(hash, VoteType::Final, later);

        assert_eq!(last_votes.len(), 2);
        assert_eq!(last_votes.get(hash, VoteType::NonFinal), Some(now));
        assert_eq!(last_votes.get(hash, VoteType::Final), Some(later));
    }

    #[test]
    fn insert_differentiates_hash() {
        let mut last_votes = LastSentVotes::default();
        let hash1 = BlockHash::from(1);
        let hash2 = BlockHash::from(2);
        let now = Timestamp::new_test_instance();
        let later = Timestamp::new_test_instance() + Duration::from_secs(60);

        last_votes.insert(hash1, VoteType::NonFinal, now);
        last_votes.insert(hash2, VoteType::NonFinal, later);

        assert_eq!(last_votes.len(), 2);
        assert_eq!(last_votes.get(hash1, VoteType::NonFinal), Some(now));
        assert_eq!(last_votes.get(hash2, VoteType::NonFinal), Some(later));
    }

    #[test]
    fn overfill() {
        let mut last_votes = LastSentVotes::with_max_len(2);
        let hash1 = BlockHash::from(1);
        let hash2 = BlockHash::from(2);
        let hash3 = BlockHash::from(3);
        let now = Timestamp::new_test_instance();

        last_votes.insert(hash1, VoteType::NonFinal, now);
        last_votes.insert(hash2, VoteType::NonFinal, now);
        last_votes.insert(hash3, VoteType::NonFinal, now);

        assert_eq!(last_votes.len(), 2);
        assert_eq!(last_votes.get(hash2, VoteType::NonFinal), Some(now));
        assert_eq!(last_votes.get(hash3, VoteType::NonFinal), Some(now));
        assert_eq!(last_votes.get(hash1, VoteType::NonFinal), None);
    }
}
