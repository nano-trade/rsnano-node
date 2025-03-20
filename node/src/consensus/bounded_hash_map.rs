use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
};

pub(crate) struct BoundedHashMap<K, V>
where
    K: Hash + Eq + Clone,
    V: Eq + Clone,
{
    max_len: usize,
    entries: HashMap<K, V>,
    sequential: VecDeque<(K, V)>,
}

#[allow(dead_code)]
impl<K, V> BoundedHashMap<K, V>
where
    K: Hash + Eq + Clone,
    V: Eq + Clone,
{
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

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let result = self.entries.insert(key.clone(), value.clone());
        self.sequential.push_back((key, value));
        while self.entries.len() > self.max_len() {
            let (k, v) = self.sequential.pop_front().unwrap();
            if self.entries.get(&k) == Some(&v) {
                self.entries.remove(&k);
            }
        }
        result
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.entries.get(key)
    }
}

impl<K, V> Default for BoundedHashMap<K, V>
where
    K: Hash + Eq + Clone,
    V: Eq + Clone,
{
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

    type LastSentVotes = BoundedHashMap<(BlockHash, VoteType), Timestamp>;

    #[test]
    fn empty() {
        let last_votes = LastSentVotes::default();
        assert_eq!(last_votes.len(), 0);
        assert_eq!(
            last_votes.get(&(BlockHash::from(1), VoteType::NonFinal)),
            None
        );
        assert_eq!(last_votes.max_len(), 32768);
    }

    #[test]
    fn insert() {
        let mut last_votes = LastSentVotes::default();
        let hash = BlockHash::from(1);
        let now = Timestamp::new_test_instance();

        last_votes.insert((hash, VoteType::NonFinal), now);

        assert_eq!(last_votes.len(), 1);
        assert_eq!(last_votes.get(&(hash, VoteType::NonFinal)), Some(&now));
    }

    #[test]
    fn insert_replaces_previous_value() {
        let mut last_votes = LastSentVotes::default();
        let hash = BlockHash::from(1);
        let past = Timestamp::new_test_instance();
        let now = Timestamp::new_test_instance() + Duration::from_secs(60);

        last_votes.insert((hash, VoteType::NonFinal), past);
        last_votes.insert((hash, VoteType::NonFinal), now);

        assert_eq!(last_votes.len(), 1);
        assert_eq!(last_votes.get(&(hash, VoteType::NonFinal)), Some(&now));
    }

    #[test]
    fn insert_differentiates_vote_type() {
        let mut last_votes = BoundedHashMap::default();
        let hash = BlockHash::from(1);
        let now = Timestamp::new_test_instance();
        let later = Timestamp::new_test_instance() + Duration::from_secs(60);

        last_votes.insert((hash, VoteType::NonFinal), now);
        last_votes.insert((hash, VoteType::Final), later);

        assert_eq!(last_votes.len(), 2);
        assert_eq!(last_votes.get(&(hash, VoteType::NonFinal)), Some(&now));
        assert_eq!(last_votes.get(&(hash, VoteType::Final)), Some(&later));
    }

    #[test]
    fn insert_differentiates_hash() {
        let mut last_votes = BoundedHashMap::default();
        let hash1 = BlockHash::from(1);
        let hash2 = BlockHash::from(2);
        let now = Timestamp::new_test_instance();
        let later = Timestamp::new_test_instance() + Duration::from_secs(60);

        last_votes.insert((hash1, VoteType::NonFinal), now);
        last_votes.insert((hash2, VoteType::NonFinal), later);

        assert_eq!(last_votes.len(), 2);
        assert_eq!(last_votes.get(&(hash1, VoteType::NonFinal)), Some(&now));
        assert_eq!(last_votes.get(&(hash2, VoteType::NonFinal)), Some(&later));
    }

    #[test]
    fn overfill() {
        let mut last_votes = BoundedHashMap::with_max_len(2);
        let hash1 = BlockHash::from(1);
        let hash2 = BlockHash::from(2);
        let hash3 = BlockHash::from(3);
        let now = Timestamp::new_test_instance();

        last_votes.insert((hash1, VoteType::NonFinal), now);
        last_votes.insert((hash2, VoteType::NonFinal), now);
        last_votes.insert((hash3, VoteType::NonFinal), now);

        assert_eq!(last_votes.len(), 2);
        assert_eq!(last_votes.get(&(hash2, VoteType::NonFinal)), Some(&now));
        assert_eq!(last_votes.get(&(hash3, VoteType::NonFinal)), Some(&now));
        assert_eq!(last_votes.get(&(hash1, VoteType::NonFinal)), None);
    }
}
