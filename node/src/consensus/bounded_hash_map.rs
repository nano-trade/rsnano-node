use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
};

pub(crate) struct BoundedHashMap<K, V>
where
    K: Hash + Eq + Clone,
{
    max_len: usize,
    entries: HashMap<K, (V, usize)>,
    sequential: VecDeque<(K, usize)>,
    next_id: usize,
}

#[allow(dead_code)]
impl<K, V> BoundedHashMap<K, V>
where
    K: Hash + Eq + Clone,
{
    pub fn new(max_len: usize) -> Self {
        Self {
            max_len,
            entries: HashMap::new(),
            sequential: VecDeque::new(),
            next_id: 1,
        }
    }

    pub fn max_len(&self) -> usize {
        self.max_len
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let new_id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        let result = self.entries.insert(key.clone(), (value, new_id));
        self.sequential.push_back((key, new_id));
        while self.entries.len() > self.max_len() {
            let (k, id_to_remove) = self.sequential.pop_front().unwrap();
            if let Some((_, id)) = self.entries.get(&k) {
                if *id == id_to_remove {
                    self.entries.remove(&k);
                }
            }
        }
        result.map(|(old, _)| old)
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.entries.get(key).map(|(value, _)| value)
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.entries.contains_key(key)
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
        let last_votes = LastSentVotes::new(100);
        assert_eq!(last_votes.len(), 0);
        assert_eq!(
            last_votes.get(&(BlockHash::from(1), VoteType::NonFinal)),
            None
        );
        assert_eq!(last_votes.max_len(), 100);
    }

    #[test]
    fn insert() {
        let mut last_votes = LastSentVotes::new(100);
        let hash = BlockHash::from(1);
        let now = Timestamp::new_test_instance();

        last_votes.insert((hash, VoteType::NonFinal), now);

        assert_eq!(last_votes.len(), 1);
        assert_eq!(last_votes.get(&(hash, VoteType::NonFinal)), Some(&now));
    }

    #[test]
    fn insert_replaces_previous_value() {
        let mut last_votes = LastSentVotes::new(100);
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
        let mut last_votes = BoundedHashMap::new(100);
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
        let mut last_votes = BoundedHashMap::new(100);
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
        let mut last_votes = BoundedHashMap::new(2);
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
