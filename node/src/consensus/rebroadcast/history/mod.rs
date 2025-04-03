use crate::consensus::bounded_hash_map::BoundedHashMap;
use rsnano_core::{utils::UnixMillisTimestamp, Amount, BlockHash, PublicKey, Vote};
use rsnano_nullable_clock::Timestamp;
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

/// Keeps track of past rebroadcasts and decides whether a new rebroadcast is necessary
pub(crate) struct RebroadcastHistory {
    representatives: HashMap<PublicKey, RepresentativeEntry>,
    config: RebroadcastHistoryConfig,
}

impl RebroadcastHistory {
    pub(super) fn new(config: RebroadcastHistoryConfig) -> Self {
        Self {
            representatives: HashMap::new(),
            config,
        }
    }

    pub fn representatives_count(&self) -> usize {
        self.representatives.len()
    }

    pub fn total_history(&self) -> usize {
        self.representatives.values().map(|i| i.history.len()).sum()
    }

    pub fn total_vote_hashes(&self) -> usize {
        self.representatives
            .values()
            .map(|i| i.vote_hashes.len())
            .sum()
    }

    pub fn contains_representative(&self, representative: &PublicKey) -> bool {
        self.representatives.contains_key(representative)
    }

    pub fn contains_block(&self, representative: &PublicKey, block_hash: &BlockHash) -> bool {
        self.representatives
            .get(representative)
            .map(|i| i.history.contains_key(block_hash))
            .unwrap_or(false)
    }

    pub fn contains_vote(&self, vote_hash: &BlockHash) -> bool {
        self.representatives
            .values()
            .any(|i| i.vote_hashes.contains(vote_hash))
    }

    fn check_and_record(
        &mut self,
        vote: &Vote,
        weight: Amount,
        now: Timestamp,
    ) -> Result<(), RebroadcastError> {
        if !self.representatives.contains_key(&vote.voter) && !self.should_add(weight) {
            return Err(RebroadcastError::RepresentativesFull);
        }

        let entry = self.representatives.entry(vote.voter).or_insert_with(|| {
            RepresentativeEntry::new(vote.voter, weight, self.config.max_history)
        });

        let vote_hash = vote.hash();

        // Check if we already rebroadcasted this exact vote (fast lookup by hash)
        if entry.vote_hashes.contains(&vote_hash) {
            return Err(RebroadcastError::AlreadyRebroadcasted);
        }

        let should_rebroadcast = vote.hashes.iter().any(|hash| {
            let Some(existing) = entry.history.get(hash) else {
                // Block hash not seen before, rebroadcast
                return true;
            };

            // Always rebroadcast vote if rep switched to a final vote
            if vote.is_final() && vote.timestamp() > existing.vote_timestamp {
                return true;
            }

            // Only rebroadcast if sufficient time has passed since the last rebroadcast
            if existing.timestamp + self.config.rebroadcast_threshold > now {
                return false;
            }

            // Enough time has passed, block hash qualifies for rebroadcast
            true
        });

        if !should_rebroadcast {
            return Err(RebroadcastError::RebroadcastUnnecessary);
        }

        for hash in &vote.hashes {
            entry.history.insert(
                *hash,
                RebroadcastEntry {
                    block_hash: *hash,
                    vote_timestamp: vote.timestamp(),
                    timestamp: now,
                },
            );

            // Also keep track of the vote hash to quickly filter out duplicates
            entry.vote_hashes.insert(vote_hash);
        }

        // Keep representatives index within limits, erase lowest weight entries
        while self.representatives.len() > self.config.max_representatives {
            // TODO use BTreeMap
            let lowest = self
                .representatives
                .values()
                .min_by(|x, y| x.weight.cmp(&y.weight))
                .map(|i| i.representative)
                .unwrap();

            self.representatives.remove(&lowest);
        }

        Ok(())
    }

    fn should_add(&self, rep_weight: Amount) -> bool {
        // Under normal conditions the number of principal representatives should be below this limit
        if self.representatives.len() < self.config.max_representatives {
            return true;
        }

        // However, if we're at capacity, we can still add the rep if it has a higher weight
        // than the lowest weight in the container
        // TODO: use a BTreeMap for the lookup!
        self.representatives.values().any(|i| rep_weight > i.weight)
    }

    pub fn cleanup(&mut self, mut rep_query: impl FnMut(&PublicKey) -> (bool, Amount)) -> usize {
        // Remove entries for accounts that are no longer principal representatives
        let old_count = self.representatives_count();
        self.representatives.retain(|_, i| {
            let (keep, _) = rep_query(&i.representative);
            keep
        });

        // Update representative weights
        for entry in self.representatives.values_mut() {
            let (_, weight) = rep_query(&entry.representative);
            entry.weight = weight;
        }

        let removed = old_count - self.representatives_count();
        removed
    }
}

impl Default for RebroadcastHistory {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

pub(crate) struct RebroadcastHistoryConfig {
    /// Minimum amount of time between rebroadcasts for the same hash from the same representative
    pub rebroadcast_threshold: Duration,

    /// Maximum number of representatives to track rebroadcasts for
    pub max_representatives: usize,

    /// Maximum number of recently broadcast hashes to keep per representative
    pub max_history: usize,
}

impl Default for RebroadcastHistoryConfig {
    fn default() -> Self {
        Self {
            rebroadcast_threshold: Duration::from_secs(90),
            max_representatives: 100,
            max_history: 1024 * 32,
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
pub(crate) enum RebroadcastError {
    AlreadyRebroadcasted,
    RepresentativesFull,
    RebroadcastUnnecessary,
}

struct RepresentativeEntry {
    representative: PublicKey,
    weight: Amount,
    history: BoundedHashMap<BlockHash, RebroadcastEntry>,

    /// for quickly filtering out duplicates
    vote_hashes: HashSet<BlockHash>,
}

impl RepresentativeEntry {
    fn new(representative: PublicKey, weight: Amount, max_history: usize) -> Self {
        Self {
            representative,
            weight,
            history: BoundedHashMap::new(max_history),
            vote_hashes: Default::default(),
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
pub(crate) struct RebroadcastEntry {
    pub block_hash: BlockHash,
    pub vote_timestamp: UnixMillisTimestamp,
    pub timestamp: Timestamp,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_core::Vote;
    use std::time::Duration;

    #[test]
    fn empty() {
        let history = RebroadcastHistory::default();
        assert_eq!(history.representatives_count(), 0);
        assert_eq!(history.total_history(), 0);
        assert_eq!(history.total_vote_hashes(), 0);
        assert_eq!(history.contains_representative(&PublicKey::from(1)), false);
    }

    #[test]
    fn record_one_vote() {
        let mut history = RebroadcastHistory::default();
        let vote = Vote::new_test_instance();

        history.check_and_record(&vote, TEST_WEIGHT, NOW).unwrap();

        assert_eq!(history.representatives_count(), 1, "rep count");
        assert_eq!(history.total_history(), 1, "total history");
        assert_eq!(history.total_vote_hashes(), 1, "total hashes");
        assert!(history.contains_representative(&vote.voter), "contains rep");
        assert!(
            history.contains_block(&vote.voter, &vote.hashes[0]),
            "contains block"
        );
    }

    #[test]
    fn reject_duplicate_vote() {
        let mut history = RebroadcastHistory::default();
        let vote = Vote::new_test_instance();

        // First vote should be accepted
        history.check_and_record(&vote, TEST_WEIGHT, NOW).unwrap();

        // Same vote should be rejected
        let error = history
            .check_and_record(&vote, TEST_WEIGHT, NOW)
            .unwrap_err();
        assert_eq!(error, RebroadcastError::AlreadyRebroadcasted);

        // Even after time threshold
        let error = history
            .check_and_record(&vote, TEST_WEIGHT, NOW + Duration::from_secs(60 * 60))
            .unwrap_err();
        assert_eq!(error, RebroadcastError::AlreadyRebroadcasted);
    }

    #[test]
    fn rebroadcast_timing() {
        let config = RebroadcastHistoryConfig {
            rebroadcast_threshold: Duration::from_millis(1000),
            ..Default::default()
        };
        let mut history = RebroadcastHistory::new(config);

        // Initial vote
        let vote1 = Vote::build_test_instance()
            .timestamp(UnixMillisTimestamp::new(1000))
            .finish();

        history.check_and_record(&vote1, TEST_WEIGHT, NOW).unwrap();

        // Try rebroadcast immediately - should be rejected
        let vote2 = Vote::build_test_instance()
            .timestamp(UnixMillisTimestamp::new(1500))
            .finish();

        assert_eq!(
            history.check_and_record(&vote2, TEST_WEIGHT, NOW),
            Err(RebroadcastError::RebroadcastUnnecessary)
        );

        // Try after threshold - should be accepted
        let vote3 = Vote::build_test_instance()
            .timestamp(UnixMillisTimestamp::new(2500))
            .finish();

        history
            .check_and_record(&vote3, TEST_WEIGHT, NOW + Duration::from_millis(2000))
            .unwrap();
    }

    #[test]
    fn final_vote_override() {
        let mut history = RebroadcastHistory::default();

        // Regular vote
        let vote = Vote::build_test_instance().finish();
        history.check_and_record(&vote, TEST_WEIGHT, NOW).unwrap();

        // Final vote should override timing restrictions
        let final_vote = Vote::build_test_instance().final_vote().finish();
        assert_eq!(
            history.check_and_record(&final_vote, TEST_WEIGHT, NOW),
            Ok(()),
            "should override"
        );

        // Both vote should be kept in recent hashes index
        assert_eq!(history.total_history(), 1);
        assert_eq!(history.total_vote_hashes(), 2);
        assert!(history.contains_block(&vote.voter, &vote.hashes[0]));
        assert!(history.contains_vote(&vote.hash()));
        assert!(history.contains_vote(&final_vote.hash()));
    }

    #[test]
    fn representative_limit() {
        let mut history = RebroadcastHistory::new(RebroadcastHistoryConfig {
            max_representatives: 2,
            ..Default::default()
        });

        // Add first rep (weight 100)
        let vote1 = Vote::build_test_instance().voter_key(1).finish();
        assert_eq!(
            history.check_and_record(&vote1, Amount::from(100), NOW),
            Ok(())
        );

        // Add second rep (weight 200)
        let vote2 = Vote::build_test_instance().voter_key(2).finish();
        assert_eq!(
            history.check_and_record(&vote2, Amount::from(200), NOW),
            Ok(())
        );
        assert_eq!(history.representatives_count(), 2);

        // Try to add third rep with lower weight - should be rejected
        let vote3 = Vote::build_test_instance().voter_key(3).finish();
        assert_eq!(
            history.check_and_record(&vote3, Amount::from(50), NOW),
            Err(RebroadcastError::RepresentativesFull)
        );

        // Add third rep with higher weight - should replace lowest weight
        let vote4 = Vote::build_test_instance().voter_key(4).finish();
        assert_eq!(
            history.check_and_record(&vote4, Amount::from(300), NOW),
            Ok(())
        );
        // Lowest weight was removed
        assert_eq!(
            history.contains_representative(&vote1.voter),
            false,
            "voter 1 removed"
        );
        assert_eq!(history.representatives_count(), 2);
    }

    #[test]
    fn multi_hash_vote() {
        let mut history = RebroadcastHistory::default();
        let vote = Vote::build_test_instance()
            .blocks([BlockHash::from(1), BlockHash::from(2), BlockHash::from(3)])
            .finish();

        history.check_and_record(&vote, TEST_WEIGHT, NOW).unwrap();

        assert_eq!(history.total_history(), 3);
        assert!(history.contains_block(&vote.voter, &BlockHash::from(1)));
        assert!(history.contains_block(&vote.voter, &BlockHash::from(2)));
        assert!(history.contains_block(&vote.voter, &BlockHash::from(3)));
    }

    #[test]
    fn history_limit() {
        let mut history = RebroadcastHistory::new(RebroadcastHistoryConfig {
            max_history: 2,
            ..Default::default()
        });

        let vote1 = Vote::build_test_instance().blocks([1.into()]).finish();
        history.check_and_record(&vote1, TEST_WEIGHT, NOW).unwrap();

        let vote2 = Vote::build_test_instance().blocks([2.into()]).finish();
        history.check_and_record(&vote2, TEST_WEIGHT, NOW).unwrap();

        let vote3 = Vote::build_test_instance().blocks([3.into()]).finish();
        history.check_and_record(&vote3, TEST_WEIGHT, NOW).unwrap();

        assert_eq!(history.total_history(), 2);
        assert_eq!(history.contains_block(&vote1.voter, &1.into()), false); // Oldest was removed
    }

    #[test]
    fn cleanup() {
        let mut history = RebroadcastHistory::default();

        // Add two reps
        let vote1 = Vote::build_test_instance().voter_key(1).finish();
        let vote2 = Vote::build_test_instance().voter_key(2).finish();
        history.check_and_record(&vote1, TEST_WEIGHT, NOW).unwrap();
        history.check_and_record(&vote2, TEST_WEIGHT, NOW).unwrap();

        // Cleanup with rep1 becoming non-principal
        let cleanup_count = history.cleanup(|rep| {
            if *rep == vote1.voter {
                (false, Amount::zero())
            } else {
                (true, TEST_WEIGHT)
            }
        });

        assert_eq!(cleanup_count, 1);
        assert_eq!(history.representatives_count(), 1);
        assert_eq!(history.contains_representative(&vote1.voter), false);
        assert_eq!(history.contains_representative(&vote2.voter), true);
    }

    #[test]
    fn weight_updates() {
        let mut history = RebroadcastHistory::new(RebroadcastHistoryConfig {
            max_representatives: 1,
            ..Default::default()
        });

        // Add rep with initial weight
        let vote1 = Vote::build_test_instance().voter_key(1).finish();
        history
            .check_and_record(&vote1, Amount::from(100), NOW)
            .unwrap();

        // Update weight through cleanup
        history.cleanup(|_| (true, Amount::from(200)));

        // Add new rep with lower weight - should be rejected due to updated weight
        let vote2 = Vote::build_test_instance().voter_key(2).finish();
        assert_eq!(
            history.check_and_record(&vote2, Amount::from(150), NOW),
            Err(RebroadcastError::RepresentativesFull)
        );
    }

    const TEST_WEIGHT: Amount = Amount::nano(100_000);
    const NOW: Timestamp = Timestamp::new_test_instance();
}
