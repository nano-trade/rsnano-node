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

    pub fn contains_block(&self, block_hash: &BlockHash) -> bool {
        self.representatives
            .values()
            .any(|i| i.history.contains_key(block_hash))
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
        let entry = self
            .representatives
            .entry(vote.voter)
            .or_insert_with(|| RepresentativeEntry::new(vote.voter, weight));

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

            entry.vote_hashes.insert(vote_hash);
        }

        Ok(())
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
}

impl Default for RebroadcastHistoryConfig {
    fn default() -> Self {
        Self {
            rebroadcast_threshold: Duration::from_secs(90),
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
pub(crate) enum RebroadcastError {
    AlreadyRebroadcasted,
    RepresentativesFull,
    RebroadcastUnnecessary,
}

#[derive(Default)]
struct RepresentativeEntry {
    representative: PublicKey,
    weight: Amount,
    history: HashMap<BlockHash, RebroadcastEntry>,

    /// for quickly filtering out duplicates
    vote_hashes: HashSet<BlockHash>,
}

impl RepresentativeEntry {
    fn new(representative: PublicKey, weight: Amount) -> Self {
        Self {
            representative,
            weight,
            history: Default::default(),
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
        assert!(history.contains_block(&vote.hashes[0]), "contains block");
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

        let error = history
            .check_and_record(&vote2, TEST_WEIGHT, NOW)
            .unwrap_err();

        assert_eq!(error, RebroadcastError::RebroadcastUnnecessary);

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
        history
            .check_and_record(&final_vote, TEST_WEIGHT, NOW)
            .expect("should override");

        // Both vote should be kept in recent hashes index
        assert_eq!(history.total_history(), 1);
        assert_eq!(history.total_vote_hashes(), 2);
        assert!(history.contains_block(&vote.hashes[0]));
        assert!(history.contains_vote(&vote.hash()));
        assert!(history.contains_vote(&final_vote.hash()));
    }

    const TEST_WEIGHT: Amount = Amount::nano(100_000);
    const NOW: Timestamp = Timestamp::new_test_instance();
}
