mod rep_container;
mod rep_entry;

use std::{collections::HashMap, time::Duration};

use crate::consensus::bounded_hash_map::BoundedHashMap;
use rep_container::RepresentativeContainer;
use rep_entry::RepresentativeEntry;
use rsnano_core::{Amount, BlockHash, PublicKey, Vote};
use rsnano_nullable_clock::Timestamp;

/// Keeps track of past rebroadcasts and decides whether a new rebroadcast is necessary
pub(crate) struct RebroadcastHistory {
    representatives: RepresentativeContainer,
    config: RebroadcastHistoryConfig,
}

impl RebroadcastHistory {
    pub(super) fn new(config: RebroadcastHistoryConfig) -> Self {
        Self {
            representatives: Default::default(),
            config,
        }
    }

    pub fn total_representatives(&self) -> usize {
        self.representatives.len()
    }

    pub fn total_history(&self) -> usize {
        self.representatives
            .entries()
            .map(|i| i.history.len())
            .sum()
    }

    pub fn total_vote_hashes(&self) -> usize {
        self.representatives
            .entries()
            .map(|i| i.vote_hashes.len())
            .sum()
    }

    pub fn contains_representative(&self, representative: &PublicKey) -> bool {
        self.representatives.contains(representative)
    }

    pub fn contains_block(&self, representative: &PublicKey, block_hash: &BlockHash) -> bool {
        self.representatives
            .get(representative)
            .map(|i| i.history.contains_key(block_hash))
            .unwrap_or(false)
    }

    pub fn contains_vote(&self, vote_hash: &BlockHash) -> bool {
        self.representatives
            .entries()
            .any(|i| i.vote_hashes.contains_key(vote_hash))
    }

    fn check_and_record(
        &mut self,
        vote: &Vote,
        weight: Amount,
        now: Timestamp,
    ) -> Result<(), RebroadcastError> {
        self.ensure_not_full(vote, weight)?;

        let entry = if let Some(existing) = self.representatives.get_mut(&vote.voter) {
            existing
        } else {
            self.representatives.insert(RepresentativeEntry::new(
                vote.voter,
                weight,
                self.config.max_blocks_per_rep,
                self.config.rebroadcast_min_gap,
            ));

            self.representatives.get_mut(&vote.voter).unwrap()
        };

        entry.check_and_record(vote, now)?;

        self.trim_representatives();
        Ok(())
    }

    fn ensure_not_full(&self, vote: &Vote, weight: Amount) -> Result<(), RebroadcastError> {
        if self.representatives.contains(&vote.voter) || self.can_add(weight) {
            Ok(())
        } else {
            Err(RebroadcastError::RepresentativesFull)
        }
    }

    fn can_add(&self, rep_weight: Amount) -> bool {
        // Under normal conditions the number of principal representatives should be below this limit
        if self.representatives.len() < self.config.max_representatives {
            return true;
        }

        // However, if we're at capacity, we can still add the rep if it has a higher weight
        // than the lowest weight in the container
        // TODO: use a BTreeMap for the lookup!
        self.representatives
            .entries()
            .any(|i| rep_weight > i.weight)
    }

    fn trim_representatives(&mut self) {
        // Keep representatives index within limits, erase lowest weight entries
        while self.representatives.len() > self.config.max_representatives {
            // TODO use BTreeMap
            let lowest = self
                .representatives
                .entries()
                .min_by(|x, y| x.weight.cmp(&y.weight))
                .map(|i| i.representative)
                .unwrap();

            self.representatives.remove(&lowest);
        }
    }

    pub fn update_weights(&mut self, rep_weights: &HashMap<PublicKey, Amount>) {
        for entry in self.representatives.entries_mut() {
            entry.weight = rep_weights
                .get(&entry.representative)
                .cloned()
                .unwrap_or(Amount::zero());
        }
    }
}

impl Default for RebroadcastHistory {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

pub(crate) struct RebroadcastHistoryConfig {
    /// Minimum amount of time between rebroadcasts for the same hash from the same representative
    pub rebroadcast_min_gap: Duration,

    /// Maximum number of representatives to track rebroadcasts for
    pub max_representatives: usize,

    /// Maximum number of recently broadcast hashes to keep per representative
    pub max_blocks_per_rep: usize,
}

impl Default for RebroadcastHistoryConfig {
    fn default() -> Self {
        Self {
            rebroadcast_min_gap: Duration::from_secs(90),
            max_representatives: 100,
            max_blocks_per_rep: 1024 * 32,
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
pub(crate) enum RebroadcastError {
    AlreadyRebroadcasted,
    RepresentativesFull,
    RebroadcastUnnecessary,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_core::{utils::UnixMillisTimestamp, Vote};
    use std::time::Duration;

    #[test]
    fn empty() {
        let history = RebroadcastHistory::default();
        assert_eq!(history.total_representatives(), 0);
        assert_eq!(history.total_history(), 0);
        assert_eq!(history.total_vote_hashes(), 0);
        assert_eq!(history.contains_representative(&PublicKey::from(1)), false);
    }

    #[test]
    fn record_one_vote() {
        let mut history = RebroadcastHistory::default();
        let vote = Vote::new_test_instance();

        history.check_and_record(&vote, TEST_WEIGHT, NOW).unwrap();

        assert_eq!(history.total_representatives(), 1, "rep count");
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
    fn reject_when_last_rebroadcast_is_too_close() {
        let config = RebroadcastHistoryConfig {
            rebroadcast_min_gap: Duration::from_millis(1000),
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
            .timestamp(UnixMillisTimestamp::new(5000))
            .finish();

        assert_eq!(
            history.check_and_record(&vote2, TEST_WEIGHT, NOW),
            Err(RebroadcastError::RebroadcastUnnecessary)
        );

        // Try after threshold - should be accepted
        let vote3 = Vote::build_test_instance()
            .timestamp(UnixMillisTimestamp::new(9000))
            .finish();

        history
            .check_and_record(&vote3, TEST_WEIGHT, NOW + Duration::from_millis(2000))
            .unwrap();
    }

    #[test]
    fn reject_when_vote_timestamp_is_too_close_to_previous_vote() {
        let config = RebroadcastHistoryConfig {
            rebroadcast_min_gap: Duration::from_millis(1000),
            ..Default::default()
        };
        let mut history = RebroadcastHistory::new(config);

        // Initial vote
        let vote1 = Vote::build_test_instance()
            .timestamp(UnixMillisTimestamp::new(1000))
            .finish();

        history.check_and_record(&vote1, TEST_WEIGHT, NOW).unwrap();

        // timestamp too close - should be rejected
        let vote2 = Vote::build_test_instance()
            .timestamp(UnixMillisTimestamp::new(1500))
            .finish();

        assert_eq!(
            history.check_and_record(&vote2, TEST_WEIGHT, NOW + Duration::from_secs(2)),
            Err(RebroadcastError::RebroadcastUnnecessary)
        );

        // gap big enough - should be accepted
        let vote3 = Vote::build_test_instance()
            .timestamp(UnixMillisTimestamp::new(2000))
            .finish();

        history
            .check_and_record(&vote3, TEST_WEIGHT, NOW + Duration::from_secs(4))
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
        assert_eq!(history.total_representatives(), 2);

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
        assert_eq!(history.total_representatives(), 2);
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
            max_blocks_per_rep: 2,
            ..Default::default()
        });

        let vote1 = Vote::build_test_instance().blocks([1.into()]).finish();
        history.check_and_record(&vote1, TEST_WEIGHT, NOW).unwrap();

        let vote2 = Vote::build_test_instance().blocks([2.into()]).finish();
        history.check_and_record(&vote2, TEST_WEIGHT, NOW).unwrap();

        let vote3 = Vote::build_test_instance().blocks([3.into()]).finish();
        history.check_and_record(&vote3, TEST_WEIGHT, NOW).unwrap();

        assert_eq!(history.total_history(), 2, "total history");
        assert_eq!(
            history.contains_block(&vote1.voter, &1.into()),
            false,
            "oldest block not removed"
        );
        assert_eq!(
            history.contains_vote(&vote1.hash()),
            false,
            "oldest vote not removed"
        );
        assert_eq!(history.contains_vote(&vote2.hash()), true, "vote2 missing");
    }

    #[test]
    fn weight_updates() {
        let mut history = RebroadcastHistory::new(RebroadcastHistoryConfig {
            max_representatives: 2,
            ..Default::default()
        });

        let vote1 = Vote::build_test_instance().voter_key(1).finish();
        history
            .check_and_record(&vote1, Amount::from(100), NOW)
            .unwrap();

        let vote2 = Vote::build_test_instance().voter_key(2).finish();
        history
            .check_and_record(&vote2, Amount::from(200), NOW)
            .unwrap();

        // Update weight
        let weights = [
            (vote1.voter, Amount::from(1000)),
            (vote2.voter, Amount::from(2000)),
        ]
        .into();
        history.update_weights(&weights);

        // Add new rep with lower weight - should be rejected due to updated weight
        let vote3 = Vote::build_test_instance().voter_key(3).finish();
        assert_eq!(
            history.check_and_record(&vote3, Amount::from(999), NOW),
            Err(RebroadcastError::RepresentativesFull)
        );
    }

    const TEST_WEIGHT: Amount = Amount::nano(100_000);
    const NOW: Timestamp = Timestamp::new_test_instance();
}
