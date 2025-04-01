use rsnano_core::{utils::UnixMillisTimestamp, Amount, BlockHash, PublicKey, Vote, VoteTimestamp};
use rsnano_nullable_clock::Timestamp;
use std::collections::{HashMap, HashSet};

/// Keeps track of past rebroadcasts and decides whether a new rebroadcast is necessary
pub(crate) struct RebroadcastHistory {
    representatives: HashMap<PublicKey, RepresentativeEntry>,
}

impl RebroadcastHistory {
    pub(super) fn new() -> Self {
        Self {
            representatives: HashMap::new(),
        }
    }

    pub fn representatives_count(&self) -> usize {
        self.representatives.len()
    }

    pub fn total_history(&self) -> usize {
        self.representatives.values().map(|i| i.history.len()).sum()
    }

    pub fn total_hashes(&self) -> usize {
        self.representatives.values().map(|i| i.hashes.len()).sum()
    }

    pub fn contains_representative(&self, representative: &PublicKey) -> bool {
        self.representatives.contains_key(representative)
    }

    pub fn contains_block(&self, block_hash: &BlockHash) -> bool {
        self.representatives
            .values()
            .any(|i| i.history.contains_key(block_hash))
    }

    fn check_and_record(
        &mut self,
        vote: &Vote,
        weight: Amount,
        now: Timestamp,
    ) -> Result<(), RebroadcastError> {
        let mut entry = RepresentativeEntry {
            representative: vote.voter,
            weight,
            ..Default::default()
        };

        for hash in &vote.hashes {
            entry.history.insert(
                *hash,
                RebroadcastEntry {
                    block_hash: *hash,
                    vote_timestamp: vote.timestamp(),
                    timestamp: now,
                },
            );

            entry.hashes.insert(*hash);
        }

        self.representatives.insert(vote.voter, entry);
        Ok(())
    }
}

impl Default for RebroadcastHistory {
    fn default() -> Self {
        Self::new()
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
    hashes: HashSet<BlockHash>,
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

    #[test]
    fn empty() {
        let history = RebroadcastHistory::default();
        assert_eq!(history.representatives_count(), 0);
        assert_eq!(history.total_history(), 0);
        assert_eq!(history.total_hashes(), 0);
        assert_eq!(history.contains_representative(&PublicKey::from(1)), false);
    }

    #[test]
    fn record_one_vote() {
        let mut history = RebroadcastHistory::default();
        let vote = Vote::new_test_instance();
        let weight = Amount::nano(100_000);
        let now = Timestamp::new_test_instance();

        history.check_and_record(&vote, weight, now).unwrap();

        assert_eq!(history.representatives_count(), 1, "rep count");
        assert_eq!(history.total_history(), 1, "total history");
        assert_eq!(history.total_hashes(), 1, "total hashes");
        assert!(history.contains_representative(&vote.voter), "contains rep");
        assert!(history.contains_block(&vote.hashes[0]), "contains block");
    }
}
