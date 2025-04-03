use std::{cmp::min, time::Duration};

use rsnano_core::{utils::UnixMillisTimestamp, Amount, BlockHash, PublicKey, Vote};
use rsnano_nullable_clock::Timestamp;

use super::{BoundedHashMap, RebroadcastError};

pub(crate) struct RepresentativeEntry {
    pub representative: PublicKey,
    pub weight: Amount,
    pub history: BoundedHashMap<BlockHash, RebroadcastEntry>,

    /// for quickly filtering out duplicates
    pub vote_hashes: BoundedHashMap<BlockHash, ()>,
}

impl RepresentativeEntry {
    pub fn new(representative: PublicKey, weight: Amount, max_history: usize) -> Self {
        Self {
            representative,
            weight,
            history: BoundedHashMap::new(max_history),
            vote_hashes: BoundedHashMap::new(max_history),
        }
    }

    pub fn check_and_record(
        &mut self,
        vote: &Vote,
        min_gap: Duration,
        now: Timestamp,
    ) -> Result<(), RebroadcastError> {
        let vote_hash = vote.hash();
        self.ensure_not_broadcasted_yet(&vote_hash)?;
        self.ensure_broadcast_necessary(vote, min_gap, now)?;

        // Update the history with the new vote info
        for hash in &vote.hashes {
            self.history.insert(
                *hash,
                RebroadcastEntry {
                    block_hash: *hash,
                    vote_timestamp: vote.timestamp(),
                    timestamp: now,
                },
            );
        }

        // Also keep track of the vote hash to quickly filter out duplicates
        self.vote_hashes.insert(vote_hash, ());
        Ok(())
    }

    fn ensure_not_broadcasted_yet(&self, vote_hash: &BlockHash) -> Result<(), RebroadcastError> {
        if self.vote_hashes.contains_key(&vote_hash) {
            Err(RebroadcastError::AlreadyRebroadcasted)
        } else {
            Ok(())
        }
    }

    fn ensure_broadcast_necessary(
        &self,
        vote: &Vote,
        min_gap: Duration,
        now: Timestamp,
    ) -> Result<(), RebroadcastError> {
        let should_rebroadcast = vote
            .hashes
            .iter()
            .any(|hash| self.should_rebroadcast_hash(hash, vote, min_gap, now));

        if should_rebroadcast {
            Ok(())
        } else {
            Err(RebroadcastError::RebroadcastUnnecessary)
        }
    }

    fn should_rebroadcast_hash(
        &self,
        hash: &BlockHash,
        vote: &Vote,
        min_gap: Duration,
        now: Timestamp,
    ) -> bool {
        let Some(last_rebroadcast) = self.history.get(hash) else {
            // Block hash not seen before, rebroadcast
            return true;
        };

        last_rebroadcast.should_rebroadcast(vote, min_gap, now)
    }
}

#[derive(PartialEq, Eq, Debug)]
pub(crate) struct RebroadcastEntry {
    pub block_hash: BlockHash,
    pub vote_timestamp: UnixMillisTimestamp,
    pub timestamp: Timestamp,
}

impl RebroadcastEntry {
    fn should_rebroadcast(&self, new_vote: &Vote, min_gap: Duration, now: Timestamp) -> bool {
        if self.switched_to_final_vote(&new_vote) {
            return true;
        }

        if self.gap(new_vote, now) <= min_gap {
            return false;
        }

        true
    }

    fn switched_to_final_vote(&self, new_vote: &Vote) -> bool {
        new_vote.is_final() && new_vote.timestamp() > self.vote_timestamp
    }

    fn gap(&self, new_vote: &Vote, now: Timestamp) -> Duration {
        let gap_last_rebroadcast = self.timestamp.elapsed(now);
        let gap_last_vote = self.vote_timestamp.elapsed(new_vote.timestamp());
        min(gap_last_rebroadcast, gap_last_vote)
    }
}
