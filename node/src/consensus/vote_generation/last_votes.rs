use crate::consensus::{bounded_hash_map::BoundedHashMap, election::VoteType};
use rsnano_core::{BlockHash, Networks};
use rsnano_nullable_clock::Timestamp;
use std::time::Duration;

/// Keeps track of when the last votes where generated
pub(crate) struct LastVotes {
    vote_broadcast_interval: Duration,
    entries: BoundedHashMap<(BlockHash, VoteType), Timestamp>,
}

impl LastVotes {
    pub(crate) fn new(network: Networks) -> Self {
        Self {
            vote_broadcast_interval: match network {
                Networks::NanoDevNetwork => Duration::from_millis(500),
                _ => Duration::from_secs(15),
            },
            entries: BoundedHashMap::new(1024 * 32),
        }
    }

    pub fn try_insert(
        &mut self,
        block_hash: BlockHash,
        vote_type: VoteType,
        now: Timestamp,
    ) -> bool {
        let last_vote = self.entries.get(&(block_hash, vote_type));

        if let Some(last_vote) = last_vote {
            if last_vote.elapsed(now) < self.vote_broadcast_interval {
                return false;
            }
        }
        self.entries.insert((block_hash, vote_type), now);
        true
    }
}
