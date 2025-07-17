use super::last_votes::LastVotes;
use crate::consensus::election::VoteType;
use rsnano_core::{BlockHash, Networks};
use rsnano_nullable_clock::Timestamp;

/// Decides whether it is ok to create a vote for a given block hash
pub(crate) struct VoteApprover {
    last_votes: LastVotes,
}
impl VoteApprover {
    pub(crate) fn new(network: Networks) -> Self {
        Self {
            last_votes: LastVotes::new(network),
        }
    }

    pub fn approve(&mut self, block_hash: BlockHash, vote_type: VoteType, now: Timestamp) -> bool {
        self.last_votes.try_insert(block_hash, vote_type, now)
    }
}
