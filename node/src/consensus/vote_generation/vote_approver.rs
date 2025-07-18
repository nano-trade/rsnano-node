use super::{cps_limiter::CpsLimiter, last_votes::LastVotes, BlockVoteRequest};
use crate::consensus::election::VoteType;
use rsnano_core::Networks;
use rsnano_nullable_clock::Timestamp;

/// Decides whether it is ok to create a vote for a given block hash.
/// Vote creation is NOT approved if
///  * a vote for the same block was recently created
///  * the network CPS is above threshold
pub(crate) struct VoteApprover {
    last_votes: LastVotes,
    cps_limiter: CpsLimiter,
}

impl VoteApprover {
    pub(crate) fn new(network: Networks, cps_limiter: CpsLimiter) -> Self {
        Self {
            last_votes: LastVotes::new(network),
            cps_limiter,
        }
    }

    pub fn approve(&mut self, request: &BlockVoteRequest, now: Timestamp) -> bool {
        if request.vote_type == VoteType::NonFinal {
            if !self.cps_limiter.try_vote(now) {
                return false;
            }
        }

        self.last_votes
            .try_insert(request.block_hash, request.vote_type, now)
    }
}

impl Default for VoteApprover {
    fn default() -> Self {
        Self::new(Networks::NanoLiveNetwork, CpsLimiter::unlimited())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ntest::{assert_false, assert_true};
    use std::time::Duration;

    #[test]
    fn happy_path() {
        let cps_limiter = CpsLimiter::unlimited();
        let mut approver = VoteApprover::new(Networks::NanoLiveNetwork, cps_limiter);
        let request = BlockVoteRequest::new_test_instance();
        let now = Timestamp::new_test_instance();
        assert_true!(approver.approve(&request, now));
    }

    #[test]
    fn disapprove_if_same_vote_created_recenty() {
        let cps_limiter = CpsLimiter::unlimited();
        let mut approver = VoteApprover::new(Networks::NanoLiveNetwork, cps_limiter);
        let request = BlockVoteRequest::new_test_instance();

        let now = Timestamp::new_test_instance();
        assert_true!(approver.approve(&request, now));
        assert_false!(approver.approve(&request, now + Duration::from_secs(1)));
    }
}
