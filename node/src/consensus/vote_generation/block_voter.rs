use std::sync::{Arc, Mutex};

use rsnano_core::{BlockHash, Root};
use rsnano_nullable_clock::SteadyClock;
use rsnano_output_tracker::{OutputListenerMt, OutputTrackerMt};

use super::{vote_approver::VoteApprover, VoteGenerators};
use crate::consensus::election::VoteType;

/// Tries to enqueue a vote for a given block
pub(crate) struct BlockVoter {
    vote_generators: Arc<VoteGenerators>,
    clock: Arc<SteadyClock>,
    vote_listener: OutputListenerMt<BlockVoteRequest>,
    vote_approver: Mutex<VoteApprover>,
}

impl BlockVoter {
    pub(crate) fn new(
        vote_generators: Arc<VoteGenerators>,
        clock: Arc<SteadyClock>,
        vote_approver: VoteApprover,
    ) -> Self {
        Self {
            vote_generators,
            clock,
            vote_listener: OutputListenerMt::new(),
            vote_approver: Mutex::new(vote_approver),
        }
    }

    #[allow(dead_code)]
    pub fn new_null() -> Self {
        let vote_generators = Arc::new(VoteGenerators::new_null());
        let clock = Arc::new(SteadyClock::new_null());
        Self::new(vote_generators, clock, VoteApprover::default())
    }

    #[allow(dead_code)]
    pub fn track(&self) -> Arc<OutputTrackerMt<BlockVoteRequest>> {
        self.vote_listener.track()
    }

    /// Broadcasts vote for the given block hash
    /// Checks if sufficient amount of time (`vote_generation_interval`) passed since the last vote generation
    pub fn try_vote(&self, request: BlockVoteRequest) {
        if self.vote_listener.is_tracked() {
            self.vote_listener.emit(request.clone());
        }

        if !self.vote_generators.voting_enabled() {
            return;
        }

        let now = self.clock.now();

        let should_vote = self.vote_approver.lock().unwrap().approve(&request, now);

        if should_vote {
            self.vote_generators.generate_vote(
                &request.root,
                &request.block_hash,
                request.vote_type,
            );
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BlockVoteRequest {
    pub block_hash: BlockHash,
    pub root: Root,
    pub vote_type: VoteType,
}

impl BlockVoteRequest {
    #[allow(dead_code)]
    pub fn new_test_instance() -> Self {
        Self {
            block_hash: 100.into(),
            root: 200.into(),
            vote_type: VoteType::Final,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn track_votes() {
        let vote_generators = Arc::new(VoteGenerators::new_null());
        let clock = Arc::new(SteadyClock::new_null());
        let vote_approver = VoteApprover::default();

        let voter = BlockVoter::new(vote_generators, clock, vote_approver);
        let vote_tracker = voter.track();

        let expected = BlockVoteRequest {
            block_hash: 1.into(),
            root: 2.into(),
            vote_type: VoteType::NonFinal,
        };

        voter.try_vote(BlockVoteRequest {
            block_hash: expected.block_hash,
            root: expected.root,
            vote_type: expected.vote_type,
        });

        let output = vote_tracker.output();
        assert_eq!(output, [expected]);
    }
}
