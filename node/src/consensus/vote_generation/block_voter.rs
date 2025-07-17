use std::sync::{Arc, Mutex};

use rsnano_core::{BlockHash, Networks, Root};
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
        network: Networks,
    ) -> Self {
        Self {
            vote_generators,
            clock,
            vote_listener: OutputListenerMt::new(),
            vote_approver: Mutex::new(VoteApprover::new(network)),
        }
    }

    #[allow(dead_code)]
    pub fn new_null() -> Self {
        let vote_generators = Arc::new(VoteGenerators::new_null());
        let clock = Arc::new(SteadyClock::new_null());
        let network = Networks::NanoLiveNetwork;
        Self::new(vote_generators, clock, network)
    }

    #[allow(dead_code)]
    pub fn track(&self) -> Arc<OutputTrackerMt<BlockVoteRequest>> {
        self.vote_listener.track()
    }

    /// Broadcasts vote for the given block hash
    /// Checks if sufficient amount of time (`vote_generation_interval`) passed since the last vote generation
    pub fn try_vote(&self, block_hash: BlockHash, root: Root, vote_type: VoteType) {
        if self.vote_listener.is_tracked() {
            self.vote_listener.emit(BlockVoteRequest {
                block_hash,
                root,
                vote_type,
            });
        }

        if !self.vote_generators.voting_enabled() {
            return;
        }

        let now = self.clock.now();

        let should_vote = self
            .vote_approver
            .lock()
            .unwrap()
            .approve(block_hash, vote_type, now);

        if should_vote {
            self.vote_generators
                .generate_vote(&root, &block_hash, vote_type);
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BlockVoteRequest {
    pub block_hash: BlockHash,
    pub root: Root,
    pub vote_type: VoteType,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn track_votes() {
        let vote_generators = Arc::new(VoteGenerators::new_null());
        let clock = Arc::new(SteadyClock::new_null());
        let network = Networks::NanoLiveNetwork;

        let voter = BlockVoter::new(vote_generators, clock, network);
        let vote_tracker = voter.track();

        let expected = BlockVoteRequest {
            block_hash: 1.into(),
            root: 2.into(),
            vote_type: VoteType::NonFinal,
        };

        voter.try_vote(expected.block_hash, expected.root, expected.vote_type);

        let output = vote_tracker.output();
        assert_eq!(output, [expected]);
    }
}
