use std::sync::{Arc, Mutex};

use rsnano_core::{BlockHash, Networks, Root};
use rsnano_nullable_clock::SteadyClock;
use rsnano_output_tracker::{OutputListenerMt, OutputTrackerMt};

use super::{last_votes::LastVotes, VoteGenerators};
use crate::{block_rate_calculator::CurrentBlockRates, consensus::election::VoteType};

/// Tries to enqueue a vote for a given block
pub(crate) struct BlockVoter {
    vote_generators: Arc<VoteGenerators>,
    clock: Arc<SteadyClock>,
    block_rates: Arc<CurrentBlockRates>,
    vote_listener: OutputListenerMt<BlockVoteRequest>,
    last_votes: Mutex<LastVotes>,
}

impl BlockVoter {
    pub(crate) fn new(
        vote_generators: Arc<VoteGenerators>,
        clock: Arc<SteadyClock>,
        block_rates: Arc<CurrentBlockRates>,
        network: Networks,
    ) -> Self {
        Self {
            vote_generators,
            clock,
            block_rates,
            vote_listener: OutputListenerMt::new(),
            last_votes: Mutex::new(LastVotes::new(network)),
        }
    }

    #[allow(dead_code)]
    pub fn new_null() -> Self {
        let vote_generators = Arc::new(VoteGenerators::new_null());
        let clock = Arc::new(SteadyClock::new_null());
        let block_rates = Arc::new(CurrentBlockRates::default());
        let network = Networks::NanoLiveNetwork;
        Self::new(vote_generators, clock, block_rates, network)
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

        // Testing CPS limit:
        //if self.block_rates.cps() > 500 {
        //    return;
        //}

        let now = self.clock.now();

        let should_vote = self
            .last_votes
            .lock()
            .unwrap()
            .try_insert(block_hash, vote_type, now);

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
        let block_rates = Arc::new(CurrentBlockRates::default());
        let network = Networks::NanoLiveNetwork;

        let voter = BlockVoter::new(vote_generators, clock, block_rates, network);
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
