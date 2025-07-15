use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use rsnano_core::{BlockHash, Networks, Root};
use rsnano_nullable_clock::{SteadyClock, Timestamp};
use rsnano_output_tracker::{OutputListenerMt, OutputTrackerMt};

use super::VoteGenerators;
use crate::consensus::{bounded_hash_map::BoundedHashMap, election::VoteType};

/// Tries to enqueue a vote for a given block
pub(crate) struct BlockVoter {
    vote_generators: Arc<VoteGenerators>,
    clock: Arc<SteadyClock>,
    last_votes: Mutex<BoundedHashMap<(BlockHash, VoteType), Timestamp>>,
    vote_broadcast_interval: Duration,
    vote_listener: OutputListenerMt<BlockVoteRequest>,
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
            last_votes: Mutex::new(BoundedHashMap::new(1024 * 32)),
            vote_broadcast_interval: match network {
                Networks::NanoDevNetwork => Duration::from_millis(500),
                _ => Duration::from_secs(15),
            },
            vote_listener: OutputListenerMt::new(),
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
    pub fn try_vote_for_block(&self, block_hash: BlockHash, root: Root, vote_type: VoteType) {
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

        let last_vote = self
            .last_votes
            .lock()
            .unwrap()
            .get(&(block_hash, vote_type))
            .cloned();

        if let Some(last_vote) = last_vote {
            if last_vote.elapsed(self.clock.now()) < self.vote_broadcast_interval {
                return;
            }
        }

        self.vote_generators
            .generate_vote(&root, &block_hash, vote_type);

        self.last_votes
            .lock()
            .unwrap()
            .insert((block_hash, vote_type), self.clock.now());
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

        voter.try_vote_for_block(expected.block_hash, expected.root, expected.vote_type);

        let output = vote_tracker.output();
        assert_eq!(output, [expected]);
    }
}
