use std::{
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use rsnano_core::{BlockHash, Networks, Root};
use rsnano_nullable_clock::{SteadyClock, Timestamp};
use rsnano_stats::{DetailType, StatType, Stats};

use crate::consensus::{
    bounded_hash_map::BoundedHashMap, election::VoteType, ActiveElectionsContainer,
};

use super::VoteGenerators;
use rsnano_output_tracker::{OutputListenerMt, OutputTrackerMt};

/// Tries to enqueue a vote for a given block
pub(crate) struct BlockVoter {
    stats: Arc<Stats>,
    vote_generators: Arc<VoteGenerators>,
    active_elections: Arc<RwLock<ActiveElectionsContainer>>,
    clock: Arc<SteadyClock>,
    last_votes: Mutex<BoundedHashMap<(BlockHash, VoteType), Timestamp>>,
    vote_broadcast_interval: Duration,
    vote_listener: OutputListenerMt<BlockVoteRequest>,
}

impl BlockVoter {
    pub(crate) fn new(
        stats: Arc<Stats>,
        vote_generators: Arc<VoteGenerators>,
        active_elections: Arc<RwLock<ActiveElectionsContainer>>,
        clock: Arc<SteadyClock>,
        network: Networks,
    ) -> Self {
        Self {
            stats,
            vote_generators,
            active_elections,
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
        let stats = Arc::new(Stats::default());
        let vote_generators = Arc::new(VoteGenerators::new_null());
        let active_elections = Arc::new(RwLock::new(ActiveElectionsContainer::default()));
        let clock = Arc::new(SteadyClock::new_null());
        let network = Networks::NanoLiveNetwork;
        Self::new(stats, vote_generators, active_elections, clock, network)
    }

    #[allow(dead_code)]
    pub fn track(&self) -> Arc<OutputTrackerMt<BlockVoteRequest>> {
        self.vote_listener.track()
    }

    pub fn try_vote(&self, block_hash: &BlockHash) {
        let (block_hash, root, vote_type) = {
            let active = self.active_elections.read().unwrap();
            let Some(election) = active.election_for_block(block_hash) else {
                return;
            };
            (
                election.winner().hash(),
                election.qualified_root().root,
                election.vote_type(),
            )
        };

        self.try_vote_for_block(block_hash, root, vote_type);
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

        self.stats
            .inc(StatType::Election, DetailType::BroadcastVote);

        match vote_type {
            VoteType::NonFinal => {
                self.stats
                    .inc(StatType::Election, DetailType::GenerateVoteNormal);
                self.vote_generators
                    .generate_non_final_vote(&root, &block_hash);
            }
            VoteType::Final => {
                self.stats
                    .inc(StatType::Election, DetailType::GenerateVoteFinal);
                self.vote_generators.generate_final_vote(&root, &block_hash);
            }
        }

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
        let stats = Arc::new(Stats::default());
        let vote_generators = Arc::new(VoteGenerators::new_null());
        let aec = Arc::new(RwLock::new(ActiveElectionsContainer::default()));
        let clock = Arc::new(SteadyClock::new_null());
        let network = Networks::NanoLiveNetwork;

        let voter = BlockVoter::new(stats, vote_generators, aec, clock, network);
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
