use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use rsnano_core::{BlockHash, Networks, Root};
use rsnano_nullable_clock::{SteadyClock, Timestamp};
use rsnano_stats::{DetailType, StatType, Stats};

use crate::consensus::{bounded_hash_map::BoundedHashMap, election::VoteType, ActiveElections};

use super::VoteGenerators;

/// Tries to enqueue a vote for a given block
pub(crate) struct BlockVoter {
    stats: Arc<Stats>,
    vote_generators: Arc<VoteGenerators>,
    active_elections: Arc<ActiveElections>,
    clock: Arc<SteadyClock>,
    last_votes: Mutex<BoundedHashMap<(BlockHash, VoteType), Timestamp>>,
    vote_broadcast_interval: Duration,
}

impl BlockVoter {
    pub(crate) fn new(
        stats: Arc<Stats>,
        vote_generators: Arc<VoteGenerators>,
        active_elections: Arc<ActiveElections>,
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
        }
    }

    pub fn try_vote(&self, block_hash: &BlockHash) {
        let (block_hash, root, vote_type) = {
            let active = self.active_elections.read();
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
