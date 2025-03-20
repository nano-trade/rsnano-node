use std::sync::{Arc, Mutex};

use rsnano_core::BlockHash;
use rsnano_nullable_clock::SteadyClock;
use rsnano_stats::{DetailType, StatType, Stats};
use tracing::trace;

use crate::consensus::{ActiveElections, Election};

use super::{last_sent_votes::LastSentVotes, VoteGenerators};

/// Tries to generate a vote for a given election
pub(crate) struct ElectionVoter {
    stats: Arc<Stats>,
    vote_generators: Arc<VoteGenerators>,
    active_elections: Arc<ActiveElections>,
    clock: Arc<SteadyClock>,
    last_votes: Mutex<LastSentVotes>,
}

impl ElectionVoter {
    pub(crate) fn new(
        stats: Arc<Stats>,
        vote_generators: Arc<VoteGenerators>,
        active_elections: Arc<ActiveElections>,
        clock: Arc<SteadyClock>,
    ) -> Self {
        Self {
            stats,
            vote_generators,
            active_elections,
            clock,
            last_votes: Mutex::new(LastSentVotes::default()),
        }
    }

    pub fn try_vote(&self, block_hash: &BlockHash) {
        if let Some(election) = self.active_elections.election_for_block(block_hash) {
            self.try_vote_for_election(&mut election.lock().unwrap());
        }
    }

    /// Broadcasts vote for the current winner of this election
    /// Checks if sufficient amount of time (`vote_generation_interval`) passed since the last vote generation
    pub fn try_vote_for_election(&self, election: &mut Election) {
        if !self.vote_generators.voting_enabled() {
            return;
        }

        let vote_type = election.vote_type();
        let winner_hash = election.winner().hash();

        let last_vote = self.last_votes.lock().unwrap().get(winner_hash, vote_type);

        if !election.can_vote() {
            return;
        }

        self.stats
            .inc(StatType::Election, DetailType::BroadcastVote);

        let winner = election.winner().hash();

        if election.is_final() {
            self.stats
                .inc(StatType::Election, DetailType::GenerateVoteFinal);
            trace!(qualified_root = ?election.qualified_root(), %winner, "type" = "final", "broadcast vote");
            self.vote_generators
                .generate_final_vote(&election.qualified_root().root, &winner);
        // Broadcasts vote to the network
        } else {
            self.stats
                .inc(StatType::Election, DetailType::GenerateVoteNormal);
            trace!(qualified_root = ?election.qualified_root(), %winner, "type" = "normal", "broadcast vote");
            self.vote_generators
                .generate_non_final_vote(&election.qualified_root().root, &winner);
            // Broadcasts vote to the network
        }

        election.voted();
        self.last_votes
            .lock()
            .unwrap()
            .insert(winner_hash, vote_type, self.clock.now());
    }
}
