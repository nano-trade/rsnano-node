use std::sync::Arc;

use rsnano_ledger::Election;
use rsnano_stats::{DetailType, StatType, Stats};
use tracing::trace;

use crate::consensus::VoteApplier;

use super::VoteGenerators;

/// Tries to generate a vote for a given election
pub(crate) struct ElectionVoter {
    pub stats: Arc<Stats>,
    pub vote_applier: Arc<VoteApplier>,
    pub vote_generators: Arc<VoteGenerators>,
}

impl ElectionVoter {
    /// Broadcasts vote for the current winner of this election
    /// Checks if sufficient amount of time (`vote_generation_interval`) passed since the last vote generation
    pub fn try_vote(&self, election: &mut Election) {
        if !election.should_vote() {
            return;
        }

        if self.vote_generators.voting_enabled() {
            self.stats
                .inc(StatType::Election, DetailType::BroadcastVote);
            election.vote_broadcasted();

            if election.is_confirmed() || self.vote_applier.have_quorum(&election.tallies()) {
                self.stats
                    .inc(StatType::Election, DetailType::GenerateVoteFinal);
                let winner = election.winner_hash();
                trace!(qualified_root = ?election.qualified_root(), %winner, "type" = "final", "broadcast vote");
                self.vote_generators
                    .generate_final_vote(election.root(), &winner); // Broadcasts vote to the network
            } else {
                self.stats
                    .inc(StatType::Election, DetailType::GenerateVoteNormal);
                let winner = election.winner_hash();
                trace!(qualified_root = ?election.qualified_root(), %winner, "type" = "normal", "broadcast vote");
                self.vote_generators
                    .generate_non_final_vote(election.root(), &winner); // Broadcasts vote to the network
            }
        }
        election.set_last_vote();
    }
}
