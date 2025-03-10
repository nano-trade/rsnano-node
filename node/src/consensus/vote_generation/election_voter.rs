use std::sync::Arc;

use rsnano_stats::{DetailType, StatType, Stats};
use tracing::trace;

use crate::{
    config::NodeConfig,
    consensus::{Election, VoteApplier},
    wallets::Wallets,
};

use super::VoteGenerators;

/// Tries to generate a vote for a given election
pub(crate) struct ElectionVoter {
    pub stats: Arc<Stats>,
    pub node_config: NodeConfig,
    pub wallets: Arc<Wallets>,
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

        if self.node_config.enable_voting && self.wallets.voting_reps_count() > 0 {
            self.stats
                .inc(StatType::Election, DetailType::BroadcastVote);
            election.status.vote_broadcast_count += 1;

            if election.is_confirmed()
                || self
                    .vote_applier
                    .have_quorum(&self.vote_applier.tally_impl(election))
            {
                self.stats
                    .inc(StatType::Election, DetailType::GenerateVoteFinal);
                let winner = election.winner_hash().unwrap();
                trace!(qualified_root = ?election.qualified_root(), %winner, "type" = "final", "broadcast vote");
                self.vote_generators
                    .generate_final_vote(election.root(), &winner); // Broadcasts vote to the network
            } else {
                self.stats
                    .inc(StatType::Election, DetailType::GenerateVoteNormal);
                let winner = election.winner_hash().unwrap();
                trace!(qualified_root = ?election.qualified_root(), %winner, "type" = "normal", "broadcast vote");
                self.vote_generators
                    .generate_non_final_vote(election.root(), &winner); // Broadcasts vote to the network
            }
        }
        election.set_last_vote();
    }
}
