use super::{
    recently_confirmed_cache::RecentlyConfirmedCache, root_container::RootContainer,
    stats::VoteCounter, AecEvent, ApplyVoteArgs,
};
use crate::consensus::election::{ConfirmationType, Election, VoteSummary};
use rsnano_core::{utils::BackpressureSender, Amount, BlockHash, VoteError, VoteSource};
use std::{collections::HashMap, ops::Deref};

pub(super) struct ApplyVoteHelper<'a> {
    pub args: &'a ApplyVoteArgs<'a>,
    pub recently_confirmed: &'a mut RecentlyConfirmedCache,
    pub vote_counter: &'a mut VoteCounter,
    pub observer: &'a Option<BackpressureSender<AecEvent>>,
    pub roots: &'a mut RootContainer,
}

impl<'a> ApplyVoteHelper<'a> {
    pub fn apply_vote(&mut self) -> HashMap<BlockHash, Result<(), VoteError>> {
        let mut results = HashMap::new();
        for block_hash in self.args.vote.filtered_blocks() {
            // Ignore duplicate hashes (should not happen with a well-behaved voting node)
            if results.contains_key(block_hash) {
                continue;
            }

            if let Some(election) = self.roots.election_for_block_mut(block_hash) {
                let mut apply_to_election = ApplyVoteToElectionHelper {
                    args: self.args,
                    recently_confirmed: self.recently_confirmed,
                    vote_counter: self.vote_counter,
                    observer: self.observer,
                    election,
                    block_hash,
                };
                let vote_result = apply_to_election.apply_vote();
                results.insert(*block_hash, vote_result);
            } else {
                if self.recently_confirmed.hash_exists(block_hash) {
                    results.insert(*block_hash, Err(VoteError::Late));
                } else {
                    results.insert(*block_hash, Err(VoteError::Indeterminate));
                }
            }
        }

        results
    }
}

pub(super) struct ApplyVoteToElectionHelper<'a> {
    pub args: &'a ApplyVoteArgs<'a>,
    pub recently_confirmed: &'a mut RecentlyConfirmedCache,
    pub vote_counter: &'a mut VoteCounter,
    pub observer: &'a Option<BackpressureSender<AecEvent>>,
    pub election: &'a mut Election,
    pub block_hash: &'a BlockHash,
}

impl<'a> ApplyVoteToElectionHelper<'a> {
    pub fn apply_vote(&mut self) -> Result<(), VoteError> {
        let rep_weight = self.args.rep_weights.weight(&self.args.vote.voter);

        if let Some(last_vote) = self.election.votes().get(&self.args.vote.voter) {
            last_vote.ensure_no_replay(self.args.vote, self.block_hash)?;

            if self.should_cool_down(last_vote, rep_weight) {
                return Err(VoteError::Ignored);
            }
        }

        self.add_vote();
        Ok(())
    }

    fn should_cool_down(&self, last_vote: &VoteSummary, rep_weight: Amount) -> bool {
        if self.args.vote.source == VoteSource::Cache {
            // Only cooldown live votes
            return false;
        }

        if last_vote.has_switched_to_final_vote(self.args.vote) {
            return false;
        }

        let cooldown = self.args.quorum_specs.cooldown_time(rep_weight);
        last_vote.vote_received.elapsed(self.args.now) < cooldown
    }

    fn add_vote(&mut self) {
        self.election.add_vote(
            self.args.vote.voter,
            *self.block_hash,
            self.args.vote.timestamp(),
            self.args.now,
        );
        self.vote_counter.count(self.args.vote.source);
        self.confirm_if_quorum();
    }

    pub fn confirm_if_quorum(&mut self) {
        if self.election.is_confirmed() {
            return;
        }

        let old_winner = self.election.winner().hash();
        let old_final = self.election.is_final();

        self.election
            .update_tallies(self.args.rep_weights, self.args.quorum_specs.quorum_delta);

        self.notify_winner_changed(old_winner);

        if self.election.is_final() {
            if !old_final {
                self.final_phase_started();
            }

            if self.election.is_confirmed() {
                self.election_got_confirmed();
            }
        }
    }

    fn notify_winner_changed(&mut self, old_winner: BlockHash) {
        let winner_changed = self.election.winner().hash() != old_winner;
        if winner_changed {
            self.notify(AecEvent::WinnerChanged(
                old_winner,
                self.election.winner().deref().clone(),
            ));
        }
    }

    fn election_got_confirmed(&mut self) {
        self.insert_recently_confirmed();

        let confirmed_election = self
            .election
            .into_confirmed_election(self.args.now, ConfirmationType::ActiveConfirmedQuorum);

        self.notify(AecEvent::ElectionConfirmed(confirmed_election));
    }

    fn insert_recently_confirmed(&mut self) {
        self.recently_confirmed.put(
            self.election.qualified_root().clone(),
            self.election.winner().hash(),
        );
    }

    fn final_phase_started(&self) {
        self.notify(AecEvent::FinalPhaseStarted(
            self.election.winner().hash(),
            self.election.qualified_root().clone(),
        ));
    }

    fn notify(&self, event: AecEvent) {
        if let Some(o) = self.observer {
            o.send(event).unwrap();
        }
    }
}
