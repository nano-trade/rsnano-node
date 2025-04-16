use super::{recently_confirmed_cache::RecentlyConfirmedCache, stats::VoteCounter, AecEvent};
use crate::{
    consensus::{
        election::{ConfirmationType, Election, VoteSummary},
        FilteredVote,
    },
    representatives::QuorumSpecs,
};
use rsnano_core::{utils::BackpressureSender, Amount, BlockHash, VoteError, VoteSource};
use rsnano_ledger::RepWeights;
use rsnano_nullable_clock::Timestamp;
use std::ops::Deref;

pub(super) struct ApplyVoteHelper<'a> {
    pub vote: &'a FilteredVote,
    pub recently_confirmed: &'a mut RecentlyConfirmedCache,
    pub vote_counter: &'a mut VoteCounter,
    pub observer: &'a Option<BackpressureSender<AecEvent>>,
    pub rep_weights: &'a RepWeights,
    pub quorum_specs: QuorumSpecs,
    pub now: Timestamp,
}

impl<'a> ApplyVoteHelper<'a> {
    pub fn apply_vote(
        &mut self,
        election: &mut Election,
        block_hash: BlockHash,
    ) -> Result<(), VoteError> {
        let rep_weight = self.rep_weights.weight(&self.vote.voter);

        if let Some(last_vote) = election.votes().get(&self.vote.voter) {
            last_vote.ensure_no_replay(self.vote, &block_hash)?;

            if self.should_cool_down(last_vote, rep_weight) {
                return Err(VoteError::Ignored);
            }
        }

        self.add_vote(election, block_hash);
        Ok(())
    }

    fn should_cool_down(&self, last_vote: &VoteSummary, rep_weight: Amount) -> bool {
        if self.vote.source == VoteSource::Cache {
            // Only cooldown live votes
            return false;
        }

        if last_vote.has_switched_to_final_vote(self.vote) {
            return false;
        }

        let cooldown = self.quorum_specs.cooldown_time(rep_weight);
        last_vote.vote_received.elapsed(self.now) < cooldown
    }

    fn add_vote(&mut self, election: &mut Election, block_hash: BlockHash) {
        election.add_vote(self.vote.voter, block_hash, self.vote.timestamp(), self.now);
        self.vote_counter.count(self.vote.source);
        self.confirm_if_quorum(election);
    }

    pub fn confirm_if_quorum(&mut self, election: &mut Election) {
        if election.is_confirmed() {
            return;
        }

        let old_winner = election.winner().hash();
        let old_final = election.is_final();

        election.update_tallies(self.rep_weights, self.quorum_specs.quorum_delta);

        self.notify_winner_changed(old_winner, election);

        if election.is_final() {
            if !old_final {
                self.final_phase_started(election);
            }

            if election.is_confirmed() {
                self.election_got_confirmed(election);
            }
        }
    }

    fn notify_winner_changed(&mut self, old_winner: BlockHash, election: &Election) {
        let winner_changed = election.winner().hash() != old_winner;
        if winner_changed {
            self.notify(AecEvent::WinnerChanged(
                old_winner,
                election.winner().deref().clone(),
            ));
        }
    }

    fn election_got_confirmed(&mut self, election: &Election) {
        self.insert_recently_confirmed(election);

        let confirmed_election =
            election.into_confirmed_election(self.now, ConfirmationType::ActiveConfirmedQuorum);

        self.notify(AecEvent::ElectionConfirmed(confirmed_election));
    }

    fn insert_recently_confirmed(&mut self, election: &Election) {
        self.recently_confirmed
            .put(election.qualified_root().clone(), election.winner().hash());
    }

    fn final_phase_started(&self, election: &Election) {
        self.notify(AecEvent::FinalPhaseStarted(
            election.winner().hash(),
            election.qualified_root().clone(),
        ));
    }

    fn notify(&self, event: AecEvent) {
        if let Some(o) = self.observer {
            o.send(event).unwrap();
        }
    }
}
