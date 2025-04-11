use super::{recently_confirmed_cache::RecentlyConfirmedCache, stats::VoteCounter, AecEvent};
use crate::{
    consensus::election::{ConfirmationType, Election},
    representatives::QuorumSpecs,
};
use rsnano_core::{
    utils::{BackpressureSender, UnixMillisTimestamp},
    Amount, BlockHash, PublicKey, Vote, VoteCode, VoteSource,
};
use rsnano_nullable_clock::Timestamp;
use std::{collections::HashMap, ops::Deref, time::SystemTime};

pub(super) struct ApplyVoteHelper<'a> {
    pub election: &'a mut Election,
    pub recently_confirmed: &'a mut RecentlyConfirmedCache,
    pub observer: &'a Option<BackpressureSender<AecEvent>>,
    pub now: Timestamp,
    pub rep_weights: &'a HashMap<PublicKey, Amount>,
    pub quorum_specs: QuorumSpecs,
    pub vote_counter: &'a mut VoteCounter,
    pub vote_counted: &'a mut bool,
}

impl<'a> ApplyVoteHelper<'a> {
    pub fn apply_vote(
        &mut self,
        vote: &Vote,
        block_hash: BlockHash,
        source: VoteSource,
    ) -> VoteCode {
        let rep_weight = self
            .rep_weights
            .get(&vote.voter)
            .cloned()
            .unwrap_or_default();

        if let Some(last_vote) = self.election.votes().get(&vote.voter) {
            if last_vote.timestamp > vote.timestamp() {
                return VoteCode::Replay;
            } else if last_vote.timestamp == vote.timestamp() && !(last_vote.hash < block_hash) {
                return VoteCode::Replay;
            }

            let max_vote = vote.timestamp() == UnixMillisTimestamp::MAX
                && last_vote.timestamp < vote.timestamp();

            let mut past_cooldown = true;
            // Only cooldown live votes
            if source != VoteSource::Cache {
                let cooldown = self.quorum_specs.cooldown_time(rep_weight);
                past_cooldown = last_vote.time <= SystemTime::now() - cooldown;
            }

            if !max_vote && !past_cooldown {
                return VoteCode::Ignored;
            }
        }

        self.add_vote(vote, block_hash, source)
    }

    fn add_vote(&mut self, vote: &Vote, block_hash: BlockHash, source: VoteSource) -> VoteCode {
        self.election
            .add_vote(vote.voter, vote.timestamp(), block_hash);

        self.vote_counter.count(source);

        if !*self.vote_counted {
            // send vote counted event only once!
            *self.vote_counted = true;
            self.notify(AecEvent::VoteCounted(vote.voter, source));
        }

        self.confirm_if_quorum();
        VoteCode::Vote
    }

    pub fn confirm_if_quorum(&mut self) {
        if self.election.is_confirmed() {
            return;
        }

        let old_winner = self.election.winner().hash();
        let old_final = self.election.is_final();

        self.election
            .update_tallies(self.rep_weights, self.quorum_specs.quorum_delta);

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
            .into_confirmed_election(self.now, ConfirmationType::ActiveConfirmedQuorum);

        self.notify(AecEvent::ElectionConfirmed(confirmed_election));
    }

    fn insert_recently_confirmed(&mut self) {
        self.recently_confirmed.put(
            self.election.qualified_root().clone(),
            self.election.winner().hash(),
        );
    }

    fn final_phase_started(&mut self) {
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
