use std::{
    collections::HashMap,
    sync::{mpsc::SyncSender, Arc, Mutex, RwLock},
    time::SystemTime,
};

use rsnano_nullable_clock::SteadyClock;

use rsnano_core::{Amount, BlockHash, Vote, VoteCode, VoteSource};
use rsnano_ledger::Ledger;
use rsnano_stats::Stats;

use super::{ActiveElections, AecEvent, BlockVoter, LocalVoteHistory};
use crate::{
    block_processing::BlockProcessor, cementation::ConfirmingSet, consensus::VoteSummary,
    representatives::OnlineReps,
};

/// Applies a vote to an election
pub struct VoteApplier {
    active_elections: Arc<ActiveElections>,
    event_senders: RwLock<Vec<SyncSender<AecEvent>>>,
    ledger: Arc<Ledger>,
    online_reps: Arc<Mutex<OnlineReps>>,
    stats: Arc<Stats>,
    block_processor: Arc<BlockProcessor>,
    history: Arc<LocalVoteHistory>,
    confirming_set: Arc<ConfirmingSet>,
    clock: Arc<SteadyClock>,
    election_voter: Arc<BlockVoter>,
    is_dev_network: bool,
}

impl VoteApplier {
    pub(crate) fn new(
        active_elections: Arc<ActiveElections>,
        ledger: Arc<Ledger>,
        online_reps: Arc<Mutex<OnlineReps>>,
        stats: Arc<Stats>,
        block_processor: Arc<BlockProcessor>,
        history: Arc<LocalVoteHistory>,
        confirming_set: Arc<ConfirmingSet>,
        clock: Arc<SteadyClock>,
        election_voter: Arc<BlockVoter>,
        is_dev_network: bool,
    ) -> Self {
        Self {
            active_elections,
            event_senders: RwLock::new(Vec::new()),
            ledger,
            online_reps,
            stats,
            block_processor,
            history,
            confirming_set,
            clock,
            election_voter,
            is_dev_network,
        }
    }

    pub fn add_event_sink(&self, sink: SyncSender<AecEvent>) {
        self.event_senders.write().unwrap().push(sink);
    }

    pub fn stop(&self) {
        self.event_senders.write().unwrap().clear();
    }

    /// Route vote to associated elections
    /// Distinguishes replay votes, cannot be determined if the block is not in any election
    pub fn vote(&self, vote: &Arc<Vote>, source: VoteSource) -> HashMap<BlockHash, VoteCode> {
        self.vote_filter(vote, source, &BlockHash::zero())
    }

    /// Route vote to associated elections
    /// Distinguishes replay votes, cannot be determined if the block is not in any election
    /// If 'filter' parameter is non-zero, only elections for the specified hash are notified.
    /// This eliminates duplicate processing when triggering votes from the vote_cache as the result of a specific election being created.
    pub fn vote_filter(
        &self,
        vote: &Arc<Vote>,
        source: VoteSource,
        filter: &BlockHash,
    ) -> HashMap<BlockHash, VoteCode> {
        debug_assert!(vote.validate().is_ok());
        // If present, filter should be set to one of the hashes in the vote
        debug_assert!(filter.is_zero() || vote.hashes.iter().any(|h| h == filter));

        let relevant_hashes = vote.hashes.iter().filter(|h| {
            // Ignore votes for other hashes if a filter is set
            if !filter.is_zero() && *h != filter {
                false
            } else {
                true
            }
        });

        let minimum_pr_weight = self.online_reps.lock().unwrap().minimum_principal_weight();
        let rep_weights = self.ledger.rep_weights.read();
        let voter_weight = rep_weights.get(&vote.voter).cloned().unwrap_or_default();

        if !self.is_dev_network && voter_weight <= minimum_pr_weight {
            // Ignore votes from reps below min PR weight!
            return relevant_hashes
                .map(|h| (*h, VoteCode::Indeterminate))
                .collect();
        }

        if source != VoteSource::Cache {
            let is_active = {
                let active = self.active_elections.read();
                vote.hashes.iter().any(|hash| active.is_active_hash(hash))
            };

            if is_active {
                // Representative is defined as online if replying to live votes or rep_crawler queries.
                // The rep weights have to be updated before the votes are processed!
                self.online_reps
                    .lock()
                    .unwrap()
                    .vote_observed(vote.voter, self.clock.now());
            }
        }

        let (online_weight, quorum_delta) = {
            let online_reps = self.online_reps.lock().unwrap();
            (
                online_reps.trended_or_minimum_weight(),
                online_reps.quorum_delta(),
            )
        };
        let sys_now = SystemTime::now();

        let vote_summaries = vote
            .hashes
            .iter()
            .filter(|h| {
                // Ignore votes for other hashes if a filter is set
                if !filter.is_zero() && *h != filter {
                    false
                } else {
                    true
                }
            })
            .map(|hash| VoteSummary {
                voter: vote.voter,
                time: sys_now,
                timestamp: vote.timestamp(),
                hash: *hash,
                weight: voter_weight,
            });

        let mut results = self.active_elections.apply_votes(
            vote_summaries,
            source,
            &rep_weights,
            online_weight,
            quorum_delta,
        );

        // Handle vote application results
        //--------------------------------------------------------------------------------

        for result in &results {
            if let Some((old_winner, new_winner)) = &result.winner_changed {
                // Remove votes from election
                let root = new_winner.root();
                let list_generated_votes = self.history.votes(&root, &old_winner, false);
                self.active_elections.remove_votes(
                    &new_winner.qualified_root(),
                    list_generated_votes.iter().map(|i| &i.voter),
                );
                // Clear votes cache
                self.history.erase(&root);
                // Roll back the previous winner and add the new winner to the ledger
                self.block_processor.force(new_winner.clone().into());
            }
        }

        let results = results
            .drain(..)
            .map(|i| (i.voted_block, i.vote_result))
            .collect();
        self.notify_vote_processed(vote, voter_weight, source, &results);
        results
    }

    fn notify_vote_processed(
        &self,
        vote: &Arc<Vote>,
        voter_weight: Amount,
        source: VoteSource,
        results: &HashMap<BlockHash, VoteCode>,
    ) {
        for sender in self.event_senders.read().unwrap().iter() {
            sender
                .send(AecEvent::VoteProcessed(
                    vote.clone(),
                    voter_weight,
                    source,
                    results.clone(),
                ))
                .unwrap();
        }
    }

    pub fn force_confirm(&self, block_hash: &BlockHash) {
        let confirmed = self
            .active_elections
            .force_confirm(block_hash)
            .expect("no election found for given block");

        self.confirming_set.add(confirmed);
    }
}
