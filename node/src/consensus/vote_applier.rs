use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use rsnano_nullable_clock::SteadyClock;

use rsnano_core::{utils::BackpressureSender, Amount, BlockHash, VoteCode};
use rsnano_ledger::RepWeightCache;

use super::{ActiveElectionsContainer, AecEvent, FilteredVote, ReceivedVote};
use crate::representatives::OnlineReps;

/// Applies a vote to an election
pub(crate) struct VoteApplier {
    active_elections: Arc<RwLock<ActiveElectionsContainer>>,
    event_senders: RwLock<Vec<BackpressureSender<AecEvent>>>,
    online_reps: Arc<Mutex<OnlineReps>>,
    clock: Arc<SteadyClock>,
    rep_weights: Arc<RepWeightCache>,
    is_dev_network: bool,
}

impl VoteApplier {
    pub(crate) fn new(
        active_elections: Arc<RwLock<ActiveElectionsContainer>>,
        online_reps: Arc<Mutex<OnlineReps>>,
        clock: Arc<SteadyClock>,
        rep_weights: Arc<RepWeightCache>,
        is_dev_network: bool,
    ) -> Self {
        Self {
            active_elections,
            event_senders: RwLock::new(Vec::new()),
            online_reps,
            clock,
            rep_weights,
            is_dev_network,
        }
    }

    pub fn add_event_sink(&self, sink: BackpressureSender<AecEvent>) {
        self.event_senders.write().unwrap().push(sink);
    }

    pub fn stop(&self) {
        self.event_senders.write().unwrap().clear();
    }

    /// Route vote to associated elections
    /// Distinguishes replay votes, cannot be determined if the block is not in any election
    /// If 'filter' parameter is non-zero, only elections for the specified hash are notified.
    /// This eliminates duplicate processing when triggering votes from the vote_cache as the result of a specific election being created.
    pub fn vote(&self, vote: &FilteredVote) -> HashMap<BlockHash, VoteCode> {
        debug_assert!(vote.validate().is_ok());

        let minimum_pr_weight = self.online_reps.lock().unwrap().minimum_principal_weight();
        let voter_weight = self.rep_weights.weight(&vote.voter);

        if !self.is_dev_network && voter_weight <= minimum_pr_weight {
            // Ignore votes from reps below min PR weight!
            return vote
                .filtered_blocks()
                .map(|h| (*h, VoteCode::Indeterminate))
                .collect();
        }

        let is_active = {
            let active = self.active_elections.read().unwrap();
            vote.filtered_blocks()
                .any(|hash| active.is_active_hash(hash))
        };

        let now = self.clock.now();

        let quorum_specs = {
            let mut online = self.online_reps.lock().unwrap();
            if is_active {
                // Representative is defined as online if replying to live votes or rep_crawler queries.
                // The rep weights have to be updated before the votes are processed!
                online.vote_observed(vote.voter, now);
            }
            online.quorum_specs()
        };

        let results = {
            let rep_weights = self.rep_weights.read();
            let mut active = self.active_elections.write().unwrap();
            active.apply_vote(vote, &rep_weights, quorum_specs, now)
        };

        self.notify_vote_processed(&vote, voter_weight, &results);
        results
    }

    fn notify_vote_processed(
        &self,
        vote: &ReceivedVote,
        voter_weight: Amount,
        results: &HashMap<BlockHash, VoteCode>,
    ) {
        for sender in self.event_senders.read().unwrap().iter() {
            sender
                .send(AecEvent::VoteProcessed(
                    vote.clone(),
                    voter_weight,
                    results.clone(),
                ))
                .unwrap();
        }
    }
}
