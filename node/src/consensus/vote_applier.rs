use std::{
    collections::HashMap,
    sync::{mpsc::SyncSender, Arc, Mutex, MutexGuard, RwLock},
    time::{Duration, SystemTime},
};

use rsnano_nullable_clock::SteadyClock;
use tracing::trace;

use rsnano_core::{
    utils::UnixMillisTimestamp, Amount, BlockHash, MaybeSavedBlock, PublicKey, SavedBlock, Vote,
    VoteCode, VoteSource,
};
use rsnano_ledger::Ledger;
use rsnano_network::ChannelId;
use rsnano_stats::{DetailType, Direction, StatType, Stats};

use super::{
    ActiveElections, CementingElectionsCache, Election, EndedElection, LocalVoteHistory,
    VoteGenerators, VoteSummary,
};
use crate::{
    block_processing::{BlockProcessor, BlockSource},
    cementation::ConfirmingSet,
    config::NetworkParams,
    consensus::ElectionResult,
    representatives::OnlineReps,
    wallets::Wallets,
};

#[derive(Clone)]
pub enum VoteApplierEvent {
    VoteProcessed(Arc<Vote>, VoteSource, HashMap<BlockHash, VoteCode>),
    BlockCemented(SavedBlock, EndedElection, Vec<VoteSummary>),
}

/// Applies a vote to an election
pub struct VoteApplier {
    active_elections: Arc<ActiveElections>,
    event_senders: RwLock<Vec<SyncSender<VoteApplierEvent>>>,
    ledger: Arc<Ledger>,
    network_params: NetworkParams,
    online_reps: Arc<Mutex<OnlineReps>>,
    stats: Arc<Stats>,
    vote_generators: Arc<VoteGenerators>,
    block_processor: Arc<BlockProcessor>,
    history: Arc<LocalVoteHistory>,
    wallets: Arc<Wallets>,
    confirming_set: Arc<ConfirmingSet>,
    clock: Arc<SteadyClock>,
    cementing_elections_cache: Mutex<CementingElectionsCache>,
}

impl VoteApplier {
    pub(crate) fn new(
        active_elections: Arc<ActiveElections>,
        ledger: Arc<Ledger>,
        network_params: NetworkParams,
        online_reps: Arc<Mutex<OnlineReps>>,
        stats: Arc<Stats>,
        vote_generators: Arc<VoteGenerators>,
        block_processor: Arc<BlockProcessor>,
        history: Arc<LocalVoteHistory>,
        wallets: Arc<Wallets>,
        confirming_set: Arc<ConfirmingSet>,
        clock: Arc<SteadyClock>,
    ) -> Self {
        Self {
            active_elections,
            event_senders: RwLock::new(Vec::new()),
            ledger,
            network_params,
            online_reps,
            stats,
            vote_generators,
            block_processor,
            history,
            wallets,
            confirming_set,
            clock,
            cementing_elections_cache: Mutex::new(CementingElectionsCache::default()),
        }
    }

    pub fn add_event_sink(&self, sink: SyncSender<VoteApplierEvent>) {
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

        let mut results = HashMap::new();
        let mut process = HashMap::new();
        {
            let hashes = vote.hashes.iter().filter(|h| {
                // Ignore votes for other hashes if a filter is set
                if !filter.is_zero() && *h != filter {
                    false
                } else {
                    true
                }
            });

            let active = self.active_elections.read();
            for hash in hashes {
                // Ignore duplicate hashes (should not happen with a well-behaved voting node)
                if results.contains_key(hash) {
                    continue;
                }

                let election = active.election_for_block(hash);
                if let Some(election) = election {
                    process.insert(*hash, election.clone());
                } else {
                    if !active.was_recently_confirmed(hash) {
                        results.insert(*hash, VoteCode::Indeterminate);
                    } else {
                        results.insert(*hash, VoteCode::Replay);
                    }
                }
            }
        }

        for (block_hash, election) in process {
            let vote_result = self.apply_vote(
                &election,
                &vote.voter,
                vote.timestamp(),
                &block_hash,
                source,
            );

            results.insert(block_hash, vote_result);
        }

        self.notify_vote_processed(vote, source, &results);

        results
    }

    pub fn force_confirm(&self, block_hash: &BlockHash) {
        let election = self
            .active_elections
            .election_for_block(block_hash)
            .expect("no election found for given block");
        election.lock().unwrap().force_confirm();

        self.election_confirmed(election.clone());
    }

    fn apply_vote(
        &self,
        election_mutex: &Arc<Mutex<Election>>,
        rep: &PublicKey,
        timestamp: UnixMillisTimestamp,
        block_hash: &BlockHash,
        vote_source: VoteSource,
    ) -> VoteCode {
        let weight = self.ledger.weight(rep);
        if !self.network_params.network.is_dev_network()
            && weight <= self.online_reps.lock().unwrap().minimum_principal_weight()
        {
            return VoteCode::Indeterminate;
        }

        let mut election = election_mutex.lock().unwrap();

        if let Some(last_vote) = election.votes().get(rep) {
            if last_vote.timestamp > timestamp {
                return VoteCode::Replay;
            }
            if last_vote.timestamp == timestamp && !(last_vote.hash < *block_hash) {
                return VoteCode::Replay;
            }

            let max_vote = timestamp == UnixMillisTimestamp::MAX && last_vote.timestamp < timestamp;

            let mut past_cooldown = true;
            // Only cooldown live votes
            if vote_source != VoteSource::Cache {
                let cooldown = self.cooldown_time(weight);
                past_cooldown = last_vote.time <= SystemTime::now() - cooldown;
            }

            if !max_vote && !past_cooldown {
                return VoteCode::Ignored;
            }
        }
        election.add_vote(*rep, timestamp, *block_hash);

        if vote_source != VoteSource::Cache {
            // Representative is defined as online if replying to live votes or rep_crawler queries
            self.online_reps
                .lock()
                .unwrap()
                .vote_observed(*rep, self.clock.now());
        }

        self.stats.inc(StatType::Election, DetailType::Vote);
        self.stats.inc(StatType::ElectionVote, vote_source.into());
        tracing::trace!(
            account = %rep,
            hash = %block_hash,
            %timestamp,
            ?vote_source,
            ?weight,
            "vote processed");

        self.confirm_if_quorum(election, election_mutex);
        VoteCode::Vote
    }

    fn confirm_if_quorum(
        &self,
        mut election: MutexGuard<Election>,
        election_mutex: &Arc<Mutex<Election>>,
    ) {
        if election.is_confirmed() {
            return;
        }

        let quorum_delta = self.online_reps.lock().unwrap().quorum_delta();

        let old_winner = election.winner().hash();
        let old_final = election.is_final();

        election.update_tallies(&self.ledger.rep_weights.read(), quorum_delta);

        let winner_changed = election.winner().hash() != old_winner;
        if winner_changed {
            self.remove_votes(&mut election, &old_winner);
            // Roll back the previous winner and add the new winner to the ledger
            self.block_processor.force(election.winner().clone().into());
        }

        if election.is_final() {
            if !old_final && self.wallets.voting_enabled() {
                self.vote_generators.generate_final_vote(
                    &election.qualified_root().root,
                    &election.winner().hash(),
                );
                election.voted();
            }
            if election.is_confirmed() {
                // In some edge cases block might get rolled back while the election
                // is confirming, reprocess it to ensure it's present in the ledger
                self.block_processor.add(
                    election.winner().clone().into(),
                    BlockSource::Election,
                    ChannelId::LOOPBACK,
                );
                drop(election);
                self.election_confirmed(election_mutex.clone());
            }
        }
    }

    fn remove_votes(&self, election: &mut Election, hash: &BlockHash) {
        if self.wallets.voting_enabled() {
            // Remove votes from election
            let root = election.qualified_root().root;
            let list_generated_votes = self.history.votes(&root, hash, false);
            for vote in list_generated_votes {
                election.remove_vote(&vote.voter);
            }
            // Clear votes cache
            self.history.erase(&root);
        }
    }

    /// Calculates minimum time delay between subsequent votes when processing non-final votes
    fn cooldown_time(&self, weight: Amount) -> Duration {
        let online_stake = { self.online_reps.lock().unwrap().trended_or_minimum_weight() };
        if weight > online_stake / 20 {
            // Reps with more than 5% weight
            Duration::from_secs(1)
        } else if weight > online_stake / 100 {
            // Reps with more than 1% weight
            Duration::from_secs(5)
        } else {
            // The rest of smaller reps
            Duration::from_secs(15)
        }
    }

    fn notify_vote_processed(
        &self,
        vote: &Arc<Vote>,
        source: VoteSource,
        results: &HashMap<BlockHash, VoteCode>,
    ) {
        for sender in self.event_senders.read().unwrap().iter() {
            sender
                .send(VoteApplierEvent::VoteProcessed(
                    vote.clone(),
                    source,
                    results.clone(),
                ))
                .unwrap();
        }
    }

    fn notify(&self, event: VoteApplierEvent) {
        for sender in self.event_senders.read().unwrap().iter() {
            sender.send(event.clone()).unwrap();
        }
    }

    fn election_confirmed(&self, election: Arc<Mutex<Election>>) {
        let (winner_hash, root) = {
            let e = election.lock().unwrap();
            (e.winner().hash(), e.qualified_root().clone())
        };

        self.stats.inc(StatType::Election, DetailType::ConfirmOnce);
        trace!( qualified_root = ?root, "election confirmed");

        self.cementing_elections_cache
            .lock()
            .unwrap()
            .insert(election);

        self.confirming_set.add(winner_hash);
    }

    /// Cementing blocks might implicitly confirm dependent elections
    pub fn batch_cemented(&self, cemented: &Vec<(SavedBlock, BlockHash)>) {
        let mut results = Vec::new();

        // Process all cemented blocks while holding the lock to avoid
        // races where an election for a block that is already
        // cemented is inserted
        let active = self.active_elections.read();
        for (block, _) in cemented {
            let election = active.election_for_root(&block.qualified_root());
            let result = self.block_cemented(election, block);
            results.push(result)
        }

        // TODO: This could be offloaded to a separate notification worker, profiling is needed
        for (status, votes) in results {
            self.notify_block_cemented(status, votes);
        }
    }

    /// Distinguishes replay votes, cannot be determined if the block is not in any election
    fn block_cemented(
        &self,
        dependent_election: Option<&Arc<Mutex<Election>>>,
        block: &SavedBlock,
    ) -> (EndedElection, Vec<VoteSummary>) {
        // Dependent elections are implicitly confirmed when their block is cemented
        if let Some(dependent_election) = &dependent_election {
            self.stats
                .inc(StatType::ActiveElections, DetailType::ConfirmDependent);

            // TODO: This should either confirm or cancel the election
            self.try_confirm(&dependent_election, &block.hash());
        }

        let mut election_result = EndedElection::new(block.clone());
        let mut votes = Vec::new();

        let mut handled = false;

        let source_election = self
            .cementing_elections_cache
            .lock()
            .unwrap()
            .get(&block.hash())
            .cloned();

        // Check if the currently cemented block was part of an election that triggered the confirmation
        if let Some(source_election) = source_election {
            let election = source_election.lock().unwrap();
            // TODO compare winner hash instead!
            if *election.qualified_root() == block.qualified_root() {
                election_result.winner = election.winner().clone();
                election_result.tally = election.winner_tally();
                election_result.final_tally = election.winner_final_tally();
                election_result.confirmation_request_count =
                    election.confirmation_request_count() as u32;
                election_result.block_count = election.block_count() as u32;
                election_result.voter_count = election.votes().len() as u32;
                election_result.election_duration = election.start().elapsed(self.clock.now());
                election_result.election_end = SystemTime::now();
                election_result.vote_broadcast_count = election.vote_broadcast_count() as u32;
                election_result.result = ElectionResult::ActiveConfirmedQuorum;
                debug_assert_eq!(election_result.winner.hash(), block.hash());
                votes = election.votes().values().cloned().collect();
                // sort descending
                votes.sort_by(|a, b| b.weight.cmp(&a.weight));
                handled = true;
            }
        }

        if handled {
            // already handled
        } else if dependent_election.is_some() {
            election_result.result = ElectionResult::ActiveConfirmationHeight;
        } else {
            election_result.result = ElectionResult::InactiveConfirmationHeight;
        }

        self.stats
            .inc(StatType::ActiveElections, DetailType::Cemented);
        self.stats.inc(
            StatType::ActiveElectionsCemented,
            election_result.result.into(),
        );

        (election_result, votes)
    }

    fn try_confirm(&self, election_mutex: &Arc<Mutex<Election>>, cemented_hash: &BlockHash) {
        let mut election = election_mutex.lock().unwrap();
        let winner_hash = election.winner().hash();
        if winner_hash == *cemented_hash {
            if election.force_confirm() {
                self.stats.inc(StatType::Election, DetailType::ConfirmOnce);
                trace!(
                    qualified_root = ?election.qualified_root(),
                    "election confirmed"
                );
            }
        }
    }

    fn notify_block_cemented(&self, status: EndedElection, votes: Vec<VoteSummary>) {
        let MaybeSavedBlock::Saved(block) = &status.winner else {
            return;
        };
        let block = block.clone();

        match status.result {
            ElectionResult::ActiveConfirmedQuorum => self.stats.inc_dir(
                StatType::ConfirmationObserver,
                DetailType::ActiveQuorum,
                Direction::Out,
            ),
            ElectionResult::ActiveConfirmationHeight => self.stats.inc_dir(
                StatType::ConfirmationObserver,
                DetailType::ActiveConfHeight,
                Direction::Out,
            ),
            ElectionResult::InactiveConfirmationHeight => self.stats.inc_dir(
                StatType::ConfirmationObserver,
                DetailType::InactiveConfHeight,
                Direction::Out,
            ),
        }

        self.notify(VoteApplierEvent::BlockCemented(block, status, votes));
    }
}
