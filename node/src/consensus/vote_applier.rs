use std::{
    collections::{HashMap, HashSet},
    sync::{mpsc::SyncSender, Arc, Mutex, RwLock},
    time::SystemTime,
};

use rsnano_nullable_clock::SteadyClock;

use rsnano_core::{
    utils::UnixMillisTimestamp, BlockHash, MaybeSavedBlock, SavedBlock, Vote, VoteCode, VoteSource,
};
use rsnano_ledger::Ledger;
use rsnano_network::ChannelId;
use rsnano_stats::{DetailType, Direction, StatType, Stats};

use super::{
    ActiveElections, BlockVoter, CementingElectionsCache, Election, EndedElection, LocalVoteHistory,
};
use crate::{
    block_processing::{BlockProcessor, BlockSource},
    cementation::ConfirmingSet,
    config::NetworkParams,
    consensus::{ActiveElectionsContainer, ApplyVoteResult, ElectionResult, VoteSummary, VoteType},
    representatives::OnlineReps,
};

#[derive(Clone)]
pub enum VoteApplierEvent {
    VoteProcessed(Arc<Vote>, VoteSource, HashMap<BlockHash, VoteCode>),
    BlockCemented(SavedBlock, EndedElection),
}

/// Applies a vote to an election
pub struct VoteApplier {
    active_elections: Arc<ActiveElections>,
    event_senders: RwLock<Vec<SyncSender<VoteApplierEvent>>>,
    ledger: Arc<Ledger>,
    network_params: NetworkParams,
    online_reps: Arc<Mutex<OnlineReps>>,
    stats: Arc<Stats>,
    block_processor: Arc<BlockProcessor>,
    history: Arc<LocalVoteHistory>,
    confirming_set: Arc<ConfirmingSet>,
    clock: Arc<SteadyClock>,
    election_voter: Arc<BlockVoter>,
    cementing_elections_cache: Mutex<CementingElectionsCache>,
}

impl VoteApplier {
    pub(crate) fn new(
        active_elections: Arc<ActiveElections>,
        ledger: Arc<Ledger>,
        network_params: NetworkParams,
        online_reps: Arc<Mutex<OnlineReps>>,
        stats: Arc<Stats>,
        block_processor: Arc<BlockProcessor>,
        history: Arc<LocalVoteHistory>,
        confirming_set: Arc<ConfirmingSet>,
        clock: Arc<SteadyClock>,
        election_voter: Arc<BlockVoter>,
    ) -> Self {
        Self {
            active_elections,
            event_senders: RwLock::new(Vec::new()),
            ledger,
            network_params,
            online_reps,
            stats,
            block_processor,
            history,
            confirming_set,
            clock,
            election_voter,
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

        let rep_weights = self.ledger.rep_weights.read();
        let (minimum_principal_weight, online_weight, quorum_delta) = {
            let online_reps = self.online_reps.lock().unwrap();
            (
                online_reps.minimum_principal_weight(),
                online_reps.trended_or_minimum_weight(),
                online_reps.quorum_delta(),
            )
        };
        let now = self.clock.now();
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
                weight: rep_weights.get(&vote.voter).cloned().unwrap_or_default(),
            });

        //--------------------------------------------------------------------------------
        let mut results = Vec::new();
        let mut process = Vec::new();
        {
            let mut processed_hashes = HashSet::new();

            let active = self.active_elections.read();
            for vote_summary in vote_summaries {
                // Ignore duplicate hashes (should not happen with a well-behaved voting node)
                if !processed_hashes.insert(vote_summary.hash) {
                    continue;
                }

                let election = active.election_for_block(&vote_summary.hash);
                if let Some(election) = election {
                    process.push((vote_summary, election.clone()));
                } else {
                    if !active.was_recently_confirmed(&vote_summary.hash) {
                        results.push(ApplyVoteResult::new(
                            vote_summary.hash,
                            VoteCode::Indeterminate,
                        ));
                    } else {
                        results.push(ApplyVoteResult::new(vote_summary.hash, VoteCode::Replay));
                    }
                }
            }
        }

        for (vote_summary, election_mutex) in process {
            let mut result = VoteCode::Invalid;
            let mut ended_election = None;
            let mut final_vote = None;
            let mut change_winner = None;
            let rep_weight = rep_weights.get(&vote.voter).cloned().unwrap_or_default();
            if !self.network_params.network.is_dev_network()
                && rep_weight <= minimum_principal_weight
            {
                result = VoteCode::Indeterminate;
            }

            if result == VoteCode::Invalid {
                let mut election = election_mutex.lock().unwrap();

                if let Some(last_vote) = election.votes().get(&vote.voter) {
                    if last_vote.timestamp > vote_summary.timestamp {
                        result = VoteCode::Replay;
                    } else if last_vote.timestamp == vote_summary.timestamp
                        && !(last_vote.hash < vote_summary.hash)
                    {
                        result = VoteCode::Replay;
                    }

                    if result == VoteCode::Invalid {
                        let max_vote = vote_summary.timestamp == UnixMillisTimestamp::MAX
                            && last_vote.timestamp < vote_summary.timestamp;

                        let mut past_cooldown = true;
                        // Only cooldown live votes
                        if source != VoteSource::Cache {
                            let cooldown =
                                ActiveElectionsContainer::cooldown_time(rep_weight, online_weight);
                            past_cooldown = last_vote.time <= SystemTime::now() - cooldown;
                        }

                        if !max_vote && !past_cooldown {
                            result = VoteCode::Ignored;
                        }
                    }
                }

                if result == VoteCode::Invalid {
                    election.add_vote(vote.voter, vote_summary.timestamp, vote_summary.hash);

                    // CONFIRM IF QUORUM:
                    if !election.is_confirmed() {
                        let old_winner = election.winner().hash();
                        let old_final = election.is_final();

                        election.update_tallies(&rep_weights, quorum_delta);

                        let winner_changed = election.winner().hash() != old_winner;
                        if winner_changed {
                            change_winner = Some((old_winner, election.winner().clone()));
                        }

                        if election.is_final() {
                            if !old_final {
                                final_vote = Some(election.winner().clone());
                            }
                            if election.is_confirmed() {
                                ended_election = Some(election.into_ended_election(
                                    now,
                                    ElectionResult::ActiveConfirmedQuorum,
                                ));
                            }
                        }
                    }
                    result = VoteCode::Vote;
                }
            }

            results.push(ApplyVoteResult {
                voted_block: vote_summary.hash,
                vote_result: result,
                got_confirmed: ended_election,
                final_phase_started: final_vote,
                winner_changed: change_winner,
            });
        }

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

            if result.vote_result == VoteCode::Vote {
                if source != VoteSource::Cache {
                    // Representative is defined as online if replying to live votes or rep_crawler queries
                    self.online_reps
                        .lock()
                        .unwrap()
                        .vote_observed(vote.voter, now);
                }

                self.stats.inc(StatType::Election, DetailType::Vote);
                self.stats.inc(StatType::ElectionVote, source.into());
                tracing::trace!(account = %vote.voter, hash=%result.voted_block, ?source, "vote processed");

                if let Some(winner) = &result.final_phase_started {
                    self.election_voter.try_vote_for_block(
                        winner.hash(),
                        winner.root(),
                        VoteType::Final,
                    );
                }
            }

            if let Some(election) = &result.got_confirmed {
                // In some edge cases block might get rolled back while the election
                // is confirming, reprocess it to ensure it's present in the ledger
                self.block_processor.add(
                    election.winner.clone().into(),
                    BlockSource::Election,
                    ChannelId::LOOPBACK,
                );

                self.cementing_elections_cache
                    .lock()
                    .unwrap()
                    .insert(election.clone());

                self.confirming_set.add(election.winner.hash());
            }
        }

        let results = results
            .drain(..)
            .map(|i| (i.voted_block, i.vote_result))
            .collect();
        self.notify_vote_processed(vote, source, &results);
        results
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

    pub fn force_confirm(&self, block_hash: &BlockHash) {
        let election = self
            .active_elections
            .election_for_block(block_hash)
            .expect("no election found for given block");
        election.lock().unwrap().force_confirm();

        self.election_confirmed(election.clone());
    }

    fn election_confirmed(&self, election: Arc<Mutex<Election>>) {
        let ended_election = election
            .lock()
            .unwrap()
            .into_ended_election(self.clock.now(), ElectionResult::ActiveConfirmedQuorum);

        let winner_hash = ended_election.winner.hash();

        self.cementing_elections_cache
            .lock()
            .unwrap()
            .insert(ended_election);

        self.confirming_set.add(winner_hash);
    }

    /// Cementing blocks might implicitly confirm dependent elections
    pub fn batch_cemented(&self, cemented: &Vec<(SavedBlock, BlockHash)>) {
        let mut cemented_blocks_with_election = Vec::with_capacity(cemented.len());
        {
            let cementing_cache = self.cementing_elections_cache.lock().unwrap();
            for (cemented_block, _) in cemented {
                let source_election = cementing_cache.get(&cemented_block.hash()).cloned();
                cemented_blocks_with_election.push((cemented_block.clone(), source_election));
            }
        }

        let results = self
            .active_elections
            .batch_cemented(cemented_blocks_with_election);

        // TODO: This could be offloaded to a separate notification worker, profiling is needed
        for ended_election in results {
            self.stats
                .inc(StatType::ActiveElections, DetailType::Cemented);
            self.stats.inc(
                StatType::ActiveElectionsCemented,
                ended_election.result.into(),
            );
            self.notify_block_cemented(ended_election);
        }
    }

    fn notify_block_cemented(&self, ended_election: EndedElection) {
        let MaybeSavedBlock::Saved(block) = &ended_election.winner else {
            return;
        };
        let block = block.clone();

        match ended_election.result {
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

        self.notify(VoteApplierEvent::BlockCemented(block, ended_election));
    }
}
