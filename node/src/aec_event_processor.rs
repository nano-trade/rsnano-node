use std::sync::{mpsc::SyncSender, Arc, Mutex};

use rsnano_core::{
    utils::{BackpressureReceiver, MemoryStream},
    VoteCode, VoteSource,
};
use rsnano_ledger::{Ledger, LedgerSet};
use rsnano_messages::NetworkFilter;
use rsnano_network::ChannelId;
use rsnano_nullable_clock::SteadyClock;
use rsnano_stats::{DetailType, StatType, Stats};

use crate::{
    block_processing::{BlockProcessor, BlockSource},
    cementation::ConfirmingSet,
    consensus::{
        election_schedulers::ElectionSchedulers, ActiveElections, AecCooldownReason, AecEvent,
        BlockVoter, BootstrapElectionActivator, LocalVoteHistory, VoteCache, VoteCacheProcessor,
        VoteProcessor, VoteRebroadcastQueue, VoteType,
    },
    recently_cemented_inserter::RecentlyCementedInserter,
    representatives::{OnlineReps, RepCrawler},
    NodeEvent,
};

/// Processes events from the active election container (AEC)
pub(crate) struct AecEventProcessor {
    pub(crate) receiver: BackpressureReceiver<AecEvent>,
    pub(crate) vote_cache_processor: Arc<VoteCacheProcessor>,
    pub(crate) vote_processor: Arc<VoteProcessor>,
    pub(crate) vote_cache: Arc<Mutex<VoteCache>>,
    pub(crate) node_event_sender: Option<SyncSender<NodeEvent>>,
    pub(crate) election_schedulers: Arc<ElectionSchedulers>,
    pub(crate) network_filter: Arc<NetworkFilter>,
    pub(crate) bootstrap_election_activator: BootstrapElectionActivator,
    pub(crate) block_voter: Arc<BlockVoter>,
    pub(crate) recently_cemented_inserter: RecentlyCementedInserter,
    pub(crate) vote_rebroadcast_queue: Arc<VoteRebroadcastQueue>,
    pub(crate) block_processor: Arc<BlockProcessor>,
    pub(crate) confirming_set: Arc<ConfirmingSet>,
    pub(crate) stats: Arc<Stats>,
    pub(crate) online_reps: Arc<Mutex<OnlineReps>>,
    pub(crate) history: Arc<LocalVoteHistory>,
    pub(crate) active_elections: Arc<ActiveElections>,
    pub(crate) rep_crawler: Arc<RepCrawler>,
    pub(crate) clock: Arc<SteadyClock>,
    pub(crate) ledger: Arc<Ledger>,
}

impl AecEventProcessor {
    pub(crate) fn run(&mut self) {
        let mut previous_cooldown_state = false;

        while let Ok(event) = self.receiver.recv() {
            // Check if we need to cool down the processing to avoid overwhelming the system
            let current_cooldown = self.receiver.should_cool_down();

            if current_cooldown != previous_cooldown_state {
                let queue_len = self.receiver.len();
                self.active_elections
                    .set_cooldown(current_cooldown, AecCooldownReason::AecEventQueueFull);
                self.vote_processor.set_cooldown(current_cooldown);

                if current_cooldown {
                    self.stats
                        .inc(StatType::ActiveElections, DetailType::Cooldown);
                } else {
                    self.stats
                        .inc(StatType::ActiveElections, DetailType::Recovered);
                }

                previous_cooldown_state = current_cooldown;
            }

            match event {
                AecEvent::ElectionStarted(hash) => {
                    self.bootstrap_election_activator.election_started(hash);
                    self.block_voter.try_vote(&hash);
                    self.vote_cache_processor.trigger(hash);
                    if let Some(tx) = &self.node_event_sender {
                        tx.send(NodeEvent::ElectionStarted(hash)).unwrap();
                    }
                }
                AecEvent::ElectionStopped(hash) => {
                    if let Some(tx) = &self.node_event_sender {
                        tx.send(NodeEvent::ElectionStopped(hash)).unwrap();
                    }
                }
                AecEvent::ElectionConfirmed(election) => {
                    // In some edge cases block might get rolled back while the election
                    // is confirming, reprocess it to ensure it's present in the ledger
                    if !self.ledger.any().block_exists(&election.winner.hash()) {
                        self.block_processor.add(
                            election.winner.clone().into(),
                            BlockSource::Election,
                            ChannelId::LOOPBACK,
                        );
                    }

                    self.confirming_set.add(election.clone());
                }
                AecEvent::BlockAddedToElection(hash) => self.vote_cache_processor.trigger(hash),
                AecEvent::BlockDiscarded(block) => {
                    let mut buf = MemoryStream::new();
                    block.serialize_without_block_type(&mut buf);
                    self.network_filter.clear_bytes(buf.as_bytes());
                }
                AecEvent::WinnerChanged(previous_winner, new_winner) => {
                    // Remove votes from election
                    let root = new_winner.root();
                    let list_generated_votes = self.history.votes(&root, &previous_winner, false);
                    self.active_elections.remove_votes(
                        &new_winner.qualified_root(),
                        list_generated_votes.iter().map(|i| &i.voter),
                    );
                    // Clear votes cache
                    self.history.erase(&root);
                    // Roll back the previous winner and add the new winner to the ledger
                    self.block_processor.force(new_winner.clone().into());
                }
                AecEvent::VoteCounted(voter, source) => {
                    if source != VoteSource::Cache {
                        // Representative is defined as online if replying to live votes or rep_crawler queries
                        self.online_reps
                            .lock()
                            .unwrap()
                            .vote_observed(voter, self.clock.now());
                    }

                    self.stats.inc(StatType::Election, DetailType::Vote);
                    self.stats.inc(StatType::ElectionVote, source.into());
                    tracing::trace!(account = %voter, ?source, "vote processed");
                }
                AecEvent::VoteProcessed(vote, voter_weight, source, channel, results) => {
                    // Cache the votes that didn't match any election
                    if source != VoteSource::Cache {
                        self.vote_cache
                            .lock()
                            .unwrap()
                            .insert(&vote, voter_weight, &results);
                    }

                    self.vote_rebroadcast_queue
                        .handle_processed_vote(&vote, &results);

                    // Aggregate results for individual hashes
                    let mut replay = false;
                    let mut processed = false;
                    for (_, vote_code) in results {
                        replay |= vote_code == VoteCode::Replay;
                        processed |= vote_code == VoteCode::Vote;
                    }
                    let result = if replay {
                        VoteCode::Replay
                    } else if processed {
                        VoteCode::Vote
                    } else {
                        VoteCode::Indeterminate
                    };

                    // Ignore republished votes
                    if source == VoteSource::Live {
                        let active_in_rep_crawler =
                            self.rep_crawler.process(&vote, channel.as_ref());
                        if active_in_rep_crawler {
                            // Representative is defined as online if replying to live votes or rep_crawler queries
                            self.online_reps
                                .lock()
                                .unwrap()
                                .vote_observed(vote.voter, self.clock.now());
                        }
                    }

                    if let Some(tx) = &self.node_event_sender {
                        tx.send(NodeEvent::VoteProcessed(vote, result)).unwrap();
                    }
                }
                AecEvent::FinalPhaseStarted(hash, root) => {
                    self.block_voter
                        .try_vote_for_block(hash, root.root, VoteType::Final);
                }
                AecEvent::BlockConfirmed(block, election) => {
                    if let Some(tx) = &self.node_event_sender {
                        tx.send(NodeEvent::BlockConfirmed(block, election.clone()))
                            .unwrap();
                    }
                    self.recently_cemented_inserter.insert(election);
                }
                AecEvent::VacancyUpdated => self.election_schedulers.notify(),
            }
        }
    }
}
