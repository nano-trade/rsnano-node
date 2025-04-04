use std::sync::{mpsc::SyncSender, Arc, Mutex};

use rsnano_core::{utils::MemoryStream, Block, BlockHash, Vote, VoteCode, VoteSource};
use rsnano_messages::NetworkFilter;
use rsnano_nullable_clock::SteadyClock;

use crate::{
    block_processing::BlockProcessor,
    cementation::ConfirmingSet,
    consensus::{
        aggregate_vote_results, election_schedulers::ElectionSchedulers, ActiveElections,
        AecCooldownReason, AecEvent, BlockVoter, BootstrapElectionActivator, LocalVotesRemover,
        VoteCache, VoteCacheProcessor, VoteProcessor, VoteRebroadcastQueue, VoteType,
    },
    recently_cemented_inserter::RecentlyCementedInserter,
    representatives::{OnlineReps, RepCrawler},
    utils::BackpressureEventProcessor,
    NodeEvent,
};
use rsnano_network::Channel;

/// Processes events from the active election container (AEC)
pub(crate) struct AecEventProcessor {
    pub(crate) vote_cache_processor: Arc<VoteCacheProcessor>,
    pub(crate) vote_processor: Arc<VoteProcessor>,
    pub(crate) vote_cache: Arc<Mutex<VoteCache>>,
    pub(crate) node_observer: Option<SyncSender<NodeEvent>>,
    pub(crate) election_schedulers: Arc<ElectionSchedulers>,
    pub(crate) network_filter: Arc<NetworkFilter>,
    pub(crate) bootstrap_election_activator: BootstrapElectionActivator,
    pub(crate) block_voter: Arc<BlockVoter>,
    pub(crate) recently_cemented_inserter: RecentlyCementedInserter,
    pub(crate) vote_rebroadcast_queue: Arc<VoteRebroadcastQueue>,
    pub(crate) block_processor: Arc<BlockProcessor>,
    pub(crate) confirming_set: Arc<ConfirmingSet>,
    pub(crate) online_reps: Arc<Mutex<OnlineReps>>,
    pub(crate) active_elections: Arc<ActiveElections>,
    pub(crate) rep_crawler: Arc<RepCrawler>,
    pub(crate) clock: Arc<SteadyClock>,
    pub(crate) local_votes_remover: LocalVotesRemover,
}

impl BackpressureEventProcessor<AecEvent> for AecEventProcessor {
    fn cool_down(&mut self) {
        self.active_elections
            .set_cooldown(true, AecCooldownReason::AecEventQueueFull);
        self.vote_processor.cool_down();
    }

    fn recovered(&mut self) {
        self.active_elections
            .set_cooldown(false, AecCooldownReason::AecEventQueueFull);
        self.vote_processor.recovered();
    }

    fn process(&mut self, event: AecEvent) {
        match event {
            AecEvent::ElectionStarted(hash) => {
                self.bootstrap_election_activator.election_started(hash);
                self.block_voter.try_vote(&hash);
                self.vote_cache_processor.trigger(hash);
                if let Some(tx) = &self.node_observer {
                    tx.send(NodeEvent::ElectionStarted(hash)).unwrap();
                }
            }
            AecEvent::ElectionStopped(hash) => {
                if let Some(tx) = &self.node_observer {
                    tx.send(NodeEvent::ElectionStopped(hash)).unwrap();
                }
            }
            AecEvent::ElectionConfirmed(election) => {
                self.block_processor
                    .reprocess_election_winner(&election.winner);
                self.confirming_set.add(election.clone());
            }
            AecEvent::BlockAddedToElection(hash) => self.vote_cache_processor.trigger(hash),
            AecEvent::BlockDiscarded(block) => {
                self.clear_network_filter(block);
            }
            AecEvent::WinnerChanged(previous_winner, new_winner) => {
                self.local_votes_remover
                    .remove_local_votes(&previous_winner, &new_winner.qualified_root());

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
            }
            AecEvent::VoteProcessed(vote, voter_weight, source, channel, results) => {
                // Cache the votes that didn't match any election
                if source != VoteSource::Cache {
                    self.vote_cache
                        .lock()
                        .unwrap()
                        .insert(&vote, voter_weight, &results);
                }

                self.vote_rebroadcast_queue.try_enqueue(&vote, &results);

                let result = aggregate_vote_results(&results);
                self.try_update_online_reps(&vote, result, source, channel);

                if let Some(tx) = &self.node_observer {
                    tx.send(NodeEvent::VoteProcessed(vote, result)).unwrap();
                }
            }
            AecEvent::FinalPhaseStarted(hash, root) => {
                self.block_voter
                    .try_vote_for_block(hash, root.root, VoteType::Final);
            }
            AecEvent::BlockConfirmed(block, election) => {
                if let Some(tx) = &self.node_observer {
                    tx.send(NodeEvent::BlockConfirmed(block, election.clone()))
                        .unwrap();
                }
                self.recently_cemented_inserter.insert(election);
            }
            AecEvent::VacancyUpdated => self.election_schedulers.notify(),
        }
    }
}

impl AecEventProcessor {
    fn clear_network_filter(&mut self, block: Block) {
        let mut buf = MemoryStream::new();
        block.serialize_without_block_type(&mut buf);
        self.network_filter.clear_bytes(buf.as_bytes());
    }

    fn try_update_online_reps(
        &mut self,
        vote: &Arc<Vote>,
        result: VoteCode,
        source: VoteSource,
        channel: Option<Arc<Channel>>,
    ) {
        // Track rep weight voting on live elections
        let mut should_observe = matches!(
            result,
            VoteCode::Vote | VoteCode::Replay | VoteCode::Ignored
        );

        // Ignore republished votes when rep crawling
        if source == VoteSource::Live {
            should_observe |= self.rep_crawler.process(vote, channel.as_ref());
        }

        if should_observe {
            // Representative is defined as online if replying to live votes or rep_crawler queries
            self.online_reps
                .lock()
                .unwrap()
                .vote_observed(vote.voter, self.clock.now());
        }
    }
}
