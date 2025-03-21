use std::sync::{
    mpsc::{Receiver, SyncSender},
    Arc, Mutex,
};

use rsnano_core::{utils::MemoryStream, VoteSource};
use rsnano_ledger::RepWeightCache;
use rsnano_messages::NetworkFilter;

use crate::{
    consensus::{
        election_schedulers::ElectionSchedulers, AecEvent, BlockVoter, BootstrapElectionActivator,
        VoteCache, VoteCacheProcessor, VoteRebroadcastQueue,
    },
    recently_cemented_inserter::RecentlyCementedInserter,
    NodeEvent,
};

/// Processes events from the active election container
pub(crate) struct AecEventProcessor {
    pub receiver: Receiver<AecEvent>,
    pub vote_cache_processor: Arc<VoteCacheProcessor>,
    pub vote_cache: Arc<Mutex<VoteCache>>,
    pub node_event_sender: Option<SyncSender<NodeEvent>>,
    pub election_schedulers: Arc<ElectionSchedulers>,
    pub network_filter: Arc<NetworkFilter>,
    pub bootstrap_election_activator: BootstrapElectionActivator,
    pub block_voter: Arc<BlockVoter>,
    pub(crate) recently_cemented_inserter: RecentlyCementedInserter,
    pub rep_weights: Arc<RepWeightCache>,
    pub(crate) vote_rebroadcast_queue: Arc<VoteRebroadcastQueue>,
}

impl AecEventProcessor {
    pub(crate) fn run(&mut self) {
        while let Ok(event) = self.receiver.recv() {
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

                AecEvent::BlockAddedToElection(hash) => self.vote_cache_processor.trigger(hash),
                AecEvent::BlockDiscarded(block) => {
                    let mut buf = MemoryStream::new();
                    block.serialize_without_block_type(&mut buf);
                    self.network_filter.clear_bytes(buf.as_bytes());
                }
                AecEvent::BlockCemented(block, election) => {
                    if let Some(tx) = &self.node_event_sender {
                        tx.send(NodeEvent::BlockCemented(block, election.clone()))
                            .unwrap();
                    }
                    self.recently_cemented_inserter.insert(election);
                }
                AecEvent::VoteProcessed(vote, source, results) => {
                    // Cache the votes that didn't match any election
                    if source != VoteSource::Cache {
                        let rep_weight = self.rep_weights.weight(&vote.voter);
                        self.vote_cache
                            .lock()
                            .unwrap()
                            .insert(&vote, rep_weight, &results);
                    }

                    self.vote_rebroadcast_queue
                        .handle_processed_vote(&vote, &results);
                }
                AecEvent::VacancyUpdated => self.election_schedulers.notify(),
            }
        }
    }
}
