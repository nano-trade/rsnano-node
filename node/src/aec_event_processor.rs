use std::sync::{
    mpsc::{Receiver, SyncSender},
    Arc,
};

use rsnano_core::utils::MemoryStream;
use rsnano_messages::NetworkFilter;

use crate::{
    consensus::{
        election_schedulers::ElectionSchedulers, AecEvent, BlockVoter, BootstrapElectionActivator,
        VoteCacheProcessor,
    },
    recently_cemented_inserter::RecentlyCementedInserter,
    NodeEvent,
};

/// Processes events from the active election container
pub(crate) struct AecEventProcessor {
    pub receiver: Receiver<AecEvent>,
    pub vote_cache_processor: Arc<VoteCacheProcessor>,
    pub node_event_sender: Option<SyncSender<NodeEvent>>,
    pub election_schedulers: Arc<ElectionSchedulers>,
    pub network_filter: Arc<NetworkFilter>,
    pub bootstrap_election_activator: BootstrapElectionActivator,
    pub block_voter: Arc<BlockVoter>,
    pub(crate) recently_cemented_inserter: RecentlyCementedInserter,
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
                AecEvent::VacancyUpdated => self.election_schedulers.notify(),
            }
        }
    }
}
