use std::sync::{
    mpsc::{Receiver, SyncSender},
    Arc,
};

use rsnano_core::utils::MemoryStream;
use rsnano_messages::NetworkFilter;

use crate::{
    consensus::{
        election_schedulers::ElectionSchedulers, ActiveElections, AecEvent,
        BootstrapElectionActivator, ElectionVoter, VoteCacheProcessor,
    },
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
    pub election_voter: Arc<ElectionVoter>,
    pub active_elections: Arc<ActiveElections>,
}

impl AecEventProcessor {
    pub(crate) fn run(&mut self) {
        while let Ok(event) = self.receiver.recv() {
            match event {
                AecEvent::ElectionStarted(hash) => {
                    self.bootstrap_election_activator.election_started(hash);
                    if let Some(election) = self.active_elections.election_for_block(&hash) {
                        self.election_voter.try_vote(&mut election.lock().unwrap());
                    }
                    self.vote_cache_processor.trigger(hash);
                    if let Some(tx) = &self.node_event_sender {
                        tx.send(NodeEvent::AecActiveStarted(hash)).unwrap();
                    }
                }
                AecEvent::DuplicateElectionAttempt(hash) => {
                    if let Some(election) = self.active_elections.election_for_block(&hash) {
                        self.election_voter.try_vote(&mut election.lock().unwrap());
                    }
                }
                AecEvent::ElectionDropped(hash) => {
                    if let Some(tx) = &self.node_event_sender {
                        tx.send(NodeEvent::AecActiveStopped(hash)).unwrap();
                    }
                }

                AecEvent::BlockAddedToElection(hash) => self.vote_cache_processor.trigger(hash),
                AecEvent::BlockDiscarded(block) => {
                    let mut buf = MemoryStream::new();
                    block.serialize_without_block_type(&mut buf);
                    self.network_filter.clear_bytes(buf.as_bytes());
                }
                AecEvent::VacancyUpdated => self.election_schedulers.notify(),
            }
        }
    }
}
