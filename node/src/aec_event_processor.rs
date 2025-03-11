use std::sync::{
    mpsc::{Receiver, SyncSender},
    Arc,
};

use crate::{
    consensus::{election_schedulers::ElectionSchedulers, AecEvent, VoteCacheProcessor},
    recently_cemented_inserter::RecentlyCementedInserter,
    NodeEvent,
};

/// Processes events from the active election container
pub(crate) struct AecEventProcessor {
    pub receiver: Receiver<AecEvent>,
    pub vote_cache_processor: Arc<VoteCacheProcessor>,
    pub node_event_sender: Option<SyncSender<NodeEvent>>,
    pub election_schedulers: Arc<ElectionSchedulers>,
    pub recently_cemented_inserter: RecentlyCementedInserter,
}

impl AecEventProcessor {
    pub(crate) fn run(&mut self) {
        while let Ok(event) = self.receiver.recv() {
            match event {
                AecEvent::ActiveStarted(hash) => {
                    self.vote_cache_processor.trigger(hash);
                    if let Some(tx) = &self.node_event_sender {
                        tx.send(NodeEvent::AecActiveStarted(hash)).unwrap();
                    }
                }
                AecEvent::ActiveStopped(hash) => {
                    if let Some(tx) = &self.node_event_sender {
                        tx.send(NodeEvent::AecActiveStopped(hash)).unwrap();
                    }
                }

                AecEvent::BlockCemented(block, status, votes) => {
                    if let Some(tx) = &self.node_event_sender {
                        tx.send(NodeEvent::BlockCemented(block, status.clone(), votes))
                            .unwrap();
                    }
                    self.recently_cemented_inserter.insert(status);
                }
                AecEvent::VacancyUpdated => self.election_schedulers.notify(),
            }
        }
    }
}
