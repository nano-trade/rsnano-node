use std::sync::{
    mpsc::{Receiver, SyncSender},
    Arc,
};

use crate::{
    consensus::{election_schedulers::ElectionSchedulers, AecEvent, VoteCacheProcessor},
    NodeEvent,
};

/// Processes events from the active election container
pub(crate) struct AecEventProcessor {
    pub receiver: Receiver<AecEvent>,
    pub vote_cache_processor: Arc<VoteCacheProcessor>,
    pub node_event_sender: Option<SyncSender<NodeEvent>>,
    pub election_schedulers: Arc<ElectionSchedulers>,
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

                AecEvent::ElectionEnded(status, votes, block) => {
                    if let Some(tx) = &self.node_event_sender {
                        tx.send(NodeEvent::ElectionEnded(status, votes, block))
                            .unwrap();
                    }
                }
                AecEvent::VacancyUpdated => self.election_schedulers.notify(),
            }
        }
    }
}
