use std::sync::{
    mpsc::{Receiver, SyncSender},
    Arc,
};

use crate::{
    consensus::{AecEvent, VoteCacheProcessor},
    NodeEvent,
};

/// Processes events from the active election container
pub(crate) struct AecEventProcessor {
    pub receiver: Receiver<AecEvent>,
    pub vote_cache_processor: Arc<VoteCacheProcessor>,
    pub(crate) node_event_sender: Option<SyncSender<NodeEvent>>,
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
            }
        }
    }
}
