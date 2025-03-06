use std::sync::{mpsc::Receiver, Arc};

use crate::consensus::{AecEvent, VoteCacheProcessor};

/// Processes events from the active election container
pub(crate) struct AecEventProcessor {
    pub receiver: Receiver<AecEvent>,
    pub vote_cache_processor: Arc<VoteCacheProcessor>,
}

impl AecEventProcessor {
    pub(crate) fn run(&mut self) {
        while let Ok(event) = self.receiver.recv() {
            match event {
                AecEvent::ActiveStarted(hash) => self.vote_cache_processor.trigger(hash),
            }
        }
    }
}
