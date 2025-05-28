use super::BlockProcessorQueue;
use crate::{aec_event_processor::AecEventHandler, consensus::AecEvent};
use rsnano_ledger::{BlockSource, Ledger, LedgerSet};
use rsnano_network::ChannelId;
use std::sync::Arc;

/// In some edge cases block might get rolled back while the election
/// is confirming, reprocess it to ensure it's present in the ledger
pub(crate) struct ElectionWinnerReprocessor {
    ledger: Arc<Ledger>,
    block_processor_queue: Arc<BlockProcessorQueue>,
}

impl ElectionWinnerReprocessor {
    pub(crate) fn new(
        ledger: Arc<Ledger>,
        block_processor_queue: Arc<BlockProcessorQueue>,
    ) -> Self {
        Self {
            ledger,
            block_processor_queue,
        }
    }
}

impl AecEventHandler for ElectionWinnerReprocessor {
    fn handle(&mut self, event: &AecEvent) {
        if let AecEvent::ElectionConfirmed(election) = event {
            if !self.ledger.any().block_exists(&election.winner.hash()) {
                self.block_processor_queue.add(
                    election.winner.clone().into(),
                    BlockSource::Election,
                    ChannelId::LOOPBACK,
                );
            }
        }
    }
}
