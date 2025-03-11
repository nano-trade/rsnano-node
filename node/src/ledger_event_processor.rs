use std::sync::{mpsc::Receiver, Arc};

use rsnano_ledger::LedgerEvent;

use crate::block_processing::LocalBlockBroadcaster;

pub(crate) struct LedgerEventProcessor {
    pub receiver: Receiver<LedgerEvent>,
    pub local_block_broadcaster: Arc<LocalBlockBroadcaster>,
}

impl LedgerEventProcessor {
    pub fn run(&mut self) {
        while let Ok(event) = self.receiver.recv() {
            match event {
                LedgerEvent::BatchConfirmed(blocks) => {
                    self.local_block_broadcaster.batch_confirmed(&blocks);
                }
            }
        }
    }
}
