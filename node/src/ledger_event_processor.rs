use std::sync::mpsc::Receiver;

use rsnano_ledger::LedgerEvent;

pub(crate) struct LedgerEventProcessor {
    pub receiver: Receiver<LedgerEvent>,
}

impl LedgerEventProcessor {
    pub fn run(&mut self) {
        while let Ok(event) = self.receiver.recv() {
            match event {
                LedgerEvent::BatchConfirmed(blocks) => {}
            }
        }
    }
}
