use std::sync::{mpsc::Receiver, Arc};

use rsnano_ledger::LedgerEvent;

use crate::{
    block_processing::LocalBlockBroadcaster,
    config::NodeFlags,
    consensus::{election_schedulers::ElectionSchedulers, ActiveElections},
};

pub(crate) struct LedgerEventProcessor {
    pub receiver: Receiver<LedgerEvent>,
    pub local_block_broadcaster: Arc<LocalBlockBroadcaster>,
    pub active_elections: Arc<ActiveElections>,
    pub election_schedulers: Arc<ElectionSchedulers>,
    pub flags: NodeFlags,
}

impl LedgerEventProcessor {
    pub fn run(&mut self) {
        while let Ok(event) = self.receiver.recv() {
            match event {
                LedgerEvent::BatchConfirmed(confirmed) => {
                    self.active_elections.handle_cementations(&confirmed);
                    if !self.flags.disable_activate_successors {
                        self.election_schedulers.batch_confirmed(&confirmed);
                    }
                    self.local_block_broadcaster.batch_confirmed(&confirmed);
                }
            }
        }
    }
}
