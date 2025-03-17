use std::sync::{mpsc::Receiver, Arc};

use rsnano_ledger::LedgerEvent;

use crate::{
    block_processing::{BoundedBacklog, LocalBlockBroadcaster},
    config::NodeFlags,
    consensus::{election_schedulers::ElectionSchedulers, VoteApplier},
    wallets::{Wallets, WalletsExt},
};

pub(crate) struct LedgerEventProcessor {
    pub receiver: Receiver<LedgerEvent>,
    pub local_block_broadcaster: Arc<LocalBlockBroadcaster>,
    pub vote_applier: Arc<VoteApplier>,
    pub election_schedulers: Arc<ElectionSchedulers>,
    pub bounded_backlog: Arc<BoundedBacklog>,
    pub wallets: Arc<Wallets>,
    pub flags: NodeFlags,
}

impl LedgerEventProcessor {
    pub fn run(&mut self) {
        while let Ok(event) = self.receiver.recv() {
            match event {
                LedgerEvent::BatchCemented(confirmed) => {
                    self.vote_applier.batch_cemented(&confirmed);
                    if !self.flags.disable_activate_successors {
                        self.election_schedulers.batch_cemented(&confirmed);
                    }
                    self.bounded_backlog.batch_cemented(&confirmed);
                    self.local_block_broadcaster.batch_cemented(&confirmed);
                    self.wallets.batch_cemented(&confirmed);
                }
            }
        }
    }
}
