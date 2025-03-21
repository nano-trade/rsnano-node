use std::sync::{mpsc::Receiver, Arc};

use rsnano_ledger::LedgerEvent;

use crate::{
    block_processing::{BoundedBacklog, LocalBlockBroadcaster},
    config::NodeFlags,
    consensus::{election_schedulers::ElectionSchedulers, DependentElectionsConfirmer},
    wallets::{Wallets, WalletsExt},
};

pub(crate) struct LedgerEventProcessor {
    pub receiver: Receiver<LedgerEvent>,
    pub local_block_broadcaster: Arc<LocalBlockBroadcaster>,
    pub election_schedulers: Arc<ElectionSchedulers>,
    pub bounded_backlog: Arc<BoundedBacklog>,
    pub wallets: Arc<Wallets>,
    pub flags: NodeFlags,
    pub(crate) dependent_elections_confirmer: DependentElectionsConfirmer,
}

impl LedgerEventProcessor {
    pub fn run(&mut self) {
        while let Ok(event) = self.receiver.recv() {
            match event {
                LedgerEvent::BlocksConfirmed(confirmed) => {
                    self.dependent_elections_confirmer
                        .confirm_dependent_elections(&confirmed);
                    if !self.flags.disable_activate_successors {
                        self.election_schedulers.activate_successors(&confirmed);
                    }
                    self.bounded_backlog.remove(&confirmed);
                    self.local_block_broadcaster.remove(&confirmed);
                    self.wallets.try_receive(&confirmed);
                }
            }
        }
    }
}
