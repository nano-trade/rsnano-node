use std::sync::{mpsc::Receiver, Arc};

use rsnano_core::utils::BackpressureReceiver;
use rsnano_ledger::LedgerEvent;
use rsnano_stats::{DetailType, StatType, Stats};

use crate::{
    block_processing::{BoundedBacklog, LocalBlockBroadcaster},
    cementation::ConfirmingSet,
    config::NodeFlags,
    consensus::{election_schedulers::ElectionSchedulers, DependentElectionsConfirmer},
};

pub(crate) struct LedgerEventProcessor {
    pub receiver: BackpressureReceiver<LedgerEvent>,
    pub local_block_broadcaster: Arc<LocalBlockBroadcaster>,
    pub election_schedulers: Arc<ElectionSchedulers>,
    pub bounded_backlog: Arc<BoundedBacklog>,
    pub confirming_set: Arc<ConfirmingSet>,
    pub stats: Arc<Stats>,
    pub flags: NodeFlags,
    pub(crate) dependent_elections_confirmer: DependentElectionsConfirmer,
}

impl LedgerEventProcessor {
    pub fn run(&mut self) {
        let mut previous_cooldown_state = false;

        while let Ok(event) = self.receiver.recv() {
            // Check if we need to cool down the processing to avoid overwhelming the system
            let current_cooldown = self.receiver.should_cool_down();

            self.confirming_set.set_cooldown(current_cooldown);

            // Log only when the state changes
            if current_cooldown != previous_cooldown_state {
                if current_cooldown {
                    self.stats
                        .inc(StatType::ConfirmingSet, DetailType::Cooldown);
                } else {
                    self.stats
                        .inc(StatType::ConfirmingSet, DetailType::Recovered);
                }

                previous_cooldown_state = current_cooldown;
            }

            match event {
                LedgerEvent::BlocksConfirmed(confirmed) => {
                    self.dependent_elections_confirmer
                        .confirm_dependent_elections(&confirmed);
                    if !self.flags.disable_activate_successors {
                        self.election_schedulers.activate_successors(&confirmed);
                    }
                    self.bounded_backlog.remove(&confirmed);
                    self.local_block_broadcaster.remove(&confirmed);
                }
            }
        }
    }
}
