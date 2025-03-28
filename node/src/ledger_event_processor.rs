use std::sync::Arc;

use rsnano_core::utils::BackpressureReceiver;
use rsnano_ledger::LedgerEvent;
use rsnano_stats::{DetailType, StatType, Stats};

use crate::{
    block_processing::{BoundedBacklog, LocalBlockBroadcaster},
    bootstrap::Bootstrapper,
    cementation::ConfirmingSet,
    config::NodeFlags,
    consensus::{
        election_schedulers::ElectionSchedulers, ActiveElections, DependentElectionsConfirmer,
        LocalVoteHistory,
    },
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
    pub(crate) bootstrapper: Arc<Bootstrapper>,
    pub(crate) vote_history: Arc<LocalVoteHistory>,
    pub(crate) active_elections: Arc<ActiveElections>,
}

impl LedgerEventProcessor {
    pub fn run(&mut self) {
        let mut previous_cooldown_state = false;

        while let Ok(event) = self.receiver.recv() {
            // Check if we need to cool down the processing to avoid overwhelming the system
            let should_cool_down = self.receiver.should_cool_down();

            if should_cool_down != previous_cooldown_state {
                self.confirming_set.set_cooldown(should_cool_down);
                if should_cool_down {
                    self.stats
                        .inc(StatType::ConfirmingSet, DetailType::Cooldown);
                } else {
                    self.stats
                        .inc(StatType::ConfirmingSet, DetailType::Recovered);
                }

                previous_cooldown_state = should_cool_down;
            }

            match event {
                LedgerEvent::BlocksProcessed(results) => {
                    self.confirming_set.requeue_blocks(&results);
                    self.bootstrapper.inspect_blocks(&results);
                    self.local_block_broadcaster.blocks_processed(&results);
                }
                LedgerEvent::BlocksConfirmed(confirmed) => {
                    self.dependent_elections_confirmer
                        .confirm_dependent_elections(&confirmed);
                    if !self.flags.disable_activate_successors {
                        self.election_schedulers.activate_successors(&confirmed);
                    }
                    self.bounded_backlog.remove(&confirmed);
                    self.local_block_broadcaster
                        .confirmed(confirmed.iter().map(|i| i.1));
                }
                LedgerEvent::BlocksRolledBack(rolled_back) => {
                    self.active_elections.rolled_back(&rolled_back);

                    // Unblock rolled back accounts as the dependency is no longer valid
                    self.bounded_backlog.erase_hashes(rolled_back.hashes());

                    self.vote_history.erase_batch(rolled_back.roots());

                    self.bootstrapper
                        .unblock_batch(rolled_back.affected_accounts());

                    self.local_block_broadcaster
                        .rolled_back(rolled_back.hashes());
                }
            }
        }
    }
}
