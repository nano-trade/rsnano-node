use std::sync::{mpsc::SyncSender, Arc, RwLock};

use rsnano_ledger::LedgerEvent;
use rsnano_stats::{DetailType, StatType, Stats};

use crate::{
    block_processing::{BlockProcessor, BoundedBacklog, LocalBlockBroadcaster},
    bootstrap::Bootstrapper,
    cementation::ConfirmingSet,
    config::NodeFlags,
    consensus::{
        election_schedulers::ElectionSchedulers, ActiveElectionsContainer,
        DependentElectionsConfirmer, ForkCacheUpdater, ForkProcessor, LocalVoteHistory,
    },
    utils::BackpressureEventProcessor,
    NodeEvent,
};

pub(crate) struct LedgerEventProcessor {
    pub(crate) node_event_sender: Option<SyncSender<NodeEvent>>,
    pub local_block_broadcaster: Arc<LocalBlockBroadcaster>,
    pub election_schedulers: Arc<ElectionSchedulers>,
    pub bounded_backlog: Arc<BoundedBacklog>,
    pub confirming_set: Arc<ConfirmingSet>,
    pub stats: Arc<Stats>,
    pub flags: NodeFlags,
    pub(crate) dependent_elections_confirmer: DependentElectionsConfirmer,
    pub(crate) bootstrapper: Arc<Bootstrapper>,
    pub(crate) vote_history: Arc<LocalVoteHistory>,
    pub(crate) active_elections: Arc<RwLock<ActiveElectionsContainer>>,
    pub(crate) block_processor: Arc<BlockProcessor>,
    pub(crate) fork_cache_updater: ForkCacheUpdater,
    pub(crate) fork_processor: Arc<ForkProcessor>,
}

impl BackpressureEventProcessor<LedgerEvent> for LedgerEventProcessor {
    fn cool_down(&mut self) {
        self.confirming_set.set_cooldown(true);
        self.block_processor.set_cooldown(true);
        self.stats
            .inc(StatType::ConfirmingSet, DetailType::Cooldown);
    }

    fn recovered(&mut self) {
        self.confirming_set.set_cooldown(false);
        self.block_processor.set_cooldown(false);
        self.stats
            .inc(StatType::ConfirmingSet, DetailType::Recovered);
    }

    fn process(&mut self, event: LedgerEvent) {
        match event {
            LedgerEvent::BlocksProcessed(results) => {
                // Notify elections about alternative (forked) blocks
                self.fork_processor.handle_forks(&results);
                self.election_schedulers
                    .activate_accounts_with_fresh_blocks(&results);

                self.confirming_set.requeue_blocks(&results);
                self.bounded_backlog.insert_processed(&results);
                self.bootstrapper.inspect_blocks(&results);
                self.local_block_broadcaster.blocks_processed(&results);
                self.fork_cache_updater.update(&results);
                if let Some(sender) = &self.node_event_sender {
                    sender.send(NodeEvent::BlocksProcessed(results)).unwrap();
                }
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
                {
                    let mut active = self.active_elections.write().unwrap();
                    for result in rolled_back.iter() {
                        for block in &result.rolled_back {
                            // Stop all rolled back active transactions except initial
                            if block.qualified_root() != result.target_root {
                                active.erase(&block.qualified_root());
                            }
                        }
                    }
                }

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
