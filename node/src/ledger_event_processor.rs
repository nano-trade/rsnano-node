use std::sync::{mpsc::SyncSender, Arc, RwLock};

use rsnano_ledger::LedgerEvent;
use rsnano_stats::{DetailType, StatType, Stats};

use crate::{
    block_processing::{BlockProcessor, BoundedBacklog, LocalBlockBroadcaster},
    bootstrap::Bootstrapper,
    cementation::ConfirmingSet,
    consensus::{
        election_schedulers::ElectionSchedulers, ActiveElectionsContainer,
        DependentElectionsConfirmer, ForkCache, ForkCacheUpdater, ForkProcessor, LocalVoteHistory,
    },
    utils::BackpressureEventProcessor,
    NodeEvent,
};
use rsnano_core::Networks;

pub(crate) struct LedgerEventProcessor {
    pub(crate) node_event_sender: Option<SyncSender<NodeEvent>>,
    pub local_block_broadcaster: Arc<LocalBlockBroadcaster>,
    pub election_schedulers: Arc<ElectionSchedulers>,
    pub bounded_backlog: Arc<BoundedBacklog>,
    pub confirming_set: Arc<ConfirmingSet>,
    pub stats: Arc<Stats>,
    pub(crate) dependent_elections_confirmer: DependentElectionsConfirmer,
    pub(crate) bootstrapper: Arc<Bootstrapper>,
    pub(crate) vote_history: Arc<LocalVoteHistory>,
    pub(crate) active_elections: Arc<RwLock<ActiveElectionsContainer>>,
    pub(crate) block_processor: Arc<BlockProcessor>,
    pub(crate) fork_cache_updater: ForkCacheUpdater,
    pub(crate) fork_processor: Arc<ForkProcessor>,
}

impl LedgerEventProcessor {
    pub fn new_null() -> Self {
        Self {
            node_event_sender: None,
            local_block_broadcaster: Arc::new(LocalBlockBroadcaster::new_null()),
            election_schedulers: Arc::new(ElectionSchedulers::new_null()),
            bounded_backlog: Arc::new(BoundedBacklog::new_null()),
            confirming_set: Arc::new(ConfirmingSet::new_null()),
            stats: Arc::new(Stats::default()),
            dependent_elections_confirmer: DependentElectionsConfirmer::new_null(),
            bootstrapper: Arc::new(Bootstrapper::new_null()),
            vote_history: Arc::new(LocalVoteHistory::new(Networks::NanoLiveNetwork)),
            active_elections: Arc::new(RwLock::new(ActiveElectionsContainer::default())),
            block_processor: Arc::new(BlockProcessor::new_null()),
            fork_cache_updater: ForkCacheUpdater::new(Arc::new(RwLock::new(ForkCache::default()))),
            fork_processor: Arc::new(ForkProcessor::new_test_instance()),
        }
    }
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

                self.election_schedulers
                    .activate_successors(confirmed.iter().map(|(b, _)| b));

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

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_core::{BlockHash, SavedBlock};

    #[test]
    fn when_blocks_confirmed_should_activate_elections_for_sucessors() {
        let mut ev_processor = LedgerEventProcessor::new_null();
        let activation_tracker = ev_processor.election_schedulers.track_activate_successors();

        let block = SavedBlock::new_test_instance();
        let confirmed_blocks = vec![(block.clone(), BlockHash::from(123))];
        ev_processor.process(LedgerEvent::BlocksConfirmed(confirmed_blocks));

        let output = activation_tracker.output();
        assert_eq!(output, [block]);
    }
}
