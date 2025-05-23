use std::sync::{mpsc::SyncSender, Arc, RwLock};

use rsnano_core::Networks;
use rsnano_ledger::LedgerEvent;
use rsnano_stats::{DetailType, StatType, Stats};

use crate::{
    block_processing::BlockProcessorQueue,
    bootstrap::Bootstrapper,
    cementation::ConfirmingSet,
    consensus::{
        ActiveElectionsContainer, DependentElectionsConfirmer, ForkCache, ForkCacheUpdater,
        LocalVoteHistory,
    },
    utils::BackpressureEventProcessor,
    NodeEvent,
};

pub(crate) struct LedgerEventProcessor {
    pub(crate) node_event_sender: Option<SyncSender<NodeEvent>>,
    pub confirming_set: Arc<ConfirmingSet>,
    pub stats: Arc<Stats>,
    pub(crate) dependent_elections_confirmer: DependentElectionsConfirmer,
    pub(crate) bootstrapper: Arc<Bootstrapper>,
    pub(crate) vote_history: Arc<LocalVoteHistory>,
    pub(crate) active_elections: Arc<RwLock<ActiveElectionsContainer>>,
    pub(crate) block_processor_queue: Arc<BlockProcessorQueue>,
    pub(crate) fork_cache_updater: ForkCacheUpdater,
    pub(crate) plugins: Vec<Box<dyn LedgerEventProcessorPlugin>>,
}

impl LedgerEventProcessor {
    #[allow(dead_code)]
    pub fn new_null() -> Self {
        Self {
            node_event_sender: None,
            confirming_set: Arc::new(ConfirmingSet::new_null()),
            stats: Arc::new(Stats::default()),
            dependent_elections_confirmer: DependentElectionsConfirmer::new_null(),
            bootstrapper: Arc::new(Bootstrapper::new_null()),
            vote_history: Arc::new(LocalVoteHistory::new(Networks::NanoLiveNetwork)),
            active_elections: Arc::new(RwLock::new(ActiveElectionsContainer::default())),
            block_processor_queue: Arc::new(BlockProcessorQueue::default()),
            fork_cache_updater: ForkCacheUpdater::new(Arc::new(RwLock::new(ForkCache::default()))),
            plugins: Vec::new(),
        }
    }
}

impl BackpressureEventProcessor<LedgerEvent> for LedgerEventProcessor {
    fn cool_down(&mut self) {
        self.confirming_set.set_cooldown(true);
        self.block_processor_queue.set_cooldown(true);
        self.stats
            .inc(StatType::ConfirmingSet, DetailType::Cooldown);
    }

    fn recovered(&mut self) {
        self.confirming_set.set_cooldown(false);
        self.block_processor_queue.set_cooldown(false);
        self.stats
            .inc(StatType::ConfirmingSet, DetailType::Recovered);
    }

    fn process(&mut self, event: LedgerEvent) {
        for plugin in &mut self.plugins {
            plugin.process(&event);
        }

        match event {
            LedgerEvent::BlocksProcessed(results) => {
                self.confirming_set.requeue_blocks(&results);
                self.bootstrapper.inspect_blocks(&results);
                self.fork_cache_updater.update(&results);
                if let Some(sender) = &self.node_event_sender {
                    sender.send(NodeEvent::BlocksProcessed(results)).unwrap();
                }
            }
            LedgerEvent::BlocksConfirmed(confirmed) => {
                self.dependent_elections_confirmer
                    .confirm_dependent_elections(&confirmed);
            }
            LedgerEvent::BlocksRolledBack(rolled_back) => {
                {
                    let mut aec = self.active_elections.write().unwrap();
                    for result in rolled_back.iter() {
                        for block in &result.rolled_back {
                            // Stop all rolled back active transactions except initial
                            if block.qualified_root() != result.target_root {
                                aec.erase(&block.qualified_root());
                            }
                        }
                    }
                }

                self.vote_history.erase_batch(rolled_back.roots());

                self.bootstrapper
                    .unblock_batch(rolled_back.affected_accounts());
            }
        }
    }
}

pub(crate) trait LedgerEventProcessorPlugin: Send {
    fn process(&mut self, event: &LedgerEvent);
}
