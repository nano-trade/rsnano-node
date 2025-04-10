mod active_elections_container;
mod cooldown_controller;
mod recently_confirmed_cache;
mod root_container;
mod stopped_counter;
mod vote_counter;
mod vote_router;

use std::{
    collections::HashMap,
    sync::{Arc, LockResult, RwLock, RwLockReadGuard, RwLockWriteGuard},
    time::Duration,
};

use rsnano_core::{
    utils::{BackpressureSender, BlockPriority, ContainerInfo, ContainerInfoProvider},
    Amount, Block, BlockHash, PublicKey, QualifiedRoot, SavedBlock, Vote, VoteCode, VoteSource,
};
use rsnano_ledger::RollbackResults;
use rsnano_network::Channel;
use rsnano_nullable_clock::SteadyClock;
use rsnano_stats::{StatsCollection, StatsSource};

use super::election::{ConfirmedElection, Election};
pub use active_elections_container::*;
pub use cooldown_controller::AecCooldownReason;
use root_container::{Entry, RootContainer};

#[derive(Clone, Debug, PartialEq)]
pub struct ActiveElectionsConfig {
    /// Maximum number of simultaneous active elections (AEC size)
    pub max_elections: usize,
    /// Maximum cache size for recently_confirmed
    pub confirmation_cache: usize,
}

impl Default for ActiveElectionsConfig {
    fn default() -> Self {
        Self {
            max_elections: 5000,
            confirmation_cache: 65536,
        }
    }
}

pub enum AecEvent {
    ElectionStarted(BlockHash, QualifiedRoot),
    ElectionConfirmed(ConfirmedElection),

    /// Ended ether confirmed or unconfirmed
    ElectionEnded(Election, Option<BlockPriority>),

    BlockAddedToElection(BlockHash),
    BlockDiscarded(Block),
    BlockConfirmed(SavedBlock, ConfirmedElection),
    VoteCounted(PublicKey, VoteSource),
    /// old winner + new winner block
    WinnerChanged(BlockHash, Block),

    VoteProcessed(
        Arc<Vote>,
        Amount,
        VoteSource,
        Option<Arc<Channel>>,
        HashMap<BlockHash, VoteCode>,
    ),
    FinalPhaseStarted(BlockHash, QualifiedRoot),
    VacancyUpdated,
}

pub struct ActiveElections {
    container: RwLock<ActiveElectionsContainer>,
    clock: Arc<SteadyClock>,
    observer: RwLock<Option<BackpressureSender<AecEvent>>>,
}

impl ActiveElections {
    pub(crate) fn new(
        config: ActiveElectionsConfig,
        base_latency: Duration,
        clock: Arc<SteadyClock>,
    ) -> Self {
        Self {
            container: RwLock::new(ActiveElectionsContainer::new(config, base_latency)),
            clock,
            observer: RwLock::new(None),
        }
    }

    pub fn set_observer(&self, observer: BackpressureSender<AecEvent>) {
        *self.observer.write().unwrap() = Some(observer.clone());
        self.container.write().unwrap().set_observer(observer);
    }

    pub fn read(&self) -> LockResult<RwLockReadGuard<ActiveElectionsContainer>> {
        self.container.read()
    }

    pub fn write(&self) -> LockResult<RwLockWriteGuard<ActiveElectionsContainer>> {
        self.container.write()
    }

    pub fn stop(&self) {
        self.container.write().unwrap().stop();
        // destroy send queue so that the receiver thread will be stopped too
        drop(self.observer.write().unwrap().take());
    }

    fn notify(&self, event: AecEvent) {
        if let Some(sender) = self.observer.read().unwrap().as_ref() {
            sender.send(event).unwrap()
        }
    }

    pub fn force_confirm(&self, block_hash: &BlockHash) {
        let event = self
            .container
            .write()
            .unwrap()
            .force_confirm(block_hash, self.clock.now());

        if let Some(e) = event {
            self.notify(e);
        } else {
            panic!("Force confirm failed, because no active election was found");
        }
    }

    pub fn cancel(&self, root: &QualifiedRoot) {
        self.container.write().unwrap().cancel(root);
    }

    pub fn rolled_back(&self, results: &RollbackResults) {
        let mut container = self.container.write().unwrap();
        for result in results.iter() {
            for block in &result.rolled_back {
                // Stop all rolled back active transactions except initial
                if block.qualified_root() != result.target_root {
                    container.erase(&block.qualified_root());
                }
            }
        }
    }
}

impl Drop for ActiveElections {
    fn drop(&mut self) {
        self.stop()
    }
}

impl ContainerInfoProvider for ActiveElections {
    fn container_info(&self) -> ContainerInfo {
        self.read().unwrap().container_info()
    }
}

impl StatsSource for ActiveElections {
    fn collect_stats(&self, result: &mut StatsCollection) {
        self.container.read().unwrap().collect_stats(result);
    }
}

pub struct ApplyVoteResult {
    pub voted_block: BlockHash,
    pub vote_result: VoteCode,
    pub events: Vec<AecEvent>,
}

impl ApplyVoteResult {
    pub fn new(voted_block: BlockHash, vote_result: VoteCode) -> Self {
        Self {
            voted_block,
            vote_result,
            events: Vec::new(),
        }
    }
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum AecInsertError {
    Stopped,
    Duplicate,
    RecentlyConfirmed,
}
