mod active_elections_container;
mod cooldown_controller;
mod recently_confirmed_cache;
mod root_container;
mod stopped_counter;
mod vote_counter;
mod vote_router;

use std::{
    collections::HashMap,
    sync::{Arc, LockResult, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard},
    time::{Duration, SystemTime},
};

use tracing::debug;

use rsnano_core::{
    utils::{BackpressureSender, BlockPriority, ContainerInfo, ContainerInfoProvider},
    Amount, Block, BlockHash, PublicKey, QualifiedRoot, SavedBlock, Vote, VoteCode, VoteSource,
};
use rsnano_ledger::{RepWeightCache, RollbackResults};
use rsnano_network::Channel;
use rsnano_nullable_clock::SteadyClock;
use rsnano_stats::{DetailType, StatType, Stats, StatsCollection, StatsSource};

use super::{
    election::{ConfirmedElection, Election, ElectionBehavior, VoteSummary},
    ForkCache, VoteCache,
};
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
    stats: Arc<Stats>,
    clock: Arc<SteadyClock>,
    rep_weights: Arc<RepWeightCache>,
    observer: RwLock<Option<BackpressureSender<AecEvent>>>,
    fork_cache: Arc<RwLock<ForkCache>>,
    vote_cache: Arc<Mutex<VoteCache>>,
}

impl ActiveElections {
    pub(crate) fn new(
        config: ActiveElectionsConfig,
        stats: Arc<Stats>,
        rep_weights: Arc<RepWeightCache>,
        fork_cache: Arc<RwLock<ForkCache>>,
        vote_cache: Arc<Mutex<VoteCache>>,
        base_latency: Duration,
        clock: Arc<SteadyClock>,
    ) -> Self {
        Self {
            container: RwLock::new(ActiveElectionsContainer::new(config, base_latency)),
            rep_weights,
            fork_cache,
            vote_cache,
            stats,
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

    pub fn insert(
        &self,
        block: SavedBlock,
        election_behavior: ElectionBehavior,
        priority: Option<BlockPriority>,
    ) -> Result<bool, AecInsertError> {
        let hash = block.hash();
        let root = block.qualified_root();

        let result = self.container.write().unwrap().insert(
            block,
            election_behavior,
            priority,
            self.clock.now(),
        );

        if matches!(result, Ok(true)) {
            self.stats
                .inc(StatType::ActiveElections, DetailType::Started);
            self.stats
                .inc(StatType::ActiveElectionsStarted, election_behavior.into());

            debug!(behavior = ?election_behavior, block = %hash, "Started new election");
            self.notify(AecEvent::ElectionStarted(hash, root.clone()));
        }

        result
    }

    pub fn remove_recently_confirmed(&self, block_hash: &BlockHash) {
        self.container
            .write()
            .unwrap()
            .remove_recently_confirmed(block_hash);
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

    pub fn transition_active_hash(&self, block_hash: &BlockHash) -> bool {
        let mut container = self.container.write().unwrap();
        let Some(election) = container.election_for_block_mut(block_hash) else {
            return false;
        };
        election.transition_active();
        true
    }

    // TODO: Delete!
    pub fn transition_active(&self, root: &QualifiedRoot) {
        self.container
            .write()
            .unwrap()
            .election_for_root_mut(root)
            .unwrap()
            .transition_active();
    }

    // TODO: Delete!
    pub fn change_vote_timestamp(
        &self,
        root: &QualifiedRoot,
        voter: &PublicKey,
        new_timestamp: SystemTime,
    ) {
        self.container
            .write()
            .unwrap()
            .election_for_root_mut(root)
            .expect("No election found for given root")
            .change_vote_timestamp(voter, new_timestamp);
    }

    pub fn confirm_dependent_elections(
        &self,
        confirmed_blocks: Vec<(SavedBlock, Option<ConfirmedElection>)>,
    ) -> Vec<ConfirmedElection> {
        let now = self.clock.now();
        self.container
            .write()
            .unwrap()
            .confirm_dependent_elections(confirmed_blocks, now)
    }

    pub fn remove_votes<'a>(
        &self,
        root: &QualifiedRoot,
        voters: impl IntoIterator<Item = &'a PublicKey>,
    ) {
        let mut container = self.container.write().unwrap();
        let Some(election) = container.election_for_root_mut(root) else {
            return;
        };
        for voter in voters {
            election.remove_vote(voter);
        }
    }

    pub fn apply_votes(
        &self,
        voter: PublicKey,
        votes: impl IntoIterator<Item = VoteSummary>,
        source: VoteSource,
        online_weight: Amount,
        quorum_delta: Amount,
    ) -> HashMap<BlockHash, VoteCode> {
        let (results, events) = {
            let mut container = self.container.write().unwrap();
            let rep_weights = self.rep_weights.read();
            container.apply_votes(
                voter,
                votes,
                source,
                &rep_weights,
                online_weight,
                quorum_delta,
                self.clock.now(),
            )
        };

        for e in events {
            self.notify(e);
        }

        results
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
