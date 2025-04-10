mod active_elections_container;
mod cooldown_controller;
mod recently_confirmed_cache;
mod root_container;
mod stopped_counter;
mod vote_counter;
mod vote_router;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock, RwLockReadGuard},
    time::{Duration, SystemTime},
};

use tracing::debug;

use rsnano_core::{
    utils::{BackpressureSender, ContainerInfo, ContainerInfoProvider},
    Amount, Block, BlockHash, PublicKey, QualifiedRoot, SavedBlock, Vote, VoteCode, VoteSource,
};
use rsnano_ledger::{BlockStatus, ProcessedResult, RepWeightCache, RollbackResults};
use rsnano_network::Channel;
use rsnano_nullable_clock::SteadyClock;
use rsnano_stats::{DetailType, Sample, StatType, Stats, StatsCollection, StatsSource};

use super::{
    election::{AddForkResult, ConfirmedElection, ElectionBehavior, VoteSummary},
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

#[derive(Clone)]
pub enum AecEvent {
    ElectionStarted(BlockHash),
    ElectionStopped(BlockHash),
    ElectionConfirmed(ConfirmedElection),

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
    max_elections: usize,
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
        let max_elections = config.max_elections;
        Self {
            container: RwLock::new(ActiveElectionsContainer::new(config, base_latency)),
            max_elections,
            rep_weights,
            fork_cache,
            vote_cache,
            stats,
            clock,
            observer: RwLock::new(None),
        }
    }

    pub fn set_observer(&self, observer: BackpressureSender<AecEvent>) {
        *self.observer.write().unwrap() = Some(observer);
    }

    pub fn read(&self) -> RwLockReadGuard<ActiveElectionsContainer> {
        self.container.read().unwrap()
    }

    pub fn len(&self) -> usize {
        self.container.read().unwrap().len()
    }

    pub fn max_len(&self) -> usize {
        self.container.read().unwrap().max_len()
    }

    pub fn set_cooldown(&self, cool_down: bool, reason: AecCooldownReason) {
        let ev = self
            .container
            .write()
            .unwrap()
            .set_cooldown(cool_down, reason);
        if let Some(ev) = ev {
            self.notify(ev);
        }
    }

    pub fn is_active_root(&self, root: &QualifiedRoot) -> bool {
        self.container.read().unwrap().is_active_root(root)
    }

    pub fn is_active_hash(&self, block_hash: &BlockHash) -> bool {
        self.container.read().unwrap().is_active_hash(block_hash)
    }

    pub fn clear_recently_confirmed(&self) {
        self.container.write().unwrap().clear_recently_confirmed();
    }

    pub fn transition_time(&self) {
        let now = self.clock.now();
        self.container.write().unwrap().transition_time(now);
    }

    pub fn erase_ended_elections(&self) {
        let erased = self.container.write().unwrap().erase_ended_elections();
        let something_erased = erased.len() > 0;
        for entry in erased {
            self.handle_removed_election(entry);
        }
        if something_erased {
            self.notify(AecEvent::VacancyUpdated);
        }
    }

    pub fn erase(&self, root: &QualifiedRoot) -> bool {
        let removed = self.container.write().unwrap().erase(root);
        let was_removed = removed.is_some();

        if let Some(entry) = removed {
            self.handle_removed_election(entry);
            self.notify(AecEvent::VacancyUpdated);
        }

        was_removed
    }

    fn handle_removed_election(&self, entry: Entry) {
        self.add_stats(&entry);

        let election = &entry.election;
        let winner_hash = election.winner().hash();
        let is_confirmed = election.is_confirmed();
        let blocks = election.candidate_blocks().clone();

        for (hash, block) in blocks {
            // Notify observers about dropped elections & blocks lost confirmed elections
            if !is_confirmed || hash != winner_hash {
                self.notify(AecEvent::ElectionStopped(hash));
            }

            if !is_confirmed {
                self.notify(AecEvent::BlockDiscarded(block.into()));
            }
        }
    }

    fn add_stats(&self, entry: &Entry) {
        let election = &entry.election;
        let start = election.start();
        let root = election.qualified_root().clone();

        // Track election duration
        self.stats.sample(
            Sample::ActiveElectionDuration,
            start.elapsed(self.clock.now()).as_millis() as i64,
            (0, 1000 * 60 * 10),
        ); // 0-10 minutes range

        // Notify observers without holding the lock
        if let Some(callback) = &entry.erased_callback {
            callback(&root);
        }
    }

    pub fn insert(
        &self,
        block: SavedBlock,
        election_behavior: ElectionBehavior,
        erased_callback: Option<ErasedCallback>,
    ) -> Result<(), AecInsertError> {
        let hash = block.hash();
        let root = block.qualified_root();

        let result = self.container.write().unwrap().insert(
            block,
            election_behavior,
            erased_callback,
            self.clock.now(),
        );

        if result.is_ok() {
            self.stats
                .inc(StatType::ActiveElections, DetailType::Started);
            self.stats
                .inc(StatType::ActiveElectionsStarted, election_behavior.into());

            debug!(behavior = ?election_behavior, block = %hash, "Started new election");
            self.notify(AecEvent::ElectionStarted(hash));

            let fork_cache = self.fork_cache.read().unwrap();
            for fork in fork_cache.get_forks(&root) {
                self.handle_fork(fork);
            }
        }

        result
    }

    pub fn try_add_fork(&self, fork: &Block, fork_tally: Amount) -> bool {
        let result = self
            .container
            .write()
            .unwrap()
            .try_add_fork(fork, fork_tally);

        let added = match result {
            AddForkResult::Added => {
                self.notify(AecEvent::BlockAddedToElection(fork.hash()));
                true
            }
            AddForkResult::Replaced(removed) => {
                self.notify(AecEvent::BlockDiscarded(removed.into()));
                self.notify(AecEvent::BlockAddedToElection(fork.hash()));
                true
            }
            AddForkResult::TallyTooLow => {
                self.notify(AecEvent::BlockDiscarded(fork.clone()));
                false
            }
            AddForkResult::Duplicate | AddForkResult::ElectionEnded => false,
        };

        added
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

    pub fn handle_processed_blocks(&self, batch: &[ProcessedResult]) {
        for result in batch {
            if result.status == BlockStatus::Fork {
                self.handle_fork(&result.block);
            }
        }
    }

    fn handle_fork(&self, fork: &Block) {
        let fork_tally = self.get_cached_tally(&fork.hash());
        let added = self.try_add_fork(fork, fork_tally);
        if added {
            self.stats
                .inc(StatType::Active, DetailType::ElectionBlockConflict);
            debug!("Block was added to an existing election: {}", fork.hash());
        }
    }

    fn get_cached_tally(&self, hash: &BlockHash) -> Amount {
        let votes = self.vote_cache.lock().unwrap().find(hash);
        let mut tally = Amount::zero();
        let weights = self.rep_weights.read();
        for vote in votes {
            tally += weights.get(&vote.voter).cloned().unwrap_or_default();
        }
        tally
    }
}

impl Drop for ActiveElections {
    fn drop(&mut self) {
        self.stop()
    }
}

impl ContainerInfoProvider for ActiveElections {
    fn container_info(&self) -> ContainerInfo {
        self.read().container_info()
    }
}

impl StatsSource for ActiveElections {
    fn collect_stats(&self, result: &mut StatsCollection) {
        self.container.read().unwrap().collect_stats(result);
    }
}

pub(crate) type ErasedCallback = Box<dyn Fn(&QualifiedRoot) + Send + Sync>;

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
