mod root_container;

use std::{
    cmp::min,
    sync::{mpsc::SyncSender, Arc, Condvar, Mutex, MutexGuard, RwLock},
};

use root_container::{Entry, RootContainer};
use rsnano_nullable_clock::{SteadyClock, Timestamp};
use tracing::debug;

use rsnano_core::{utils::ContainerInfo, Amount, Block, BlockHash, QualifiedRoot, SavedBlock};
use rsnano_ledger::{BlockStatus, RepWeightCache};
use rsnano_stats::{DetailType, Sample, StatType, Stats};

use super::{
    AddForkResult, Election, ElectionBehavior, ElectionConfig, ElectionVoter,
    RecentlyConfirmedCache, VoteCache, VoteRouter,
};
use crate::{block_processing::BlockContext, cementation::ConfirmingSet};

#[derive(Clone, Debug, PartialEq)]
pub struct ActiveElectionsConfig {
    /// Maximum number of simultaneous active elections (AEC size)
    pub size: usize,
    /// Maximum confirmation history size
    pub confirmation_history_size: usize,
    /// Maximum cache size for recently_confirmed
    pub confirmation_cache: usize,
    /// Maximum size of election winner details set
    pub max_election_winners: usize,
}

impl Default for ActiveElectionsConfig {
    fn default() -> Self {
        Self {
            size: 5000,
            confirmation_history_size: 2048,
            confirmation_cache: 65536,
            max_election_winners: 1024 * 16,
        }
    }
}

pub enum AecEvent {
    ActiveStarted(BlockHash),
    ActiveStopped(BlockHash),
    BlockAddedToElection(BlockHash),
    BlockDiscarded(Block),
    VacancyUpdated,
}

pub struct ActiveElections {
    mutex: Mutex<ActiveElectionsState>,
    condition: Condvar,
    config: ActiveElectionsConfig,
    rep_weights: Arc<RepWeightCache>,
    confirming_set: Arc<ConfirmingSet>,
    recently_confirmed: Arc<RwLock<RecentlyConfirmedCache>>,
    vote_cache: Arc<Mutex<VoteCache>>,
    stats: Arc<Stats>,
    event_sender: RwLock<Option<SyncSender<AecEvent>>>,
    election_voter: ElectionVoter,
    clock: Arc<SteadyClock>,
}

impl ActiveElections {
    pub(crate) fn new(
        config: ActiveElectionsConfig,
        rep_weights: Arc<RepWeightCache>,
        confirming_set: Arc<ConfirmingSet>,
        vote_cache: Arc<Mutex<VoteCache>>,
        stats: Arc<Stats>,
        recently_confirmed: Arc<RwLock<RecentlyConfirmedCache>>,
        election_voter: ElectionVoter,
        election_config: ElectionConfig,
        clock: Arc<SteadyClock>,
    ) -> Self {
        Self {
            mutex: Mutex::new(ActiveElectionsState {
                roots: RootContainer::default(),
                vote_router: VoteRouter::new(),
                stopped: false,
                manual_count: 0,
                priority_count: 0,
                hinted_count: 0,
                optimistic_count: 0,
                config: election_config,
            }),
            condition: Condvar::new(),
            rep_weights,
            confirming_set,
            recently_confirmed,
            config,
            vote_cache,
            stats,
            event_sender: RwLock::new(None),
            election_voter,
            clock,
        }
    }

    pub fn set_event_sink(&mut self, sink: SyncSender<AecEvent>) {
        *self.event_sender.write().unwrap() = Some(sink);
    }

    pub fn len(&self) -> usize {
        self.mutex.lock().unwrap().roots.len()
    }

    pub fn info(&self) -> ActiveElectionsInfo {
        let guard = self.mutex.lock().unwrap();
        ActiveElectionsInfo {
            max_queue: self.config.size,
            total: guard.roots.len(),
            priority: guard.priority_count,
            hinted: guard.hinted_count,
            optimistic: guard.optimistic_count,
        }
    }

    pub fn max_len(&self) -> usize {
        self.config.size
    }

    /// How many election slots are available
    /// This is a soft limit and can be negative!
    pub fn vacancy(&self) -> i64 {
        let current_size = self.mutex.lock().unwrap().roots.len() as i64;
        let election_vacancy = self.config.size as i64 - current_size;
        let winners_vacancy = self.election_winners_vacancy();
        min(election_vacancy, winners_vacancy)
    }

    pub fn count_by_behavior(&self, behavior: ElectionBehavior) -> usize {
        self.mutex.lock().unwrap().count_by_behavior(behavior)
    }

    fn election_winners_vacancy(&self) -> i64 {
        self.config.max_election_winners as i64 - self.confirming_set.len() as i64
    }

    pub fn clear(&self) {
        // TODO: Call erased_callback for each election
        {
            let mut guard = self.mutex.lock().unwrap();
            guard.roots.clear();
        }

        self.notify(AecEvent::VacancyUpdated);
    }

    pub fn is_active_root(&self, root: &QualifiedRoot) -> bool {
        let guard = self.mutex.lock().unwrap();
        guard.roots.get(root).is_some()
    }

    pub fn is_active_hash(&self, block_hash: &BlockHash) -> bool {
        self.mutex.lock().unwrap().vote_router.is_active(block_hash)
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

    /// Broadcasts vote for the current winner of this election
    /// Checks if sufficient amount of time (`vote_generation_interval`) passed since the last vote generation
    pub fn try_generate_vote(&self, election: &mut Election) {
        self.election_voter.try_vote(election);
    }

    pub fn election_for_root(&self, root: &QualifiedRoot) -> Option<Arc<Mutex<Election>>> {
        self.mutex.lock().unwrap().election(root).cloned()
    }

    pub fn election_for_block(&self, block_hash: &BlockHash) -> Option<Arc<Mutex<Election>>> {
        self.mutex
            .lock()
            .unwrap()
            .vote_router
            .election(block_hash)
            .cloned()
    }

    pub fn get_all(&self) -> Vec<Arc<Mutex<Election>>> {
        self.mutex
            .lock()
            .unwrap()
            .roots
            .iter_sequenced()
            .map(|i| i.election.clone())
            .collect()
    }

    pub fn erase(&self, root: &QualifiedRoot) -> bool {
        let mut guard = self.mutex.lock().unwrap();
        if let Some(entry) = guard.roots.erase(root) {
            self.cleanup_election(guard, entry);
            true
        } else {
            false
        }
    }

    /// Erase all blocks from active, if not confirmed, clear digests from network filters
    fn cleanup_election<'a>(&self, mut guard: MutexGuard<'a, ActiveElectionsState>, entry: Entry) {
        // lock vote router before locking election to prevent dead lock!
        let election = entry.election.lock().unwrap();

        // Keep track of election count by election type
        *guard.count_by_behavior_mut(election.behavior()) -= 1;
        guard.vote_router.disconnect_election(&election);
        let winner_hash = election.winner().hash();

        self.stats
            .inc(StatType::ActiveElections, DetailType::Stopped);

        self.stats.inc(
            StatType::ActiveElections,
            if election.is_confirmed() {
                DetailType::Confirmed
            } else {
                DetailType::Unconfirmed
            },
        );
        self.stats
            .inc(StatType::ActiveElectionsStopped, election.state().into());
        self.stats
            .inc(election.state().into(), election.behavior().into());
        drop(guard);

        // Track election duration
        self.stats.sample(
            Sample::ActiveElectionDuration,
            election.start().elapsed(self.clock.now()).as_millis() as i64,
            (0, 1000 * 60 * 10),
        ); // 0-10 minutes range

        // Notify observers without holding the lock
        if let Some(callback) = entry.erased_callback {
            callback(election.qualified_root());
        }
        self.notify(AecEvent::VacancyUpdated);

        for (hash, block) in election.candidate_blocks() {
            // Notify observers about dropped elections & blocks lost confirmed elections
            if !election.is_confirmed() || *hash != winner_hash {
                self.notify(AecEvent::ActiveStopped(*hash));
            }

            if !election.is_confirmed() {
                self.notify(AecEvent::BlockDiscarded(block.clone().into()));
            }
        }
    }

    pub fn insert(
        &self,
        block: SavedBlock,
        election_behavior: ElectionBehavior,
        erased_callback: Option<ErasedCallback>,
    ) -> Option<ElectionInsertInfo> {
        if self
            .recently_confirmed
            .read()
            .unwrap()
            .root_exists(&block.qualified_root())
        {
            // This block or a fork got recently confirmed, so there is no need for a new election.
            return None;
        }

        let hash = block.hash();

        let mut guard = self.mutex.lock().unwrap();
        let result = guard.insert(block, election_behavior, erased_callback, self.clock.now());

        if let Some(info) = &result {
            if info.inserted {
                guard.vote_router.connect(hash, info.election.clone());

                // Skip passive phase for blocks without cached votes to avoid bootstrap delays
                let in_cache = self.vote_cache.lock().unwrap().contains(&hash);

                if !in_cache {
                    self.stats
                        .inc(StatType::ActiveElections, DetailType::ActivateImmediately);
                    info.election.lock().unwrap().transition_active();
                }

                self.stats
                    .inc(StatType::ActiveElections, DetailType::Started);
                self.stats
                    .inc(StatType::ActiveElectionsStarted, election_behavior.into());

                debug!(
                    in_cache,
                    behavior = ?election_behavior,
                    block = %hash,
                    "Started new election"
                );

                self.notify(AecEvent::ActiveStarted(hash));
            }
            drop(guard);

            // Votes are also generated for ongoing elections
            self.try_generate_vote(&mut info.election.lock().unwrap());
        }

        result
    }

    pub fn handle_processed_blocks(&self, batch: &[(BlockStatus, Arc<BlockContext>)]) {
        for (status, context) in batch {
            if *status == BlockStatus::Fork {
                self.handle_fork(&context.block);
            }
        }
    }

    pub fn handle_fork(&self, fork: &Block) {
        let mut guard = self.mutex.lock().unwrap();
        if let Some(entry) = guard.roots.get(&fork.qualified_root()) {
            let election_mutex = entry.election.clone();
            let added = {
                let mut election = election_mutex.lock().unwrap();
                self.try_add_fork(&mut guard, &mut election, fork)
            };
            if added {
                guard.vote_router.connect(fork.hash(), election_mutex);

                self.notify(AecEvent::BlockAddedToElection(fork.hash()));

                self.stats
                    .inc(StatType::Active, DetailType::ElectionBlockConflict);
                debug!("Block was added to an existing election: {}", fork.hash());
            }
        }
    }

    /// Returns wether the fork was added to the election.
    /// Result is false if:
    /// 1) election is confirmed or expired
    /// 2) given election contains 10 blocks & new block didn't receive enough votes to replace existing blocks
    /// 3) given block is already in election & election contains less than 10 blocks (replacing block content with new)
    fn try_add_fork(
        &self,
        state: &mut ActiveElectionsState,
        election: &mut Election,
        fork: &Block,
    ) -> bool {
        // Try to remove a block with a lower tally, so that the fork can be added
        let fork_tally = self.get_cached_tally(&fork.hash());

        let added = match election.add_fork(fork.clone(), fork_tally) {
            AddForkResult::Added => true,
            AddForkResult::Replaced(removed) => {
                state.vote_router.disconnect(&removed.hash());
                self.notify(AecEvent::BlockDiscarded(removed.into()));
                true
            }
            AddForkResult::Duplicate => false,
            AddForkResult::TallyTooLow | AddForkResult::ElectionEnded => {
                self.notify(AecEvent::BlockDiscarded(fork.clone()));
                false
            }
        };

        added
    }

    pub fn iter_batch_by_root<'a, 'b, T>(
        &'a self,
        roots: impl IntoIterator<Item = (QualifiedRoot, &'b T)>,
        mut handle: impl FnMut(QualifiedRoot, Option<&Arc<Mutex<Election>>>, &'b T),
    ) where
        T: 'b,
    {
        let guard = self.mutex.lock().unwrap();
        for (root, context) in roots.into_iter() {
            let election = guard.election(&root);
            handle(root, election, context);
        }
    }

    pub fn iter_batch_by_hash<'a>(
        &self,
        blocks: impl IntoIterator<Item = &'a BlockHash>,
        mut handle: impl FnMut(&BlockHash, Option<&Arc<Mutex<Election>>>),
    ) {
        let guard = self.mutex.lock().unwrap();
        for hash in blocks.into_iter() {
            let election = guard.vote_router.election(hash);
            handle(hash, election);
        }
    }

    pub fn stop(&self) {
        self.mutex.lock().unwrap().stopped = true;
        self.condition.notify_all();
        self.clear();
        // destroy send queue so that the receiver thread will be stopped too
        drop(self.event_sender.write().unwrap().take())
    }

    pub fn container_info(&self) -> ContainerInfo {
        let guard = self.mutex.lock().unwrap();
        let vote_router = guard.vote_router.container_info();

        ContainerInfo::builder()
            .leaf("roots", guard.roots.len(), RootContainer::ELEMENT_SIZE)
            .leaf(
                "normal",
                guard.count_by_behavior(ElectionBehavior::Priority),
                0,
            )
            .leaf(
                "hinted".to_string(),
                guard.count_by_behavior(ElectionBehavior::Hinted),
                0,
            )
            .leaf(
                "optimistic".to_string(),
                guard.count_by_behavior(ElectionBehavior::Optimistic),
                0,
            )
            .node(
                "recently_confirmed",
                self.recently_confirmed.read().unwrap().container_info(),
            )
            .node("vote_router", vote_router)
            .finish()
    }

    fn notify(&self, event: AecEvent) {
        if let Some(sender) = self.event_sender.read().unwrap().as_ref() {
            sender.send(event).unwrap()
        }
    }
}

impl Drop for ActiveElections {
    fn drop(&mut self) {
        self.stop()
    }
}

pub struct ActiveElectionsState {
    roots: RootContainer,
    stopped: bool,
    manual_count: usize,
    priority_count: usize,
    hinted_count: usize,
    optimistic_count: usize,
    config: ElectionConfig,
    vote_router: VoteRouter,
}

impl ActiveElectionsState {
    fn count_by_behavior(&self, behavior: ElectionBehavior) -> usize {
        match behavior {
            ElectionBehavior::Manual => self.manual_count,
            ElectionBehavior::Priority => self.priority_count,
            ElectionBehavior::Hinted => self.hinted_count,
            ElectionBehavior::Optimistic => self.optimistic_count,
        }
    }

    fn count_by_behavior_mut(&mut self, behavior: ElectionBehavior) -> &mut usize {
        match behavior {
            ElectionBehavior::Manual => &mut self.manual_count,
            ElectionBehavior::Priority => &mut self.priority_count,
            ElectionBehavior::Hinted => &mut self.hinted_count,
            ElectionBehavior::Optimistic => &mut self.optimistic_count,
        }
    }

    fn election(&self, root: &QualifiedRoot) -> Option<&Arc<Mutex<Election>>> {
        self.roots.get(root).map(|i| &i.election)
    }

    fn maybe_upgrade_to(
        &mut self,
        new_behavior: ElectionBehavior,
        election: &mut Election,
    ) -> bool {
        let previous_behavior = election.behavior();
        let upgraded = election.maybe_upgrade_to(new_behavior);
        if upgraded {
            *self.count_by_behavior_mut(previous_behavior) -= 1;
            *self.count_by_behavior_mut(new_behavior) += 1;
        }
        upgraded
    }

    fn insert(
        &mut self,
        block: SavedBlock,
        election_behavior: ElectionBehavior,
        erased_callback: Option<ErasedCallback>,
        now: Timestamp,
    ) -> Option<ElectionInsertInfo> {
        if self.stopped {
            return None;
        }

        let root = block.qualified_root();
        let existing = self.roots.get(&root).map(|i| i.election.clone());
        if let Some(existing) = existing {
            {
                // Try upgrading to priority election to enable immediate vote broadcasting.
                let mut election = existing.lock().unwrap();
                self.maybe_upgrade_to(election_behavior, &mut election);
            }
            Some(ElectionInsertInfo {
                election: existing,
                inserted: false,
            })
        } else {
            let election = Arc::new(Mutex::new(Election::new(
                block,
                election_behavior,
                self.config.clone(),
                now,
            )));

            self.roots.insert(Entry {
                root,
                election: election.clone(),
                erased_callback,
            });

            // Keep track of election count by election type
            *self.count_by_behavior_mut(election_behavior) += 1;

            Some(ElectionInsertInfo {
                election,
                inserted: true,
            })
        }
    }
}

pub struct ElectionInsertInfo {
    pub election: Arc<Mutex<Election>>,
    pub inserted: bool,
}

#[derive(Default)]
pub struct ActiveElectionsInfo {
    pub max_queue: usize,
    pub total: usize,
    pub priority: usize,
    pub hinted: usize,
    pub optimistic: usize,
}

pub(crate) type ErasedCallback = Box<dyn Fn(&QualifiedRoot) + Send + Sync>;
