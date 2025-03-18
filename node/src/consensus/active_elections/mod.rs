mod root_container;

use std::{
    cmp::min,
    sync::{mpsc::SyncSender, Arc, Mutex, MutexGuard, RwLock},
};

use root_container::{Entry, RootContainer};
use rsnano_nullable_clock::{SteadyClock, Timestamp};
use tracing::debug;

use rsnano_core::{utils::ContainerInfo, Amount, Block, BlockHash, QualifiedRoot, SavedBlock};
use rsnano_stats::{DetailType, Sample, StatType, Stats};

use super::{
    AddForkResult, Election, ElectionBehavior, ElectionConfig, RecentlyConfirmedCache, VoteRouter,
};
use crate::cementation::ConfirmingSet;

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
    ElectionStarted(BlockHash),
    /// An attempt was made to start an election, but an election for this block
    /// did already exist
    DuplicateElectionAttempt(BlockHash),
    /// The election was dropped without being confirmed
    ElectionDropped(BlockHash),
    BlockAddedToElection(BlockHash),
    BlockDiscarded(Block),
    VacancyUpdated,
}

pub struct ActiveElections {
    container: Mutex<ActiveElectionsContainer>,
    config: ActiveElectionsConfig,
    confirming_set: Arc<ConfirmingSet>,
    stats: Arc<Stats>,
    clock: Arc<SteadyClock>,
    event_sender: RwLock<Option<SyncSender<AecEvent>>>,
}

impl ActiveElections {
    pub(crate) fn new(
        config: ActiveElectionsConfig,
        confirming_set: Arc<ConfirmingSet>,
        stats: Arc<Stats>,
        election_config: ElectionConfig,
        clock: Arc<SteadyClock>,
    ) -> Self {
        Self {
            container: Mutex::new(ActiveElectionsContainer {
                roots: RootContainer::default(),
                vote_router: VoteRouter::new(),
                stopped: false,
                manual_count: 0,
                priority_count: 0,
                hinted_count: 0,
                optimistic_count: 0,
                config: election_config,
                recently_confirmed: RecentlyConfirmedCache::new(config.confirmation_cache),
            }),
            confirming_set,
            config,
            stats,
            clock,
            event_sender: RwLock::new(None),
        }
    }

    pub fn set_event_sink(&self, sink: SyncSender<AecEvent>) {
        *self.event_sender.write().unwrap() = Some(sink);
    }

    pub fn len(&self) -> usize {
        self.container.lock().unwrap().roots.len()
    }

    pub fn info(&self) -> ActiveElectionsInfo {
        let guard = self.container.lock().unwrap();
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
        let current_size = self.container.lock().unwrap().roots.len() as i64;
        let election_vacancy = self.config.size as i64 - current_size;
        let winners_vacancy = self.election_winners_vacancy();
        min(election_vacancy, winners_vacancy)
    }

    pub fn count_by_behavior(&self, behavior: ElectionBehavior) -> usize {
        self.container.lock().unwrap().count_by_behavior(behavior)
    }

    fn election_winners_vacancy(&self) -> i64 {
        self.config.max_election_winners as i64 - self.confirming_set.len() as i64
    }

    pub fn is_active_root(&self, root: &QualifiedRoot) -> bool {
        self.container.lock().unwrap().roots.get(root).is_some()
    }

    pub fn is_active_hash(&self, block_hash: &BlockHash) -> bool {
        self.container
            .lock()
            .unwrap()
            .vote_router
            .is_active(block_hash)
    }

    pub fn was_recently_confirmed(&self, block_hash: &BlockHash) -> bool {
        self.container
            .lock()
            .unwrap()
            .recently_confirmed
            .hash_exists(block_hash)
    }

    pub fn clear_recently_confirmed(&self) {
        self.container.lock().unwrap().recently_confirmed.clear();
    }

    pub fn election_for_root(&self, root: &QualifiedRoot) -> Option<Arc<Mutex<Election>>> {
        self.container.lock().unwrap().election(root).cloned()
    }

    pub fn election_for_block(&self, block_hash: &BlockHash) -> Option<Arc<Mutex<Election>>> {
        self.container
            .lock()
            .unwrap()
            .vote_router
            .election(block_hash)
            .cloned()
    }

    pub fn get_all(&self) -> Vec<Arc<Mutex<Election>>> {
        self.container
            .lock()
            .unwrap()
            .roots
            .iter_sequenced()
            .map(|i| i.election.clone())
            .collect()
    }

    pub fn erase(&self, root: &QualifiedRoot) -> bool {
        let entry;
        {
            let mut guard = self.container.lock().unwrap();
            entry = guard.roots.erase(root);
            if let Some(e) = &entry {
                self.cleanup_election(guard, e);
            }
        };

        if let Some(entry) = &entry {
            self.notify(AecEvent::VacancyUpdated);
            let election = entry.election.lock().unwrap();
            let winner_hash = election.winner().hash();
            for (hash, block) in election.candidate_blocks() {
                // Notify observers about dropped elections & blocks lost confirmed elections
                if !election.is_confirmed() || *hash != winner_hash {
                    self.notify(AecEvent::ElectionDropped(*hash));
                }

                if !election.is_confirmed() {
                    self.notify(AecEvent::BlockDiscarded(block.clone().into()));
                }
            }
        }

        entry.is_some()
    }

    /// Erase all blocks from active, if not confirmed, clear digests from network filters
    fn cleanup_election<'a>(
        &'a self,
        mut guard: MutexGuard<'a, ActiveElectionsContainer>,
        entry: &Entry,
    ) {
        {
            let election = entry.election.lock().unwrap();

            // Keep track of election count by election type
            *guard.count_by_behavior_mut(election.behavior()) -= 1;
            guard.vote_router.disconnect_election(&election);
            let winner_hash = election.winner().hash();
            if election.is_confirmed() {
                guard
                    .recently_confirmed
                    .put(election.qualified_root().clone(), winner_hash);
            }

            drop(guard);
        }

        let election = entry.election.lock().unwrap();
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
        // Track election duration
        self.stats.sample(
            Sample::ActiveElectionDuration,
            election.start().elapsed(self.clock.now()).as_millis() as i64,
            (0, 1000 * 60 * 10),
        ); // 0-10 minutes range

        // Notify observers without holding the lock
        if let Some(callback) = &entry.erased_callback {
            callback(election.qualified_root());
        }
    }

    pub fn insert(
        &self,
        block: SavedBlock,
        election_behavior: ElectionBehavior,
        erased_callback: Option<ErasedCallback>,
    ) -> Option<ElectionInsertInfo> {
        let hash = block.hash();

        let result = {
            let mut guard = self.container.lock().unwrap();
            if guard
                .recently_confirmed
                .root_exists(&block.qualified_root())
            {
                // This block or a fork got recently confirmed, so there is no need for a new election.
                return None;
            }

            let result = guard.insert(block, election_behavior, erased_callback, self.clock.now());
            if let Some(info) = &result {
                if info.inserted {
                    guard.vote_router.connect(hash, info.election.clone());

                    self.stats
                        .inc(StatType::ActiveElections, DetailType::Started);
                    self.stats
                        .inc(StatType::ActiveElectionsStarted, election_behavior.into());

                    debug!(behavior = ?election_behavior, block = %hash, "Started new election");
                }
            }

            result
        };

        if let Some(info) = &result {
            if info.inserted {
                self.notify(AecEvent::ElectionStarted(hash));
            } else {
                self.notify(AecEvent::DuplicateElectionAttempt(hash));
            }
        }

        result
    }

    pub fn try_add_fork(&self, fork: &Block, fork_tally: Amount) -> bool {
        let result = self
            .container
            .lock()
            .unwrap()
            .try_add_fork(fork, fork_tally);
        let added = match result {
            AddForkResult::Added => true,
            AddForkResult::Replaced(removed) => {
                self.notify(AecEvent::BlockDiscarded(removed.into()));
                true
            }
            AddForkResult::TallyTooLow => {
                self.notify(AecEvent::BlockDiscarded(fork.clone()));
                false
            }
            AddForkResult::Duplicate => false,
            AddForkResult::ElectionEnded => false,
        };

        if added {
            self.notify(AecEvent::BlockAddedToElection(fork.hash()));
        }

        added
    }

    pub fn iter_batch_by_root<'a, 'b, T>(
        &'a self,
        roots: impl IntoIterator<Item = (QualifiedRoot, &'b T)>,
        mut handle: impl FnMut(QualifiedRoot, Option<&Arc<Mutex<Election>>>, &'b T),
    ) where
        T: 'b,
    {
        let guard = self.container.lock().unwrap();
        for (root, context) in roots.into_iter() {
            let election = guard.election(&root);
            handle(root, election, context);
        }
    }

    pub fn iter_batch_by_hash<'a>(
        &self,
        blocks: impl IntoIterator<Item = &'a BlockHash>,
        mut handle: impl FnMut(&BlockHash, Option<&Arc<Mutex<Election>>>, bool),
    ) {
        let guard = self.container.lock().unwrap();
        for hash in blocks.into_iter() {
            let election = guard.vote_router.election(hash);
            let recently_confirmed = guard.recently_confirmed.hash_exists(hash);
            handle(hash, election, recently_confirmed);
        }
    }

    pub fn cementing_failed(&self, block_hash: &BlockHash) {
        self.container
            .lock()
            .unwrap()
            .recently_confirmed
            .erase(block_hash);
    }

    pub fn stop(&self) {
        self.container.lock().unwrap().stop();
        // destroy send queue so that the receiver thread will be stopped too
        drop(self.event_sender.write().unwrap().take());
    }

    fn notify(&self, event: AecEvent) {
        if let Some(sender) = self.event_sender.read().unwrap().as_ref() {
            sender.send(event).unwrap()
        }
    }

    pub fn container_info(&self) -> ContainerInfo {
        let guard = self.container.lock().unwrap();
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
                guard.recently_confirmed.container_info(),
            )
            .node("vote_router", vote_router)
            .finish()
    }
}

impl Drop for ActiveElections {
    fn drop(&mut self) {
        self.stop()
    }
}

pub struct ActiveElectionsContainer {
    roots: RootContainer,
    stopped: bool,
    manual_count: usize,
    priority_count: usize,
    hinted_count: usize,
    optimistic_count: usize,
    config: ElectionConfig,
    vote_router: VoteRouter,
    recently_confirmed: RecentlyConfirmedCache,
}

impl ActiveElectionsContainer {
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

    fn try_add_fork(&mut self, fork: &Block, fork_tally: Amount) -> AddForkResult {
        let Some(entry) = self.roots.get(&fork.qualified_root()) else {
            return AddForkResult::ElectionEnded;
        };

        let mut election = entry.election.lock().unwrap();

        let result = election.try_add_fork(fork, fork_tally);
        let added = match &result {
            AddForkResult::Added => true,
            AddForkResult::Replaced(removed) => {
                self.vote_router.disconnect(&removed.hash());
                true
            }
            AddForkResult::Duplicate => false,
            AddForkResult::TallyTooLow | AddForkResult::ElectionEnded => false,
        };

        if added {
            self.vote_router
                .connect(fork.hash(), entry.election.clone());
        }

        result
    }

    fn stop(&mut self) {
        self.stopped = true;
        self.roots.clear();
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
