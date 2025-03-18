mod active_elections_container;
mod recently_confirmed_cache;
mod root_container;

pub use active_elections_container::*;

use std::sync::{mpsc::SyncSender, Arc, Mutex, RwLock};

use root_container::{Entry, RootContainer};
use rsnano_nullable_clock::SteadyClock;
use tracing::debug;

use rsnano_core::{utils::ContainerInfo, Amount, Block, BlockHash, QualifiedRoot, SavedBlock};
use rsnano_stats::{DetailType, Sample, StatType, Stats};

use super::{AddForkResult, Election, ElectionBehavior, ElectionConfig, VoteRouter};

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
    max_elections: usize,
    stats: Arc<Stats>,
    clock: Arc<SteadyClock>,
    event_sender: RwLock<Option<SyncSender<AecEvent>>>,
}

impl ActiveElections {
    pub(crate) fn new(
        config: ActiveElectionsConfig,
        stats: Arc<Stats>,
        election_config: ElectionConfig,
        clock: Arc<SteadyClock>,
    ) -> Self {
        let max_elections = config.max_elections;
        Self {
            container: Mutex::new(ActiveElectionsContainer::new(config, election_config)),
            max_elections,
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
        self.container.lock().unwrap().info()
    }

    pub fn max_len(&self) -> usize {
        self.max_elections
    }

    /// How many election slots are available
    /// This is a soft limit and can be negative!
    pub fn vacancy(&self) -> i64 {
        self.container.lock().unwrap().vacancy()
    }

    pub fn cool_down(&self) {
        self.container.lock().unwrap().cool_down = true;
    }

    pub fn resume(&self) {
        self.container.lock().unwrap().cool_down = false;
    }

    pub fn count_by_behavior(&self, behavior: ElectionBehavior) -> usize {
        self.container.lock().unwrap().count_by_behavior(behavior)
    }

    pub fn is_active_root(&self, root: &QualifiedRoot) -> bool {
        self.container.lock().unwrap().is_active_root(root)
    }

    pub fn is_active_hash(&self, block_hash: &BlockHash) -> bool {
        self.container.lock().unwrap().is_active_hash(block_hash)
    }

    pub fn was_recently_confirmed(&self, block_hash: &BlockHash) -> bool {
        self.container
            .lock()
            .unwrap()
            .was_recently_confirmed(block_hash)
    }

    pub fn clear_recently_confirmed(&self) {
        self.container.lock().unwrap().clear_recently_confirmed();
    }

    pub fn election_for_root(&self, root: &QualifiedRoot) -> Option<Arc<Mutex<Election>>> {
        self.container
            .lock()
            .unwrap()
            .election_for_root(root)
            .cloned()
    }

    pub fn election_for_block(&self, block_hash: &BlockHash) -> Option<Arc<Mutex<Election>>> {
        self.container
            .lock()
            .unwrap()
            .election_for_block(block_hash)
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
        let removed = self.container.lock().unwrap().erase(root);

        if let Some(entry) = &removed {
            self.add_stats(entry);
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

        removed.is_some()
    }

    fn add_stats(&self, entry: &Entry) {
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

        let result = self.container.lock().unwrap().insert2(
            block,
            election_behavior,
            erased_callback,
            self.clock.now(),
        );

        if let Some(info) = &result {
            if info.inserted {
                self.stats
                    .inc(StatType::ActiveElections, DetailType::Started);
                self.stats
                    .inc(StatType::ActiveElectionsStarted, election_behavior.into());

                debug!(behavior = ?election_behavior, block = %hash, "Started new election");
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

    pub fn iter_batch_by_root<'a, 'b, T>(
        &'a self,
        roots: impl IntoIterator<Item = (QualifiedRoot, &'b T)>,
        mut handle: impl FnMut(QualifiedRoot, Option<&Arc<Mutex<Election>>>, &'b T),
    ) where
        T: 'b,
    {
        let guard = self.container.lock().unwrap();
        for (root, context) in roots.into_iter() {
            let election = guard.election_for_root(&root);
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

pub(crate) type ErasedCallback = Box<dyn Fn(&QualifiedRoot) + Send + Sync>;
