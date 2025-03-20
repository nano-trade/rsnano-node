mod active_elections_container;
mod recently_confirmed_cache;
mod root_container;

pub use active_elections_container::*;

use std::{
    collections::HashMap,
    sync::{mpsc::SyncSender, Arc, Mutex, RwLock, RwLockReadGuard},
    time::SystemTime,
};

use root_container::{Entry, RootContainer};
use rsnano_nullable_clock::SteadyClock;
use tracing::debug;

use rsnano_core::{
    Amount, Block, BlockHash, MaybeSavedBlock, PublicKey, QualifiedRoot, SavedBlock, VoteCode,
    VoteSource,
};
use rsnano_stats::{DetailType, Sample, StatType, Stats};

use super::{
    AddForkResult, Election, ElectionBehavior, ElectionConfig, EndedElection, VoteRouter,
    VoteSummary,
};

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
    ElectionStopped(BlockHash),
    BlockAddedToElection(BlockHash),
    BlockDiscarded(Block),
    VacancyUpdated,
}

pub struct ActiveElections {
    container: RwLock<ActiveElectionsContainer>,
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
            container: RwLock::new(ActiveElectionsContainer::new(config, election_config)),
            max_elections,
            stats,
            clock,
            event_sender: RwLock::new(None),
        }
    }

    pub fn set_event_sink(&self, sink: SyncSender<AecEvent>) {
        *self.event_sender.write().unwrap() = Some(sink);
    }

    pub fn read(&self) -> RwLockReadGuard<ActiveElectionsContainer> {
        self.container.read().unwrap()
    }

    pub fn len(&self) -> usize {
        self.container.read().unwrap().len()
    }

    pub fn max_len(&self) -> usize {
        self.max_elections
    }

    pub fn cool_down(&self) {
        self.container.write().unwrap().cool_down();
    }

    pub fn resume(&self) {
        self.container.write().unwrap().resume();
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

    pub fn election_for_block(&self, block_hash: &BlockHash) -> Option<Arc<Mutex<Election>>> {
        self.container
            .read()
            .unwrap()
            .election_for_block(block_hash)
            .cloned()
    }

    pub fn erase(&self, root: &QualifiedRoot) -> bool {
        let removed = self.container.write().unwrap().erase(root);

        if let Some(entry) = &removed {
            self.add_stats(entry);
            self.notify(AecEvent::VacancyUpdated);

            let election = entry.election.lock().unwrap();
            let winner_hash = election.winner().hash();
            let is_confirmed = election.is_confirmed();
            let blocks = election.candidate_blocks().clone();
            drop(election);

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

        removed.is_some()
    }

    fn add_stats(&self, entry: &Entry) {
        let election = entry.election.lock().unwrap();
        let is_confirmed = election.is_confirmed();
        let state = election.state();
        let behavior = election.behavior();
        let start = election.start();
        let root = election.qualified_root().clone();
        drop(election);

        self.stats
            .inc(StatType::ActiveElections, DetailType::Stopped);

        self.stats.inc(
            StatType::ActiveElections,
            if is_confirmed {
                DetailType::Confirmed
            } else {
                DetailType::Unconfirmed
            },
        );
        self.stats
            .inc(StatType::ActiveElectionsStopped, state.into());
        self.stats.inc(state.into(), behavior.into());
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
    ) -> Option<ElectionInsertInfo> {
        let hash = block.hash();

        let result = self.container.write().unwrap().insert(
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

    pub fn cementing_failed(&self, block_hash: &BlockHash) {
        self.container.write().unwrap().cementing_failed(block_hash);
    }

    pub fn stop(&self) {
        self.container.write().unwrap().stop();
        // destroy send queue so that the receiver thread will be stopped too
        drop(self.event_sender.write().unwrap().take());
    }

    fn notify(&self, event: AecEvent) {
        if let Some(sender) = self.event_sender.read().unwrap().as_ref() {
            sender.send(event).unwrap()
        }
    }

    pub fn transition_active_hash(&self, block_hash: &BlockHash) -> bool {
        let container = self.container.write().unwrap();
        let Some(election) = container.election_for_block(block_hash) else {
            return false;
        };
        election.lock().unwrap().transition_active();
        true
    }

    // TODO: Delete!
    pub fn transition_active(&self, root: &QualifiedRoot) {
        self.container
            .write()
            .unwrap()
            .election_for_root(root)
            .unwrap()
            .lock()
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
            .election_for_root(root)
            .expect("No election found for given root")
            .lock()
            .unwrap()
            .change_vote_timestamp(voter, new_timestamp);
    }

    pub fn batch_cemented(
        &self,
        batch: Vec<(SavedBlock, Option<EndedElection>)>,
    ) -> Vec<EndedElection> {
        let now = self.clock.now();
        self.container.read().unwrap().batch_cemented(batch, now)
    }

    pub fn remove_votes<'a>(
        &self,
        root: &QualifiedRoot,
        voters: impl IntoIterator<Item = &'a PublicKey>,
    ) {
        let container = self.container.write().unwrap();
        let Some(election_mutex) = container.election_for_root(root) else {
            return;
        };
        let mut election = election_mutex.lock().unwrap();
        for voter in voters {
            election.remove_vote(voter);
        }
    }

    pub fn apply_votes(
        &self,
        votes: impl IntoIterator<Item = VoteSummary>,
        source: VoteSource,
        rep_weights: &HashMap<PublicKey, Amount>,
        online_weight: Amount,
        quorum_delta: Amount,
    ) -> Vec<ApplyVoteResult> {
        self.container.write().unwrap().apply_votes(
            votes,
            source,
            rep_weights,
            online_weight,
            quorum_delta,
            self.clock.now(),
        )
    }
}

impl Drop for ActiveElections {
    fn drop(&mut self) {
        self.stop()
    }
}

pub(crate) type ErasedCallback = Box<dyn Fn(&QualifiedRoot) + Send + Sync>;

pub struct ApplyVoteResult {
    pub voted_block: BlockHash,
    pub vote_result: VoteCode,
    pub got_confirmed: Option<EndedElection>,
    pub final_phase_started: Option<MaybeSavedBlock>,
    pub winner_changed: Option<(BlockHash, MaybeSavedBlock)>,
}

impl ApplyVoteResult {
    pub fn new(voted_block: BlockHash, vote_result: VoteCode) -> Self {
        Self {
            voted_block,
            vote_result,
            got_confirmed: None,
            final_phase_started: None,
            winner_changed: None,
        }
    }
}
