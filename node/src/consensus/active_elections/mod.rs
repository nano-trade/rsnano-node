mod root_container;

use std::{
    cmp::min,
    sync::{mpsc::SyncSender, Arc, Condvar, Mutex, MutexGuard, RwLock},
};

use root_container::{Entry, RootContainer};
use tracing::{debug, trace};

use rsnano_core::{
    utils::ContainerInfo, Amount, Block, BlockHash, MaybeSavedBlock, QualifiedRoot, SavedBlock,
    VoteWithWeightInfo,
};
use rsnano_ledger::{
    BlockStatus, CementingEntry, Election, ElectionBehavior, ElectionConfig, ElectionStatus,
    ElectionStatusType, RepWeightCache,
};
use rsnano_messages::{Message, Publish};
use rsnano_network::TrafficType;
use rsnano_stats::{DetailType, Direction, Sample, StatType, Stats};

use super::{ElectionVoter, RecentlyConfirmedCache, VoteApplier, VoteCache, VoteRouter};
use crate::{
    block_processing::BlockContext, cementation::ConfirmingSet, config::NodeConfig,
    consensus::VoteApplierExt, transport::MessageFlooder,
};

pub type ElectionEndCallback =
    Box<dyn Fn(&ElectionStatus, &Vec<VoteWithWeightInfo>, &SavedBlock, Amount) + Send + Sync>;

#[derive(Clone, Debug, PartialEq)]
pub struct ActiveElectionsConfig {
    /// Maximum number of simultaneous active elections (AEC size)
    pub size: usize,
    /// Limit of hinted elections as percentage of `active_elections_size`
    pub hinted_limit_percentage: usize,
    /// Limit of optimistic elections as percentage of `active_elections_size`
    pub optimistic_limit_percentage: usize,
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
            hinted_limit_percentage: 20,
            optimistic_limit_percentage: 10,
            confirmation_history_size: 2048,
            confirmation_cache: 65536,
            max_election_winners: 1024 * 16,
        }
    }
}

pub enum AecEvent {
    ActiveStarted(BlockHash),
    ActiveStopped(BlockHash),
    BlockCemented(SavedBlock, ElectionStatus, Vec<VoteWithWeightInfo>),
    BlockAddedToElection(BlockHash),
    UnconfirmedBlockRemoved(Block),
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
    pub vote_applier: Arc<VoteApplier>,
    vote_router: Arc<VoteRouter>,
    message_flooder: Mutex<MessageFlooder>,
    event_sender: RwLock<Option<SyncSender<AecEvent>>>,
    election_voter: ElectionVoter,
}

impl ActiveElections {
    pub(crate) fn new(
        node_config: NodeConfig,
        rep_weights: Arc<RepWeightCache>,
        confirming_set: Arc<ConfirmingSet>,
        vote_cache: Arc<Mutex<VoteCache>>,
        stats: Arc<Stats>,
        recently_confirmed: Arc<RwLock<RecentlyConfirmedCache>>,
        vote_applier: Arc<VoteApplier>,
        vote_router: Arc<VoteRouter>,
        message_flooder: MessageFlooder,
        election_voter: ElectionVoter,
        election_config: ElectionConfig,
    ) -> Self {
        Self {
            mutex: Mutex::new(ActiveElectionsState {
                roots: RootContainer::default(),
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
            config: node_config.active_elections.clone(),
            vote_cache,
            stats,
            vote_applier,
            vote_router,
            message_flooder: Mutex::new(message_flooder),
            event_sender: RwLock::new(None),
            election_voter,
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

    /// Maximum number of elections that should be present in this container
    /// NOTE: This is only a soft limit, it is possible for this container to exceed this count
    pub fn limit(&self, behavior: ElectionBehavior) -> usize {
        match behavior {
            ElectionBehavior::Manual => usize::MAX,
            ElectionBehavior::Priority => self.config.size,
            ElectionBehavior::Hinted => {
                self.config.hinted_limit_percentage * self.config.size / 100
            }
            ElectionBehavior::Optimistic => {
                self.config.optimistic_limit_percentage * self.config.size / 100
            }
        }
    }

    /// How many election slots are available for specified election type
    pub fn vacancy(&self, behavior: ElectionBehavior) -> i64 {
        let election_vacancy = self.election_vacancy(behavior);
        let winners_vacancy = self.election_winners_vacancy();
        min(election_vacancy, winners_vacancy)
    }

    fn election_vacancy(&self, behavior: ElectionBehavior) -> i64 {
        let guard = self.mutex.lock().unwrap();
        match behavior {
            ElectionBehavior::Manual => i64::MAX,
            ElectionBehavior::Priority => {
                self.limit(ElectionBehavior::Priority) as i64 - guard.roots.len() as i64
            }
            ElectionBehavior::Hinted | ElectionBehavior::Optimistic => {
                self.limit(behavior) as i64 - guard.count_by_behavior(behavior) as i64
            }
        }
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

    pub fn active_root(&self, root: &QualifiedRoot) -> bool {
        let guard = self.mutex.lock().unwrap();
        guard.roots.get(root).is_some()
    }

    pub fn active(&self, block: &Block) -> bool {
        self.active_root(&block.qualified_root())
    }

    fn get_cached_tally(&self, hash: &BlockHash) -> Amount {
        let votes = self.vote_cache.lock().unwrap().find(hash);
        let mut tally = Amount::zero();
        let weights = self.rep_weights.read();
        for vote in votes {
            tally += weights
                .get(&vote.voting_account)
                .cloned()
                .unwrap_or_default();
        }
        tally
    }

    /// Broadcasts vote for the current winner of this election
    /// Checks if sufficient amount of time (`vote_generation_interval`) passed since the last vote generation
    pub fn try_generate_vote(&self, election: &mut Election) {
        self.election_voter.try_vote(election);
    }

    /// Erase all blocks from active, if not confirmed, clear digests from network filters
    fn cleanup_election<'a>(
        &self,
        mut guard: MutexGuard<'a, ActiveElectionsState>,
        election_mutex: &'a Arc<Mutex<Election>>,
    ) {
        // Keep track of election count by election type
        *guard.count_by_behavior_mut(election_mutex.lock().unwrap().behavior) -= 1;

        let election_winner: BlockHash;
        let election_state;
        let blocks;
        {
            let election_guard = election_mutex.lock().unwrap();
            blocks = election_guard.last_blocks.clone();
            election_winner = election_guard.winner_hash().unwrap();
            election_state = election_guard.state;
        }

        self.vote_router.disconnect_election(election_mutex);

        let election = election_mutex.lock().unwrap();
        // Erase root info
        let entry = guard
            .roots
            .erase(election.qualified_root())
            .expect("election not found");

        let state = election.state;
        drop(election);

        self.stats
            .inc(StatType::ActiveElections, DetailType::Stopped);

        self.stats.inc(
            StatType::ActiveElections,
            if state.is_confirmed() {
                DetailType::Confirmed
            } else {
                DetailType::Unconfirmed
            },
        );
        self.stats
            .inc(StatType::ActiveElectionsStopped, state.into());
        self.stats
            .inc(state.into(), election_mutex.lock().unwrap().behavior.into());

        debug!(
            "Erased election for blocks: {} (behavior: {:?}, state: {:?})",
            blocks
                .keys()
                .map(|k| k.to_string())
                .collect::<Vec<_>>()
                .join(", "),
            election_mutex.lock().unwrap().behavior,
            election_state
        );
        drop(guard);

        // Track election duration
        let election_duration;
        let qualified_root;
        {
            let el = election_mutex.lock().unwrap();
            election_duration = el.duration();
            qualified_root = el.qualified_root().clone();
        }

        self.stats.sample(
            Sample::ActiveElectionDuration,
            election_duration.as_millis() as i64,
            (0, 1000 * 60 * 10),
        ); // 0-10 minutes range

        // Notify observers without holding the lock
        if let Some(callback) = entry.erased_callback {
            callback(&qualified_root);
        }

        self.notify(AecEvent::VacancyUpdated);

        let is_confirmed = election_mutex.lock().unwrap().is_confirmed();
        for (hash, block) in blocks {
            // Notify observers about dropped elections & blocks lost confirmed elections
            if !is_confirmed || hash != election_winner {
                self.notify(AecEvent::ActiveStopped(hash));
            }

            if !is_confirmed {
                self.notify(AecEvent::UnconfirmedBlockRemoved(block.into()));
            }
        }
    }

    pub fn election(&self, root: &QualifiedRoot) -> Option<Arc<Mutex<Election>>> {
        self.mutex.lock().unwrap().election(root)
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
        let guard = self.mutex.lock().unwrap();
        if let Some(entry) = guard.roots.get(root) {
            let election = entry.election.clone();
            self.cleanup_election(guard, &election);
            true
        } else {
            false
        }
    }

    fn try_confirm(&self, election_mutex: &Arc<Mutex<Election>>, hash: &BlockHash) {
        let mut election = election_mutex.lock().unwrap();
        if let Some(winner_hash) = &election.winner_hash() {
            if winner_hash == hash {
                if !election.is_confirmed() {
                    self.vote_applier
                        .confirm_once(&mut election, election_mutex);
                }
            }
        }
    }

    pub fn force_confirm(&self, election: &Arc<Mutex<Election>>) {
        let mut guard = election.lock().unwrap();
        self.vote_applier.confirm_once(&mut guard, election);
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

        let result = self
            .mutex
            .lock()
            .unwrap()
            .insert(block, election_behavior, erased_callback);

        if let Some(info) = &result {
            if info.inserted {
                self.vote_router
                    .connect(hash, Arc::downgrade(&info.election));

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
            drop(guard);
            let added = {
                let mut election = election_mutex.lock().unwrap();
                self.try_add_fork(&mut election, fork)
            };
            if added {
                guard = self.mutex.lock().unwrap();
                self.vote_router
                    .connect(fork.hash(), Arc::downgrade(&election_mutex));
                drop(guard);

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
    fn try_add_fork(&self, election: &mut Election, fork: &Block) -> bool {
        // Do not insert new blocks if already confirmed
        if election.is_confirmed() {
            return false;
        }

        if election.last_blocks.len() >= Election::MAX_BLOCKS
            && !election.last_blocks.contains_key(&fork.hash())
        {
            let fork_tally = self.get_cached_tally(&fork.hash());
            let removed = election.remove_tally_below(fork_tally);
            if let Some(removed) = removed {
                self.vote_router.disconnect(&removed.hash());
                self.notify(AecEvent::UnconfirmedBlockRemoved(removed.into()));
            } else {
                self.notify(AecEvent::UnconfirmedBlockRemoved(fork.clone()));
                return false;
            }
        }

        if election.last_blocks.get(&fork.hash()).is_some() {
            election
                .last_blocks
                .insert(fork.hash(), MaybeSavedBlock::Unsaved(fork.clone()));

            if election.winner_hash().unwrap() == fork.hash() {
                election.set_winner(MaybeSavedBlock::Unsaved(fork.clone()));
                let message = Message::Publish(Publish::new_forward(fork.clone()));
                let mut publisher = self.message_flooder.lock().unwrap();
                publisher.flood(&message, TrafficType::BlockBroadcast, 1.0);
            }

            return false;
        }

        election
            .last_blocks
            .insert(fork.hash(), MaybeSavedBlock::Unsaved(fork.clone()));

        true
    }

    /// Cementing blocks might implicitly confirm dependent elections
    pub fn batch_cemented(&self, cemented: &Vec<(SavedBlock, CementingEntry)>) {
        let mut results = Vec::new();
        {
            let mut guard = self.mutex.lock().unwrap();
            // Process all cemented blocks while holding the lock to avoid
            // races where an election for a block that is already
            // cemented is inserted
            for (block, entry) in cemented {
                let result = self.block_cemented(&mut guard, block, &entry.hash, &entry.election);
                results.push(result)
            }
        }

        // TODO: This could be offloaded to a separate notification worker, profiling is needed
        for (status, votes) in results {
            self.notify_block_cemented(status, votes);
        }
    }

    /// Distinguishes replay votes, cannot be determined if the block is not in any election
    fn block_cemented(
        &self,
        guard: &mut ActiveElectionsState,
        block: &SavedBlock,
        confirmation_root: &BlockHash,
        source_election: &Option<Arc<Mutex<Election>>>,
    ) -> (ElectionStatus, Vec<VoteWithWeightInfo>) {
        // Dependent elections are implicitly confirmed when their block is cemented
        let dependent_election = guard.election(&block.qualified_root());
        if let Some(dependent_election) = &dependent_election {
            self.stats
                .inc(StatType::ActiveElections, DetailType::ConfirmDependent);

            // TODO: This should either confirm or cancel the election
            self.try_confirm(&dependent_election, &block.hash());
        }

        let mut status = ElectionStatus::default();
        let mut votes = Vec::new();
        status.winner = Some(MaybeSavedBlock::Saved(block.clone()));

        // Check if the currently cemented block was part of an election that triggered the confirmation
        let mut handled = false;
        if let Some(source_election) = source_election {
            let election = source_election.lock().unwrap();
            if *election.qualified_root() == block.qualified_root() {
                status = election.status.clone();
                debug_assert_eq!(status.winner.as_ref().unwrap().hash(), block.hash());
                votes = election.votes_with_weight(&self.rep_weights);
                status.election_status_type = ElectionStatusType::ActiveConfirmedQuorum;
                handled = true;
            }
        }

        if handled {
            // already handled
        } else if dependent_election.is_some() {
            status.election_status_type = ElectionStatusType::ActiveConfirmationHeight;
        } else {
            status.election_status_type = ElectionStatusType::InactiveConfirmationHeight;
        }

        self.stats
            .inc(StatType::ActiveElections, DetailType::Cemented);
        self.stats.inc(
            StatType::ActiveElectionsCemented,
            status.election_status_type.into(),
        );

        trace!(?block, %confirmation_root, "active cemented");

        (status, votes)
    }

    fn notify_block_cemented(&self, status: ElectionStatus, votes: Vec<VoteWithWeightInfo>) {
        let block = status.winner.as_ref().unwrap();
        let MaybeSavedBlock::Saved(block) = block else {
            return;
        };
        let block = block.clone();

        match status.election_status_type {
            ElectionStatusType::ActiveConfirmedQuorum => self.stats.inc_dir(
                StatType::ConfirmationObserver,
                DetailType::ActiveQuorum,
                Direction::Out,
            ),
            ElectionStatusType::ActiveConfirmationHeight => self.stats.inc_dir(
                StatType::ConfirmationObserver,
                DetailType::ActiveConfHeight,
                Direction::Out,
            ),
            ElectionStatusType::InactiveConfirmationHeight => self.stats.inc_dir(
                StatType::ConfirmationObserver,
                DetailType::InactiveConfHeight,
                Direction::Out,
            ),
            _ => {}
        }

        self.notify(AecEvent::BlockCemented(block, status, votes));
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
}

impl ActiveElectionsState {
    pub fn count_by_behavior(&self, behavior: ElectionBehavior) -> usize {
        match behavior {
            ElectionBehavior::Manual => self.manual_count,
            ElectionBehavior::Priority => self.priority_count,
            ElectionBehavior::Hinted => self.hinted_count,
            ElectionBehavior::Optimistic => self.optimistic_count,
        }
    }

    pub fn count_by_behavior_mut(&mut self, behavior: ElectionBehavior) -> &mut usize {
        match behavior {
            ElectionBehavior::Manual => &mut self.manual_count,
            ElectionBehavior::Priority => &mut self.priority_count,
            ElectionBehavior::Hinted => &mut self.hinted_count,
            ElectionBehavior::Optimistic => &mut self.optimistic_count,
        }
    }

    pub fn election(&self, root: &QualifiedRoot) -> Option<Arc<Mutex<Election>>> {
        self.roots.get(root).map(|i| i.election.clone())
    }

    pub fn maybe_upgrade_to(
        &mut self,
        new_behavior: ElectionBehavior,
        election: &mut Election,
    ) -> bool {
        let previous_behavior = election.behavior;
        let upgraded = election.maybe_upgrade_to(new_behavior);
        if upgraded {
            *self.count_by_behavior_mut(previous_behavior) -= 1;
            *self.count_by_behavior_mut(new_behavior) += 1;
        }
        upgraded
    }

    pub fn insert(
        &mut self,
        block: SavedBlock,
        election_behavior: ElectionBehavior,
        erased_callback: Option<ErasedCallback>,
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
