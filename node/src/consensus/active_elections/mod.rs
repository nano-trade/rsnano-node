mod root_container;

use std::{
    cmp::min,
    collections::VecDeque,
    mem::size_of,
    ops::Deref,
    sync::{mpsc::SyncSender, Arc, Condvar, Mutex, MutexGuard, RwLock},
};

use bounded_vec_deque::BoundedVecDeque;
use root_container::{Entry, RootContainer};
use tracing::{debug, trace};

use rsnano_core::{
    utils::{ContainerInfo, MemoryStream},
    Amount, Block, BlockHash, MaybeSavedBlock, QualifiedRoot, SavedBlock, Vote, VoteWithWeightInfo,
};
use rsnano_ledger::{BlockStatus, RepWeightCache};
use rsnano_messages::{Message, NetworkFilter, Publish};
use rsnano_network::TrafficType;
use rsnano_stats::{DetailType, Direction, Sample, StatType, Stats};

use super::{
    Election, ElectionBehavior, ElectionConfig, ElectionStatus, ElectionStatusType, ElectionVoter,
    RecentlyConfirmedCache, VoteApplier, VoteCache, VoteCacheProcessor, VoteRouter,
};
use crate::{
    block_processing::BlockContext,
    cementation::{CementingContext, ConfirmingSet},
    config::NodeConfig,
    consensus::VoteApplierExt,
    transport::MessageFlooder,
};

const ELECTION_MAX_BLOCKS: usize = 10;

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
    ElectionEnded(ElectionStatus, Vec<VoteWithWeightInfo>, SavedBlock),
    VacancyUpdated,
}

pub struct ActiveElections {
    mutex: Mutex<ActiveElectionsState>,
    condition: Condvar,
    config: ActiveElectionsConfig,
    rep_weights: Arc<RepWeightCache>,
    confirming_set: Arc<ConfirmingSet>,
    recently_confirmed: Arc<RwLock<RecentlyConfirmedCache>>,
    /// Helper container for storing recently cemented elections (a block from election might be confirmed but not yet cemented by confirmation height processor)
    recently_cemented: Arc<Mutex<BoundedVecDeque<ElectionStatus>>>,
    network_filter: Arc<NetworkFilter>,
    vote_cache: Arc<Mutex<VoteCache>>,
    stats: Arc<Stats>,
    pub vote_applier: Arc<VoteApplier>,
    vote_router: Arc<VoteRouter>,
    vote_cache_processor: Arc<VoteCacheProcessor>,
    message_flooder: Mutex<MessageFlooder>,
    event_sender: RwLock<Option<SyncSender<AecEvent>>>,
    election_voter: ElectionVoter,
}

impl ActiveElections {
    pub(crate) fn new(
        node_config: NodeConfig,
        rep_weights: Arc<RepWeightCache>,
        confirming_set: Arc<ConfirmingSet>,
        network_filter: Arc<NetworkFilter>,
        vote_cache: Arc<Mutex<VoteCache>>,
        stats: Arc<Stats>,
        recently_confirmed: Arc<RwLock<RecentlyConfirmedCache>>,
        vote_applier: Arc<VoteApplier>,
        vote_router: Arc<VoteRouter>,
        vote_cache_processor: Arc<VoteCacheProcessor>,
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
            recently_cemented: Arc::new(Mutex::new(BoundedVecDeque::new(
                node_config.active_elections.confirmation_history_size,
            ))),
            config: node_config.active_elections.clone(),
            network_filter,
            vote_cache,
            stats,
            vote_applier,
            vote_router,
            vote_cache_processor,
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

    pub fn recently_cemented_count(&self) -> usize {
        self.recently_cemented.lock().unwrap().len()
    }

    pub fn insert_recently_cemented(&self, status: ElectionStatus) {
        let MaybeSavedBlock::Saved(block) = status.winner.clone().unwrap() else {
            return;
        };
        self.recently_cemented
            .lock()
            .unwrap()
            .push_back(status.clone());

        self.notify(AecEvent::ElectionEnded(status, Vec::new(), block));
    }

    pub fn recently_cemented_list(&self) -> BoundedVecDeque<ElectionStatus> {
        self.recently_cemented.lock().unwrap().clone()
    }

    //--------------------------------------------------------------------------------

    fn notify_observers(&self, status: ElectionStatus, votes: Vec<VoteWithWeightInfo>) {
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

        self.notify(AecEvent::ElectionEnded(status, votes, block));
    }

    fn clear_publish_filter(&self, block: &Block) {
        let mut buf = MemoryStream::new();
        block.serialize_without_block_type(&mut buf);
        self.network_filter.clear_bytes(buf.as_bytes());
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

    fn replace_by_weight<'a>(
        &self,
        election_mutex: &'a Mutex<Election>,
        mut election: MutexGuard<'a, Election>,
        hash: &BlockHash,
    ) -> (bool, MutexGuard<'a, Election>) {
        let mut replaced_block = BlockHash::zero();
        let winner_hash = election.winner_hash().unwrap();
        // Sort existing blocks tally
        let mut sorted: Vec<_> = election
            .last_tally
            .iter()
            .map(|(hash, amount)| (*hash, *amount))
            .collect();
        drop(election);

        // Sort in ascending order
        sorted.sort_by(|left, right| right.cmp(left));

        let votes_tally = |votes: &[Arc<Vote>]| {
            let mut result = Amount::zero();
            for vote in votes {
                result += self.rep_weights.weight(&vote.voting_account);
            }
            result
        };

        // Replace if lowest tally is below inactive cache new block weight
        let inactive_existing = self.vote_cache.lock().unwrap().find(hash);
        let inactive_tally = votes_tally(&inactive_existing);
        if inactive_tally > Amount::zero() && sorted.len() < ELECTION_MAX_BLOCKS {
            // If count of tally items is less than 10, remove any block without tally
            let guard = election_mutex.lock().unwrap();
            for (hash, _) in &guard.last_blocks {
                if sorted.iter().all(|(h, _)| h != hash) && *hash != winner_hash {
                    replaced_block = *hash;
                    break;
                }
            }
        } else if inactive_tally > Amount::zero() && inactive_tally > sorted.first().unwrap().1 {
            if sorted.first().unwrap().0 != winner_hash {
                replaced_block = sorted[0].0;
            } else if inactive_tally > sorted[1].1 {
                // Avoid removing winner
                replaced_block = sorted[1].0;
            }
        }

        let mut replaced = false;
        if !replaced_block.is_zero() {
            self.vote_router.disconnect(&replaced_block);
            election = election_mutex.lock().unwrap();
            self.remove_block(&mut election, &replaced_block);
            replaced = true;
        } else {
            election = election_mutex.lock().unwrap();
        }
        (replaced, election)
    }

    fn remove_block(&self, election: &mut Election, hash: &BlockHash) {
        if let Some(block) = election.remove_block(hash) {
            self.clear_publish_filter(&block);
        }
    }

    /// Result is true if:
    /// 1) election is confirmed or expired
    /// 2) given election contains 10 blocks & new block didn't receive enough votes to replace existing blocks
    /// 3) given block is already in election & election contains less than 10 blocks (replacing block content with new)
    fn publish(&self, fork: &Block, election_mutex: &Mutex<Election>) -> bool {
        let mut election = election_mutex.lock().unwrap();

        // Do not insert new blocks if already confirmed
        if election.is_confirmed() {
            return true;
        }

        if election.last_blocks.len() >= ELECTION_MAX_BLOCKS
            && !election.last_blocks.contains_key(&fork.hash())
        {
            let (replaced, guard) = self.replace_by_weight(election_mutex, election, &fork.hash());
            election = guard;
            if !replaced {
                self.clear_publish_filter(fork);
                return true;
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

            return true;
        }

        election
            .last_blocks
            .insert(fork.hash(), MaybeSavedBlock::Unsaved(fork.clone()));

        false
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
        election: &'a Arc<Mutex<Election>>,
    ) {
        // Keep track of election count by election type
        *guard.count_by_behavior_mut(election.lock().unwrap().behavior) -= 1;

        let election_winner: BlockHash;
        let election_state;
        let blocks;
        {
            let election_guard = election.lock().unwrap();
            blocks = election_guard.last_blocks.clone();
            election_winner = election_guard.winner_hash().unwrap();
            election_state = election_guard.state;
        }

        self.vote_router.disconnect_election(election);

        let election_guard = election.lock().unwrap();
        // Erase root info
        let entry = guard
            .roots
            .erase(election_guard.qualified_root())
            .expect("election not found");

        let state = election_guard.state;
        drop(election_guard);

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
            .inc(state.into(), election.lock().unwrap().behavior.into());

        debug!(
            "Erased election for blocks: {} (behavior: {:?}, state: {:?})",
            blocks
                .keys()
                .map(|k| k.to_string())
                .collect::<Vec<_>>()
                .join(", "),
            election.lock().unwrap().behavior,
            election_state
        );
        drop(guard);

        // Track election duration
        let election_duration;
        let qualified_root;
        {
            let el = election.lock().unwrap();
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

        let is_confirmed = election.lock().unwrap().is_confirmed();
        for (hash, block) in blocks {
            // Notify observers about dropped elections & blocks lost confirmed elections
            if !is_confirmed || hash != election_winner {
                self.notify(AecEvent::ActiveStopped(hash));
            }

            if !is_confirmed {
                // Clear from publish filter
                self.clear_publish_filter(&block);
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
            let source_election_guard = source_election.lock().unwrap();
            if *source_election_guard.qualified_root() == block.qualified_root() {
                status = source_election_guard.status.clone();
                debug_assert_eq!(status.winner.as_ref().unwrap().hash(), block.hash());
                votes = source_election_guard.votes_with_weight(&self.rep_weights);
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

        self.recently_cemented
            .lock()
            .unwrap()
            .push_back(status.clone());

        self.stats
            .inc(StatType::ActiveElections, DetailType::Cemented);
        self.stats.inc(
            StatType::ActiveElectionsCemented,
            status.election_status_type.into(),
        );

        trace!(?block, %confirmation_root, "active cemented");

        (status, votes)
    }

    pub fn publish_block(&self, fork: &Block) -> bool {
        let mut guard = self.mutex.lock().unwrap();
        let root = fork.qualified_root();
        let mut result = false;
        if let Some(entry) = guard.roots.get(&root) {
            let election = entry.election.clone();
            drop(guard);
            result = self.publish(fork, &election);
            if !result {
                guard = self.mutex.lock().unwrap();
                self.vote_router
                    .connect(fork.hash(), Arc::downgrade(&election));
                drop(guard);

                self.vote_cache_processor.trigger(fork.hash());

                self.stats
                    .inc(StatType::Active, DetailType::ElectionBlockConflict);
                debug!("Block was added to an existing election: {}", fork.hash());
            }
        }

        result
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
                self.publish_block(&context.block);
            }
        }
    }

    pub fn handle_cementations(&self, cemented: &VecDeque<CementingContext>) {
        let mut results = Vec::new();
        {
            let mut guard = self.mutex.lock().unwrap();
            // Process all cemented blocks while holding the lock to avoid
            // races where an election for a block that is already
            // cemented is inserted
            for context in cemented {
                let result = self.block_cemented(
                    &mut guard,
                    &context.block,
                    &context.confirmation_root,
                    &context.election,
                );
                results.push(result)
            }
        }

        // TODO: This could be offloaded to a separate notification worker, profiling is needed
        for (status, votes) in results {
            self.notify_observers(status, votes);
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

        let recently_cemented: ContainerInfo = [(
            "cemented",
            self.recently_cemented.lock().unwrap().len(),
            size_of::<ElectionStatus>(),
        )]
        .into();

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
            .node("recently_cemented", recently_cemented)
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

#[derive(PartialEq, Eq)]
pub struct TallyKey(pub Amount);

impl TallyKey {
    pub fn amount(&self) -> Amount {
        self.0.clone()
    }
}

impl Deref for TallyKey {
    type Target = Amount;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Ord for TallyKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.0.cmp(&self.0)
    }
}

impl PartialOrd for TallyKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        other.0.partial_cmp(&self.0)
    }
}

impl From<Amount> for TallyKey {
    fn from(value: Amount) -> Self {
        Self(value)
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
