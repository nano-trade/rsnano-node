use std::{
    collections::HashMap,
    ops::Deref,
    time::{Duration, SystemTime},
};

use rsnano_core::{
    utils::{BackpressureSender, BlockPriority, ContainerInfo, UnixMillisTimestamp},
    Amount, Block, BlockHash, PublicKey, QualifiedRoot, SavedBlock, VoteCode, VoteSource,
};
use rsnano_nullable_clock::Timestamp;
use rsnano_stats::{StatsCollection, StatsSource};

use crate::consensus::election::{
    AddForkResult, ConfirmedElection, Election, ElectionBehavior, ElectionResult, VoteSummary,
};

use super::{
    cooldown_controller::{AecCooldownReason, CooldownController},
    recently_confirmed_cache::RecentlyConfirmedCache,
    stopped_counter::StoppedCounter,
    vote_counter::VoteCounter,
    vote_router::VoteRouter,
    ActiveElectionsConfig, AecEvent, AecInsertError, Entry, RootContainer,
};

pub struct ActiveElectionsContainer {
    roots: RootContainer,
    observer: Option<BackpressureSender<AecEvent>>,
    stopped: bool,
    manual_count: usize,
    priority_count: usize,
    hinted_count: usize,
    optimistic_count: usize,
    base_latency: Duration,
    vote_router: VoteRouter,
    recently_confirmed: RecentlyConfirmedCache,
    cooldown: CooldownController,
    max_elections: usize,
    vote_counter: VoteCounter,
    stopped_counter: StoppedCounter,
    ticked: u64,
    cooldown_count: u64,
    recover_count: u64,
    conflict_counter: u64,
}

impl ActiveElectionsContainer {
    pub fn new(config: ActiveElectionsConfig, base_latency: Duration) -> Self {
        Self {
            roots: RootContainer::default(),
            vote_router: VoteRouter::new(),
            observer: None,
            stopped: false,
            manual_count: 0,
            priority_count: 0,
            hinted_count: 0,
            optimistic_count: 0,
            base_latency,
            recently_confirmed: RecentlyConfirmedCache::new(config.confirmation_cache),
            cooldown: CooldownController::new(),
            max_elections: config.max_elections,
            vote_counter: VoteCounter::new(),
            stopped_counter: StoppedCounter::new(),
            ticked: 0,
            cooldown_count: 0,
            recover_count: 0,
            conflict_counter: 0,
        }
    }

    pub fn set_observer(&mut self, observer: BackpressureSender<AecEvent>) {
        self.observer = Some(observer);
    }

    pub fn max_len(&self) -> usize {
        self.max_elections
    }

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

    pub fn iter(&self) -> impl Iterator<Item = &Election> {
        self.roots.iter_sequenced().map(|i| &i.election)
    }

    pub(super) fn insert(
        &mut self,
        block: SavedBlock,
        election_behavior: ElectionBehavior,
        priority: Option<BlockPriority>,
        now: Timestamp,
    ) -> Result<bool, AecInsertError> {
        if self.stopped {
            return Err(AecInsertError::Stopped);
        }

        let hash = block.hash();
        let root = block.qualified_root();

        if self.recently_confirmed.root_exists(&root) {
            // This block or a fork got recently confirmed, so there is no need for a new election.
            return Err(AecInsertError::RecentlyConfirmed);
        }

        let existing = self.roots.get_mut(&root);

        if let Some(existing) = existing {
            // Try upgrading to priority election to enable immediate vote broadcasting.
            let previous_behavior = existing.election.behavior();
            let upgraded = existing.election.maybe_upgrade_to(election_behavior);
            if upgraded {
                existing.priority = priority;
                *self.count_by_behavior_mut(previous_behavior) -= 1;
                *self.count_by_behavior_mut(election_behavior) += 1;
            }
            return Ok(false);
        }

        let election = Election::new(block, election_behavior, self.base_latency, now);

        self.roots.insert(Entry {
            root: root.clone(),
            election,
            priority,
        });

        // Keep track of election count by election type
        *self.count_by_behavior_mut(election_behavior) += 1;
        self.vote_router.connect(hash, root);
        Ok(true)
    }

    pub fn try_add_fork(&mut self, fork: &Block, fork_tally: Amount) -> bool {
        let Some(entry) = self.roots.get_mut(&fork.qualified_root()) else {
            return false;
        };

        let result = entry.election.try_add_fork(fork, fork_tally);
        let added = match result {
            AddForkResult::Added => {
                self.notify(AecEvent::BlockAddedToElection(fork.hash()));
                true
            }
            AddForkResult::Replaced(removed) => {
                self.vote_router.disconnect(&removed.hash());
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

        if added {
            self.vote_router.connect(fork.hash(), fork.qualified_root());
            self.conflict_counter += 1;
        }

        added
    }

    /// How many election slots are available
    /// This is a soft limit and can be negative!
    pub fn vacancy(&self) -> i64 {
        if self.cooldown.is_cooling_down() {
            return 0;
        }
        let current_size = self.roots.len() as i64;
        self.max_elections as i64 - current_size
    }

    pub fn set_cooldown(&mut self, cool_down: bool, reason: AecCooldownReason) {
        let was_cooling_down_before = self.cooldown.is_cooling_down();
        self.cooldown.set_cooldown(cool_down, reason);
        let cooling_down = self.cooldown.is_cooling_down();

        if cooling_down && !was_cooling_down_before {
            self.cooldown_count += 1;
        }

        let recovered = !cooling_down && was_cooling_down_before;
        if recovered {
            self.recover_count += 1;
            self.notify(AecEvent::VacancyUpdated);
        }
    }

    pub(super) fn stop(&mut self) {
        // destroy send queue so that the receiver thread will be stopped too
        drop(self.observer.take());
        self.stopped = true;
        self.roots.clear();
    }

    pub fn is_active_root(&self, root: &QualifiedRoot) -> bool {
        self.roots.get(root).is_some()
    }

    pub fn is_active_hash(&self, block_hash: &BlockHash) -> bool {
        self.vote_router.is_active(block_hash)
    }

    pub fn was_recently_confirmed(&self, block_hash: &BlockHash) -> bool {
        self.recently_confirmed.hash_exists(block_hash)
    }

    pub fn clear_recently_confirmed(&mut self) {
        self.recently_confirmed.clear();
    }

    pub fn transition_time(&mut self, now: Timestamp) {
        self.ticked += 1;
        for entry in self.roots.iter_mut() {
            entry.election.transition_time(now);
        }
    }

    pub fn election_for_root(&self, root: &QualifiedRoot) -> Option<&Election> {
        self.roots.get(root).map(|i| &i.election)
    }

    pub fn election_for_root_mut(&mut self, root: &QualifiedRoot) -> Option<&mut Election> {
        self.roots.get_mut(root).map(|i| &mut i.election)
    }

    pub fn election_for_block(&self, block_hash: &BlockHash) -> Option<&Election> {
        let root = self.vote_router.qualified_root(block_hash)?;
        self.election_for_root(&root)
    }

    pub(super) fn election_for_block_mut(
        &mut self,
        block_hash: &BlockHash,
    ) -> Option<&mut Election> {
        let root = self.vote_router.qualified_root(block_hash)?;
        self.roots.get_mut(&root).map(|i| &mut i.election)
    }

    pub fn info(&self) -> ActiveElectionsInfo {
        ActiveElectionsInfo {
            max_elections: self.max_elections,
            total: self.roots.len(),
            priority: self.priority_count,
            hinted: self.hinted_count,
            optimistic: self.optimistic_count,
        }
    }

    pub fn erase_ended_elections(&mut self) {
        let removed = self.roots.drain_filter(|i| i.election.state().has_ended());

        let something_removed = removed.len() > 0;

        for entry in removed {
            self.cleanup_election(entry);
        }

        if something_removed {
            self.notify(AecEvent::VacancyUpdated);
        }
    }

    pub fn erase(&mut self, root: &QualifiedRoot) -> bool {
        let Some(entry) = self.roots.erase(root) else {
            return false;
        };
        self.cleanup_election(entry);
        self.notify(AecEvent::VacancyUpdated);
        true
    }

    fn cleanup_election(&mut self, entry: Entry) {
        let election = &entry.election;

        // Keep track of election count by election type
        *self.count_by_behavior_mut(election.behavior()) -= 1;

        self.stopped_counter.stopped(&entry.election);
        self.vote_router.disconnect_election(&election);
        self.notify(AecEvent::ElectionEnded(entry.election, entry.priority));
    }

    pub fn confirm_dependent_elections(
        &mut self,
        confirmed_blocks: Vec<(SavedBlock, Option<ConfirmedElection>)>,
        now: Timestamp,
    ) -> Vec<ConfirmedElection> {
        let mut results = Vec::new();

        for (block, source_election) in confirmed_blocks {
            let mut dependent_election = self.roots.get_mut(&block.qualified_root());

            // Distinguishes replay votes, cannot be determined if the block is not in any election
            // Dependent elections are implicitly confirmed when their block is cemented
            if let Some(election) = &mut dependent_election {
                // TRY CONFIRM
                // TODO: This should either confirm or cancel the election
                let winner_hash = election.election.winner().hash();
                if winner_hash == block.hash() {
                    election.election.force_confirm();
                }
            }

            // Check if the currently confirmed block was part of an election that triggered the confirmation
            if let Some(source) = source_election {
                if source.winner.hash() == block.hash() {
                    // This is the block that was directly confirmed by the source election.
                    // The election is already confirmed, so there is nothing to do.
                    continue;
                }
            }

            let mut confirmed_election = ConfirmedElection::new(block.clone());
            let mut handled = false;
            if let Some(dep_el) = dependent_election {
                if dep_el.election.is_confirmed() {
                    confirmed_election = dep_el
                        .election
                        .into_confirmed_election(now, ElectionResult::ActiveConfirmationHeight);
                    handled = true;
                }
            }

            if !handled {
                confirmed_election.result = ElectionResult::InactiveConfirmationHeight;
            }

            results.push(confirmed_election);
        }
        results
    }

    pub(super) fn remove_recently_confirmed(&mut self, block_hash: &BlockHash) {
        self.recently_confirmed.erase(block_hash);
    }

    pub fn len(&self) -> usize {
        self.roots.len()
    }

    pub fn container_info(&self) -> ContainerInfo {
        ContainerInfo::builder()
            .leaf("roots", self.roots.len(), RootContainer::ELEMENT_SIZE)
            .leaf(
                "normal",
                self.count_by_behavior(ElectionBehavior::Priority),
                0,
            )
            .leaf(
                "hinted".to_string(),
                self.count_by_behavior(ElectionBehavior::Hinted),
                0,
            )
            .leaf(
                "optimistic".to_string(),
                self.count_by_behavior(ElectionBehavior::Optimistic),
                0,
            )
            .node(
                "recently_confirmed",
                self.recently_confirmed.container_info(),
            )
            .node("vote_router", self.vote_router.container_info())
            .finish()
    }

    /// Calculates minimum time delay between subsequent votes when processing non-final votes
    pub fn cooldown_time(rep_weight: Amount, online_weight: Amount) -> Duration {
        if rep_weight > online_weight / 20 {
            // Reps with more than 5% weight
            Duration::from_secs(1)
        } else if rep_weight > online_weight / 100 {
            // Reps with more than 1% weight
            Duration::from_secs(5)
        } else {
            // The rest of smaller reps
            Duration::from_secs(15)
        }
    }

    pub fn apply_votes(
        &mut self,
        voter: PublicKey,
        votes: impl IntoIterator<Item = VoteSummary>,
        source: VoteSource,
        rep_weights: &HashMap<PublicKey, Amount>,
        online_weight: Amount,
        quorum_delta: Amount,
        now: Timestamp,
    ) -> (HashMap<BlockHash, VoteCode>, Vec<AecEvent>) {
        let mut results = HashMap::new();
        let mut events = Vec::new();
        let mut vote_counted = false;

        for vote_summary in votes {
            // Ignore duplicate hashes (should not happen with a well-behaved voting node)
            if results.contains_key(&vote_summary.hash) {
                continue;
            }

            let root = self.vote_router.qualified_root(&vote_summary.hash);
            if let Some(root) = root {
                let entry = self.roots.get_mut(&root).unwrap();
                let election = &mut entry.election;

                let mut vote_code = VoteCode::Invalid;
                let rep_weight = rep_weights
                    .get(&vote_summary.voter)
                    .cloned()
                    .unwrap_or_default();

                if vote_code == VoteCode::Invalid {
                    if let Some(last_vote) = election.votes().get(&vote_summary.voter) {
                        if last_vote.timestamp > vote_summary.timestamp {
                            vote_code = VoteCode::Replay;
                        } else if last_vote.timestamp == vote_summary.timestamp
                            && !(last_vote.hash < vote_summary.hash)
                        {
                            vote_code = VoteCode::Replay;
                        }

                        if vote_code == VoteCode::Invalid {
                            let max_vote = vote_summary.timestamp == UnixMillisTimestamp::MAX
                                && last_vote.timestamp < vote_summary.timestamp;

                            let mut past_cooldown = true;
                            // Only cooldown live votes
                            if source != VoteSource::Cache {
                                let cooldown = ActiveElectionsContainer::cooldown_time(
                                    rep_weight,
                                    online_weight,
                                );
                                past_cooldown = last_vote.time <= SystemTime::now() - cooldown;
                            }

                            if !max_vote && !past_cooldown {
                                vote_code = VoteCode::Ignored;
                            }
                        }
                    }

                    if vote_code == VoteCode::Invalid {
                        election.add_vote(
                            vote_summary.voter,
                            vote_summary.timestamp,
                            vote_summary.hash,
                        );

                        self.vote_counter.count(source);
                        if !vote_counted {
                            // send vote counted event only once!
                            vote_counted = true;
                            events.push(AecEvent::VoteCounted(voter, source));
                        }

                        // CONFIRM IF QUORUM:
                        if !election.is_confirmed() {
                            let old_winner = election.winner().hash();
                            let old_final = election.is_final();

                            election.update_tallies(&rep_weights, quorum_delta);

                            let winner_changed = election.winner().hash() != old_winner;
                            if winner_changed {
                                events.push(AecEvent::WinnerChanged(
                                    old_winner,
                                    election.winner().deref().clone(),
                                ));
                            }

                            if election.is_final() {
                                if !old_final {
                                    events.push(AecEvent::FinalPhaseStarted(
                                        election.winner().hash(),
                                        election.qualified_root().clone(),
                                    ));
                                }
                                if election.is_confirmed() {
                                    self.recently_confirmed.put(
                                        election.qualified_root().clone(),
                                        election.winner().hash(),
                                    );

                                    let confirmed_election = election.into_confirmed_election(
                                        now,
                                        ElectionResult::ActiveConfirmedQuorum,
                                    );
                                    events.push(AecEvent::ElectionConfirmed(confirmed_election));
                                }
                            }
                        }

                        vote_code = VoteCode::Vote;
                    }
                }

                results.insert(vote_summary.hash, vote_code);
            } else {
                if self.was_recently_confirmed(&vote_summary.hash) {
                    results.insert(vote_summary.hash, VoteCode::Late);
                } else {
                    results.insert(vote_summary.hash, VoteCode::Indeterminate);
                }
            }
        }

        (results, events)
    }

    pub fn force_confirm(&mut self, block_hash: &BlockHash, now: Timestamp) -> Option<AecEvent> {
        let root = self.vote_router.qualified_root(block_hash)?;
        let entry = self.roots.get_mut(&root)?;
        let election = &mut entry.election;
        if election.force_confirm() {
            let confirmed_election =
                election.into_confirmed_election(now, ElectionResult::ActiveConfirmedQuorum);
            Some(AecEvent::ElectionConfirmed(confirmed_election))
        } else {
            None
        }
    }

    pub fn cancel(&mut self, root: &QualifiedRoot) {
        if let Some(entry) = self.roots.get_mut(root) {
            entry.election.cancel();
        }
    }

    fn notify(&self, event: AecEvent) {
        if let Some(sender) = &self.observer {
            sender.send(event).unwrap()
        }
    }
}

impl StatsSource for ActiveElectionsContainer {
    fn collect_stats(&self, result: &mut StatsCollection) {
        result.insert("active_elections", "loop", self.ticked);
        result.insert("active_elections", "cooldown", self.cooldown_count);
        result.insert("active_elections", "recovered", self.recover_count);
        result.insert("active_elections", "block_conflict", self.conflict_counter);

        self.vote_counter.collect_stats(result);
        self.stopped_counter.collect_stats(result);
    }
}

#[derive(Default)]
pub struct ActiveElectionsInfo {
    pub max_elections: usize,
    pub total: usize,
    pub priority: usize,
    pub hinted: usize,
    pub optimistic: usize,
}
