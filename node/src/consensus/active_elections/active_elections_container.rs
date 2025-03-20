use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use rsnano_core::{utils::ContainerInfo, Amount, Block, BlockHash, QualifiedRoot, SavedBlock};
use rsnano_nullable_clock::Timestamp;

use crate::consensus::{
    AddForkResult, Election, ElectionBehavior, ElectionConfig, ElectionResult, EndedElection,
};

use super::{
    recently_confirmed_cache::RecentlyConfirmedCache, ActiveElectionsConfig, Entry, ErasedCallback,
    RootContainer, VoteRouter,
};

pub struct ActiveElectionsContainer {
    roots: RootContainer,
    stopped: bool,
    manual_count: usize,
    priority_count: usize,
    hinted_count: usize,
    optimistic_count: usize,
    config: ElectionConfig,
    pub(super) vote_router: VoteRouter,
    pub(super) recently_confirmed: RecentlyConfirmedCache,
    cool_down: bool,
    max_elections: usize,
}

impl ActiveElectionsContainer {
    pub fn new(config: ActiveElectionsConfig, election_config: ElectionConfig) -> Self {
        Self {
            roots: RootContainer::default(),
            vote_router: VoteRouter::new(),
            stopped: false,
            manual_count: 0,
            priority_count: 0,
            hinted_count: 0,
            optimistic_count: 0,
            config: election_config,
            recently_confirmed: RecentlyConfirmedCache::new(config.confirmation_cache),
            cool_down: false,
            max_elections: config.max_elections,
        }
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

    pub fn iter(&self) -> impl Iterator<Item = &Arc<Mutex<Election>>> {
        self.roots.iter_sequenced().map(|i| &i.election)
    }

    pub(super) fn insert(
        &mut self,
        block: SavedBlock,
        election_behavior: ElectionBehavior,
        erased_callback: Option<ErasedCallback>,
        now: Timestamp,
    ) -> Option<ElectionInsertInfo> {
        if self.stopped {
            return None;
        }

        let hash = block.hash();
        let root = block.qualified_root();

        if self.recently_confirmed.root_exists(&root) {
            // This block or a fork got recently confirmed, so there is no need for a new election.
            return None;
        }

        let existing = self.roots.get(&root).map(|i| i.election.clone());

        let result = {
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
        };

        if let Some(info) = &result {
            if info.inserted {
                self.vote_router.connect(hash, info.election.clone());
            }
        }

        result
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

    pub(super) fn try_add_fork(&mut self, fork: &Block, fork_tally: Amount) -> AddForkResult {
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

    /// How many election slots are available
    /// This is a soft limit and can be negative!
    pub fn vacancy(&self) -> i64 {
        if self.cool_down {
            return 0;
        }
        let current_size = self.roots.len() as i64;
        self.max_elections as i64 - current_size
    }

    pub(super) fn cool_down(&mut self) {
        self.cool_down = true;
    }

    pub(super) fn resume(&mut self) {
        self.cool_down = false;
    }

    pub(super) fn stop(&mut self) {
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

    pub fn election_for_root(&self, root: &QualifiedRoot) -> Option<&Arc<Mutex<Election>>> {
        self.roots.get(root).map(|i| &i.election)
    }

    pub fn election_for_block(&self, block_hash: &BlockHash) -> Option<&Arc<Mutex<Election>>> {
        self.vote_router.election(block_hash)
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

    pub(super) fn erase(&mut self, root: &QualifiedRoot) -> Option<Entry> {
        let entry = self.roots.erase(root);
        if let Some(e) = entry {
            self.cleanup_election(&e);
            Some(e)
        } else {
            None
        }
    }

    fn cleanup_election(&mut self, entry: &Entry) {
        let election = entry.election.lock().unwrap();

        // Keep track of election count by election type
        *self.count_by_behavior_mut(election.behavior()) -= 1;
        self.vote_router.disconnect_election(&election);
        let winner_hash = election.winner().hash();
        if election.is_confirmed() {
            self.recently_confirmed
                .put(election.qualified_root().clone(), winner_hash);
        }
    }

    pub fn batch_cemented(
        &self,
        batch: Vec<(SavedBlock, Option<EndedElection>)>,
        now: Timestamp,
    ) -> Vec<EndedElection> {
        let mut results = Vec::new();

        // Process all cemented blocks while holding the lock to avoid
        // races where an election for a block that is already
        // cemented is inserted
        for (cemented_block, source_election) in batch {
            let dependent_election_opt = self.election_for_root(&cemented_block.qualified_root());

            // Distinguishes replay votes, cannot be determined if the block is not in any election
            // Dependent elections are implicitly confirmed when their block is cemented
            if let Some(dependent_election_mutex) = &dependent_election_opt {
                // TRY CONFIRM
                // TODO: This should either confirm or cancel the election
                let mut dependent_election = dependent_election_mutex.lock().unwrap();
                let winner_hash = dependent_election.winner().hash();
                if winner_hash == cemented_block.hash() {
                    dependent_election.force_confirm();
                }
            }

            let mut ended_election = EndedElection::new(cemented_block.clone());
            let mut handled = false;
            // Check if the currently cemented block was part of an election that triggered the confirmation
            if let Some(source_election) = source_election {
                // TODO compare winner hash instead!
                if source_election.winner.qualified_root() == cemented_block.qualified_root() {
                    ended_election = source_election;
                    handled = true;
                }
            }

            if handled {
                // already handled
            } else {
                if let Some(dep_el) = dependent_election_opt {
                    ended_election = dep_el
                        .lock()
                        .unwrap()
                        .into_ended_election(now, ElectionResult::ActiveConfirmationHeight);
                } else {
                    ended_election.result = ElectionResult::InactiveConfirmationHeight;
                }
            }

            results.push(ended_election);
        }
        results
    }

    pub(super) fn cementing_failed(&mut self, block_hash: &BlockHash) {
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
}

pub struct ElectionInsertInfo {
    pub election: Arc<Mutex<Election>>,
    pub inserted: bool,
}

#[derive(Default)]
pub struct ActiveElectionsInfo {
    pub max_elections: usize,
    pub total: usize,
    pub priority: usize,
    pub hinted: usize,
    pub optimistic: usize,
}
