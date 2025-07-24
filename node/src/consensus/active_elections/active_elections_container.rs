use std::{collections::HashMap, time::Duration};

use rsnano_core::{
    utils::{BackpressureSender, ContainerInfo, ContainerInfoProvider},
    Amount, Block, BlockHash, PublicKey, QualifiedRoot, SavedBlock, VoteError,
};
use rsnano_nullable_clock::Timestamp;
use rsnano_stats::{StatsCollection, StatsSource};

use crate::{
    consensus::{
        election::{
            AddForkResult, ConfirmationType, ConfirmedElection, Election, ElectionBehavior,
        },
        filtered_vote::FilteredVote,
    },
    representatives::QuorumSpecs,
};

use super::{
    apply_vote_helper::ApplyVoteHelper,
    cooldown_controller::{AecCooldownReason, CooldownController, CooldownResult},
    recently_confirmed_cache::RecentlyConfirmedCache,
    stats::AecStats,
    ActiveElectionsConfig, ActiveElectionsInfo, AecEvent, AecInsertError, AecInsertRequest, Entry,
    RootContainer,
};
use rsnano_ledger::RepWeights;
use strum::EnumCount;

pub struct ActiveElectionsContainer {
    roots: RootContainer,
    observer: Option<BackpressureSender<AecEvent>>,
    stopped: bool,
    count_by_behavior: [usize; ElectionBehavior::COUNT],
    base_latency: Duration,
    recently_confirmed: RecentlyConfirmedCache,
    cooldown: CooldownController,
    max_elections: usize,
    stats: AecStats,
}

impl ActiveElectionsContainer {
    pub fn new(config: ActiveElectionsConfig, base_latency: Duration) -> Self {
        Self {
            roots: RootContainer::default(),
            observer: None,
            stopped: false,
            count_by_behavior: Default::default(),
            base_latency,
            recently_confirmed: RecentlyConfirmedCache::new(config.confirmation_cache),
            cooldown: CooldownController::default(),
            max_elections: config.max_elections,
            stats: Default::default(),
        }
    }

    pub fn set_observer(&mut self, observer: BackpressureSender<AecEvent>) {
        self.observer = Some(observer);
    }

    pub fn max_len(&self) -> usize {
        self.max_elections
    }

    pub fn count_by_behavior(&self, behavior: ElectionBehavior) -> usize {
        self.count_by_behavior[behavior as usize]
    }

    fn count_by_behavior_mut(&mut self, behavior: ElectionBehavior) -> &mut usize {
        &mut self.count_by_behavior[behavior as usize]
    }

    pub fn iter(&self) -> impl Iterator<Item = &Election> {
        self.roots.iter().map(|i| &i.election)
    }

    pub fn insert(
        &mut self,
        request: AecInsertRequest,
        now: Timestamp,
    ) -> Result<(), AecInsertError> {
        self.ensure_not_stopped()?;
        self.ensure_not_recently_confirmed(&request)?;

        if !self.try_upgrade_existing_election(&request)? {
            self.insert_new_election(request, now);
        }

        Ok(())
    }

    fn ensure_not_stopped(&self) -> Result<(), AecInsertError> {
        if self.stopped {
            Err(AecInsertError::Stopped)
        } else {
            Ok(())
        }
    }

    fn ensure_not_recently_confirmed(
        &self,
        request: &AecInsertRequest,
    ) -> Result<(), AecInsertError> {
        let root = request.block.qualified_root();

        if self.recently_confirmed.root_exists(&root) {
            return Err(AecInsertError::RecentlyConfirmed);
        }
        Ok(())
    }

    fn try_upgrade_existing_election(
        &mut self,
        request: &AecInsertRequest,
    ) -> Result<bool, AecInsertError> {
        let Some(existing) = self.roots.get_mut(&request.block.qualified_root()) else {
            // Nothing upgraded - it's a new election
            return Ok(false);
        };

        let previous_behavior = existing.election.behavior();
        let upgraded = existing.election.maybe_upgrade_to(request.behavior);

        if upgraded {
            existing.priority = Some(request.priority);
            *self.count_by_behavior_mut(previous_behavior) -= 1;
            *self.count_by_behavior_mut(request.behavior) += 1;
            Ok(true)
        } else {
            Err(AecInsertError::Duplicate)
        }
    }

    fn insert_new_election(&mut self, request: AecInsertRequest, now: Timestamp) {
        let root = request.block.qualified_root();
        let hash = request.block.hash();
        let election = Election::new(request.block, request.behavior, self.base_latency, now);

        self.roots.insert(Entry {
            root: root.clone(),
            election,
            priority: Some(request.priority),
        });

        *self.count_by_behavior_mut(request.behavior) += 1;
        self.stats.started(request.behavior);
        self.notify(AecEvent::ElectionStarted(hash, root));
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
                self.roots.vote_router.disconnect(&removed.hash());
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
            self.roots
                .vote_router
                .connect(fork.hash(), fork.qualified_root());
            self.stats.conflicts += 1;
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
        let result = self.cooldown.set_cooldown(cool_down, reason);
        if result == CooldownResult::Recovered {
            self.notify(AecEvent::Recovered);
        }
    }

    pub fn stop(&mut self) {
        // destroy send queue so that the receiver thread will be stopped too
        drop(self.observer.take());
        self.stopped = true;
        self.roots.clear();
    }

    pub fn is_active_root(&self, root: &QualifiedRoot) -> bool {
        self.roots.get(root).is_some()
    }

    pub fn is_active_hash(&self, block_hash: &BlockHash) -> bool {
        self.roots.vote_router.is_active(block_hash)
    }

    pub fn was_recently_confirmed(&self, block_hash: &BlockHash) -> bool {
        self.recently_confirmed.hash_exists(block_hash)
    }

    pub fn clear_recently_confirmed(&mut self) {
        self.recently_confirmed.clear();
    }

    /// Returns the current active elections after transitioning
    pub fn transition_time(&mut self, now: Timestamp) -> Vec<Election> {
        self.stats.ticked += 1;
        let mut copies = Vec::with_capacity(self.roots.len());
        for entry in self.roots.iter_mut() {
            entry.election.transition_time(now);
            copies.push(entry.election.clone());
        }
        self.erase_ended_elections();
        copies
    }

    pub fn election_for_root(&self, root: &QualifiedRoot) -> Option<&Election> {
        self.roots.election_for_root(root)
    }

    pub fn election_for_block(&self, block_hash: &BlockHash) -> Option<&Election> {
        self.roots.election_for_block(block_hash)
    }

    pub fn transition_active(&mut self, block_hash: &BlockHash) -> bool {
        let Some(election) = self.roots.election_for_block_mut(block_hash) else {
            return false;
        };
        election.transition_active();
        true
    }

    pub fn remove_votes<'a>(
        &mut self,
        root: &QualifiedRoot,
        voters: impl IntoIterator<Item = &'a PublicKey>,
    ) {
        let Some(election) = self.roots.election_for_root_mut(root) else {
            return;
        };
        for voter in voters {
            election.remove_vote(voter);
        }
    }

    pub fn erase_ended_elections(&mut self) {
        let removed = self.roots.drain_filter(|i| i.election.state().has_ended());

        for entry in removed {
            self.cleanup_election(entry);
        }
    }

    pub fn erase(&mut self, root: &QualifiedRoot) -> bool {
        let Some(entry) = self.roots.erase(root) else {
            return false;
        };
        self.cleanup_election(entry);
        true
    }

    fn cleanup_election(&mut self, entry: Entry) {
        let election = &entry.election;

        // Keep track of election count by election type
        *self.count_by_behavior_mut(election.behavior()) -= 1;

        self.stats.stopped(&entry.election);
        self.notify(AecEvent::ElectionEnded(entry.election, entry.priority));
    }

    /// Dependent elections are implicitly confirmed when their block is confirmed
    pub fn confirm_dependent_elections(
        &mut self,
        confirmed: Vec<(SavedBlock, Option<ConfirmedElection>)>,
        now: Timestamp,
    ) {
        for (confirmed_block, source_election) in confirmed {
            let confirmed_election =
                self.confirm_dependent_election(&confirmed_block, source_election, now);

            self.block_confirmed(confirmed_block, confirmed_election);
        }
    }

    fn confirm_dependent_election(
        &mut self,
        confirmed_block: &SavedBlock,
        source_election: Option<ConfirmedElection>,
        now: Timestamp,
    ) -> ConfirmedElection {
        // Check if the currently confirmed block was part of an election that triggered
        // the block confirmation
        if let Some(source) = source_election {
            if confirmed_block.hash() == source.winner.hash() {
                // This is the block that was directly confirmed by the source election.
                // The election is already confirmed, so there is nothing to do.
                return source;
            }
        }

        let Some(corresponding) = self.roots.get_mut(&confirmed_block.qualified_root()) else {
            return ConfirmedElection::new(
                confirmed_block.clone(),
                ConfirmationType::InactiveConfirmationHeight,
            );
        };

        if corresponding.election.winner().hash() == confirmed_block.hash() {
            corresponding.election.force_confirm();
            corresponding
                .election
                .into_confirmed_election(now, ConfirmationType::ActiveConfirmationHeight)
        } else {
            corresponding.election.cancel();
            ConfirmedElection::new(
                confirmed_block.clone(),
                ConfirmationType::ActiveConfirmationHeight,
            )
        }
    }

    fn block_confirmed(&mut self, block: SavedBlock, election: ConfirmedElection) {
        self.stats.block_confirmations[election.confirmation_type as usize] += 1;
        self.notify(AecEvent::BlockConfirmed(block, election));
    }

    pub fn remove_recently_confirmed(&mut self, block_hash: &BlockHash) {
        self.recently_confirmed.erase(block_hash);
    }

    pub fn apply_vote<'a>(
        &mut self,
        args: ApplyVoteArgs<'a>,
    ) -> HashMap<BlockHash, Result<(), VoteError>> {
        let mut apply_helper = ApplyVoteHelper {
            args: &args,
            recently_confirmed: &mut self.recently_confirmed,
            vote_counter: &mut self.stats.vote_counter,
            observer: &self.observer,
            roots: &mut self.roots,
        };
        let result = apply_helper.apply_vote();
        for entry in result.confirmed {
            self.cleanup_election(entry);
        }
        result.per_block
    }

    pub fn force_confirm(&mut self, block_hash: &BlockHash, now: Timestamp) {
        let Some(election) = self.roots.election_for_block_mut(block_hash) else {
            panic!("Force confirm failed, because no active election was found");
        };
        if election.force_confirm() {
            let confirmed_election =
                election.into_confirmed_election(now, ConfirmationType::ActiveConfirmedQuorum);
            self.notify(AecEvent::ElectionConfirmed(confirmed_election));
        }
    }

    pub fn cancel(&mut self, root: &QualifiedRoot) {
        if let Some(entry) = self.roots.get_mut(root) {
            entry.election.cancel();
        }
    }

    pub fn len(&self) -> usize {
        self.roots.len()
    }

    pub fn info(&self) -> ActiveElectionsInfo {
        ActiveElectionsInfo {
            max_elections: self.max_elections,
            total: self.roots.len(),
            priority: self.count_by_behavior(ElectionBehavior::Priority),
            hinted: self.count_by_behavior(ElectionBehavior::Hinted),
            optimistic: self.count_by_behavior(ElectionBehavior::Optimistic),
        }
    }

    pub fn simulate_event(&self, event: AecEvent) {
        self.notify(event);
    }

    fn notify(&self, event: AecEvent) {
        if let Some(sender) = &self.observer {
            sender.send(event).unwrap()
        }
    }
}

impl Default for ActiveElectionsContainer {
    fn default() -> Self {
        Self::new(ActiveElectionsConfig::default(), Duration::from_secs(1))
    }
}

impl StatsSource for ActiveElectionsContainer {
    fn collect_stats(&self, result: &mut StatsCollection) {
        self.cooldown.collect_stats(result);
        self.stats.collect_stats(result);
    }
}

impl ContainerInfoProvider for ActiveElectionsContainer {
    fn container_info(&self) -> ContainerInfo {
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
            .node("vote_router", self.roots.vote_router.container_info())
            .finish()
    }
}

pub struct ApplyVoteArgs<'a> {
    pub vote: &'a FilteredVote,
    pub rep_weights: &'a RepWeights,
    pub quorum_specs: &'a QuorumSpecs,
    pub now: Timestamp,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::ReceivedVote;
    use rsnano_core::{utils::BlockPriority, PrivateKey, Vote, VoteSource};
    use std::sync::Arc;

    #[test]
    fn empty() {
        let container = ActiveElectionsContainer::default();
        assert_eq!(container.len(), 0);
        assert!(!container.is_active_root(&QualifiedRoot::new_test_instance()));
        assert!(!container.is_active_hash(&BlockHash::from(1)));
    }

    #[test]
    fn insert_election() {
        let mut container = ActiveElectionsContainer::default();
        let request = AecInsertRequest {
            block: SavedBlock::new_test_instance(),
            behavior: ElectionBehavior::Priority,
            priority: BlockPriority::new_test_instance(),
        };

        container
            .insert(request, Timestamp::new_test_instance())
            .unwrap();

        assert_eq!(container.len(), 1)
    }

    #[test]
    fn confirm_election() {
        let mut container = ActiveElectionsContainer::default();

        let block = SavedBlock::new_test_instance();
        let block_hash = block.hash();

        let request = AecInsertRequest {
            block,
            behavior: ElectionBehavior::Priority,
            priority: BlockPriority::new_test_instance(),
        };

        let now = Timestamp::new_test_instance();
        container.insert(request, now).unwrap();

        let rep_key = PrivateKey::from(1);
        let received_vote = test_final_vote(&rep_key, block_hash);

        let mut rep_weights = RepWeights::new();
        rep_weights.insert(rep_key.public_key(), Amount::MAX);

        let result = container.apply_vote(ApplyVoteArgs {
            vote: &received_vote.into(),
            rep_weights: &rep_weights,
            quorum_specs: &QuorumSpecs::new_test_instance(),
            now,
        });

        assert_eq!(result.get(&block_hash), Some(&Ok(())));

        assert!(container.election_for_block(&block_hash).is_none());
    }

    fn test_final_vote(rep_key: &PrivateKey, block_hash: BlockHash) -> ReceivedVote {
        let vote = Arc::new(Vote::new_final(rep_key, vec![block_hash]));
        ReceivedVote::new(vote, VoteSource::Live, None)
    }
}
