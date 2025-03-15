use std::{
    collections::HashMap,
    fmt::Debug,
    time::{Duration, Instant, SystemTime},
};

use rsnano_core::{
    utils::UnixMillisTimestamp, Amount, BlockHash, MaybeSavedBlock, Networks, PublicKey,
    QualifiedRoot, SavedBlock,
};
use rsnano_nullable_clock::Timestamp;
use rsnano_stats::DetailType;

use super::{block_tallies::BlockTallies, ElectionState};

#[derive(Clone)]
pub struct ElectionConfig {
    /// Minimum time between broadcasts of the current winner of an election, as a backup to requesting confirmations
    pub base_latency: Duration,
    pub block_broadcast_interval: Duration,
    pub vote_broadcast_interval: Duration,
}

impl Default for ElectionConfig {
    fn default() -> Self {
        Self {
            base_latency: Duration::from_millis(1000),
            block_broadcast_interval: Duration::from_secs(150),
            vote_broadcast_interval: Duration::from_secs(15),
        }
    }
}

impl ElectionConfig {
    pub fn default_for(network: Networks) -> Self {
        if network == Networks::NanoDevNetwork {
            Self {
                base_latency: Duration::from_millis(25),
                block_broadcast_interval: Duration::from_millis(500),
                vote_broadcast_interval: Duration::from_millis(500),
            }
        } else {
            Default::default()
        }
    }
}

pub struct Election {
    qualified_root: QualifiedRoot,
    winner: MaybeSavedBlock,
    state: ElectionState,
    candidate_blocks: HashMap<BlockHash, MaybeSavedBlock>,
    votes: HashMap<PublicKey, VoteSummary>,
    tally: Amount,
    final_tally: Amount,

    /// All tallies (non-final or final)
    tallies: BlockTallies,

    behavior: ElectionBehavior,
    has_quorum: bool,

    start: Timestamp,
    last_confirm_req_sent: Option<Instant>,
    last_broadcasted_block: BlockHash,
    last_block_broadcast: Instant,
    last_vote_generated: Option<Instant>,
    confirmation_request_count: usize,
    vote_broadcast_count: usize,

    config: ElectionConfig,
}

impl Election {
    const PASSIVE_DURATION_FACTOR: u32 = 5;
    pub const MAX_BLOCKS: usize = 10;

    pub fn new(
        block: SavedBlock,
        behavior: ElectionBehavior,
        config: ElectionConfig,
        now: Timestamp,
    ) -> Self {
        Self {
            qualified_root: block.qualified_root(),
            votes: HashMap::new(),
            candidate_blocks: HashMap::from([(
                block.hash(),
                MaybeSavedBlock::Saved(block.clone()),
            )]),
            state: ElectionState::Passive,
            tallies: BlockTallies::new(),
            tally: Amount::zero(),
            final_tally: Amount::zero(),
            last_vote_generated: None,
            last_broadcasted_block: BlockHash::zero(),
            behavior,
            has_quorum: false,
            last_confirm_req_sent: None,
            confirmation_request_count: 0,
            vote_broadcast_count: 0,
            last_block_broadcast: Instant::now(),
            start: now,
            config,
            winner: MaybeSavedBlock::Saved(block),
        }
    }

    pub fn new_test_instance_with(block: SavedBlock) -> Self {
        Self::new(
            block,
            ElectionBehavior::Priority,
            ElectionConfig::default(),
            Timestamp::new_test_instance(),
        )
    }

    pub fn qualified_root(&self) -> &QualifiedRoot {
        &self.qualified_root
    }

    pub fn behavior(&self) -> ElectionBehavior {
        self.behavior
    }

    pub fn state(&self) -> ElectionState {
        self.state
    }

    pub fn candidate_blocks(&self) -> &HashMap<BlockHash, MaybeSavedBlock> {
        &self.candidate_blocks
    }

    pub fn contains_block(&self, hash: &BlockHash) -> bool {
        self.candidate_blocks.contains_key(hash)
    }

    pub fn block_count(&self) -> usize {
        self.candidate_blocks.len()
    }

    pub fn has_max_blocks(&self) -> bool {
        self.block_count() >= Self::MAX_BLOCKS
    }

    pub fn add_candidate_block(&mut self, block: impl Into<MaybeSavedBlock>) -> bool {
        if self.has_max_blocks() {
            return false;
        }
        let block = block.into();
        self.candidate_blocks.insert(block.hash(), block).is_none()
    }

    pub fn votes(&self) -> &HashMap<PublicKey, VoteSummary> {
        &self.votes
    }

    pub fn add_vote(&mut self, voter: PublicKey, timestamp: UnixMillisTimestamp, hash: BlockHash) {
        self.votes
            .insert(voter, VoteSummary::new(voter, timestamp, hash));
    }

    pub fn tally(&self) -> Amount {
        self.tally
    }

    pub fn final_tally(&self) -> Amount {
        self.final_tally
    }

    /// Tallies for the candidate blocks, ordered by descending tally
    pub fn tallies(&self) -> &BlockTallies {
        &self.tallies
    }

    pub fn confirmation_request_count(&self) -> usize {
        self.confirmation_request_count
    }

    pub fn transition_time(&mut self, now: Timestamp) {
        let duration = self.start.elapsed(now);
        match self.state {
            ElectionState::Passive => {
                if self.config.base_latency * Self::PASSIVE_DURATION_FACTOR < duration {
                    self.state = ElectionState::Active;
                }
            }
            ElectionState::Confirmed => {
                self.state = ElectionState::ExpiredConfirmed;
            }
            _ => {}
        }

        if !self.state.has_ended() && self.time_to_live() < duration {
            self.state = ElectionState::ExpiredUnconfirmed;
        }
    }

    pub fn should_broadcast_winner_block(&self) -> bool {
        // Broadcast the block if enough time has passed since the last broadcast (or it's the first broadcast)
        if self.last_block_broadcast.elapsed() < self.config.block_broadcast_interval {
            true
        } else {
            // Or the current election winner has changed
            self.winner().hash() != self.last_broadcasted_block
        }
    }

    /// Calculates time delay between broadcasting confirmation requests
    fn confirm_req_interval(&self) -> Duration {
        match self.behavior {
            ElectionBehavior::Priority | ElectionBehavior::Manual | ElectionBehavior::Hinted => {
                self.config.base_latency * 5
            }
            ElectionBehavior::Optimistic => self.config.base_latency * 2,
        }
    }

    pub fn has_quorum(&self) -> bool {
        self.has_quorum
    }

    /// Returns true if final votes should be generated
    pub fn is_final(&self) -> bool {
        self.is_confirmed() || self.has_quorum()
    }

    pub fn cancel(&mut self) {
        if !self.state.has_ended() {
            self.state = ElectionState::Cancelled;
        }
    }

    pub fn vote_count(&self) -> usize {
        self.votes.len()
    }

    pub fn transition_active(&mut self) {
        if self.state == ElectionState::Passive {
            self.state = ElectionState::Active;
        }
    }

    pub fn maybe_upgrade_to(&mut self, new_behavior: ElectionBehavior) -> bool {
        if new_behavior != ElectionBehavior::Priority {
            // Only upgrades to priority elections are allowed!
            return false;
        }

        if matches!(
            self.behavior,
            ElectionBehavior::Priority | ElectionBehavior::Manual
        ) {
            // Nothing to do;
            return false;
        }

        self.behavior = ElectionBehavior::Priority;

        // allow new outgoing votes immediately
        self.last_vote_generated = None;
        true
    }

    pub fn is_confirmed(&self) -> bool {
        self.state.is_confirmed()
    }

    /// Returns true, if it was the initial broadcast
    pub fn was_winner_block_broadcasted(&mut self) -> bool {
        let is_initial_broadcast = self.last_broadcasted_block.is_zero();
        self.last_block_broadcast = Instant::now();
        self.last_broadcasted_block = self.winner.hash();
        is_initial_broadcast
    }

    pub fn vote_broadcast_count(&self) -> usize {
        self.vote_broadcast_count
    }

    pub fn winner(&self) -> &MaybeSavedBlock {
        &self.winner
    }

    pub fn should_send_confirm_req(&self) -> bool {
        self.confirm_req_interval() < self.last_confirm_request_elapsed()
    }

    pub fn confirm_request_sent(&mut self) {
        self.last_confirm_req_sent = Some(Instant::now());
        self.confirmation_request_count += 1;
    }

    fn last_confirm_request_elapsed(&self) -> Duration {
        match self.last_confirm_req_sent {
            Some(i) => i.elapsed(),
            None => Duration::MAX,
        }
    }

    pub fn update_status_to_confirmed(&mut self) {
        self.state = ElectionState::Confirmed;
    }

    pub fn time_to_live(&self) -> Duration {
        match self.behavior {
            ElectionBehavior::Manual | ElectionBehavior::Priority => Duration::from_secs(60 * 5),
            ElectionBehavior::Hinted | ElectionBehavior::Optimistic => Duration::from_secs(30),
        }
    }

    pub fn voted(&mut self) {
        self.last_vote_generated = Some(Instant::now());
        self.vote_broadcast_count += 1;
    }

    fn last_vote_elapsed(&self) -> Duration {
        match &self.last_vote_generated {
            Some(i) => i.elapsed(),
            None => Duration::from_secs(60 * 60 * 24 * 365), // Duration::MAX caused problems with C++
        }
    }

    pub fn can_vote(&self) -> bool {
        self.last_vote_elapsed() >= self.config.vote_broadcast_interval
    }

    pub fn start(&self) -> Timestamp {
        self.start
    }

    pub fn remove_tally_below(&mut self, min_tally: Amount) -> Option<MaybeSavedBlock> {
        if min_tally.is_zero() {
            return None;
        }

        let mut block_to_remove = BlockHash::zero();
        let winner_hash = self.winner.hash();

        // Replace if lowest tally is below inactive cache new block weight
        if self.tallies.len() < Self::MAX_BLOCKS {
            // If count of tally items is less than 10, remove any block without tally
            for (hash, _) in &self.candidate_blocks {
                if !self.tallies.contains(hash) && *hash != winner_hash {
                    block_to_remove = *hash;
                    break;
                }
            }
        }

        if block_to_remove.is_zero() {
            let (lowest_tally, lowest_hash) = self.tallies.lowest().unwrap();
            if min_tally > lowest_tally {
                if lowest_hash != winner_hash {
                    block_to_remove = lowest_hash;
                } else {
                    // Avoid removing winner
                    let (second_lowest_tally, second_lowest_hash) =
                        self.tallies.iter().rev().nth(1).unwrap();

                    if min_tally > second_lowest_tally {
                        block_to_remove = second_lowest_hash;
                    }
                }
            }
        }

        let removed = if !block_to_remove.is_zero() {
            self.remove_block(&block_to_remove)
        } else {
            None
        };

        removed
    }

    /// Calculate tallies and try to confirm this election
    pub fn try_confirm(&mut self, rep_weights: &HashMap<PublicKey, Amount>, quorum_delta: Amount) {
        // TODO early return if confirmed

        let old_winner_hash = self.winner().hash();

        let mut block_tallies: HashMap<BlockHash, Amount> = HashMap::new();
        let mut final_tallies: HashMap<BlockHash, Amount> = HashMap::new();

        for (account, info) in self.votes.iter_mut() {
            info.weight = rep_weights.get(account).cloned().unwrap_or_default();
            *block_tallies.entry(info.hash).or_default() += info.weight;
            if info.timestamp == UnixMillisTimestamp::MAX {
                *final_tallies.entry(info.hash).or_default() += info.weight;
            }
        }

        self.tallies.clear();
        for (hash, weight) in &block_tallies {
            if let Some(block) = self.candidate_blocks.get(hash) {
                self.tallies.insert(*weight, block.hash());
            }
        }

        let (tally, new_winner_hash) = self
            .tallies
            .winner()
            .unwrap_or((Amount::zero(), old_winner_hash));

        self.tally = tally;

        // Calculate final votes sum for winner
        if let Some(final_tally) = final_tallies.get(&new_winner_hash) {
            self.final_tally = *final_tally;
        }

        if self.tallies.sum() >= quorum_delta && new_winner_hash != old_winner_hash {
            let block = self
                .candidate_blocks()
                .get(&new_winner_hash)
                .unwrap()
                .clone();
            self.winner = block;
        }

        if self.tallies.check_quorum(quorum_delta) {
            self.has_quorum = true;
        }

        if self.final_tally >= quorum_delta {
            self.update_status_to_confirmed();
        }
    }

    pub fn remove_vote(&mut self, voter: &PublicKey) {
        self.votes.remove(voter);
    }

    fn remove_block(&mut self, hash: &BlockHash) -> Option<MaybeSavedBlock> {
        if self.winner.hash() != *hash {
            let existing = self.candidate_blocks.remove(hash);
            if existing.is_some() {
                self.votes.retain(|_, v| v.hash != *hash);
                return existing;
            }
        }

        None
    }

    /// TODO: Remove as soon as possible
    pub fn change_vote_timestamp(&mut self, voter: &PublicKey, new_timestamp: SystemTime) {
        self.votes.get_mut(voter).unwrap().time = new_timestamp;
    }
}

#[derive(Clone)]
pub struct VoteSummary {
    pub voter: PublicKey,
    pub time: SystemTime, // TODO use Instant
    pub timestamp: UnixMillisTimestamp,
    pub hash: BlockHash,
    pub weight: Amount,
}

impl VoteSummary {
    pub fn new(voter: PublicKey, timestamp: UnixMillisTimestamp, hash: BlockHash) -> Self {
        Self {
            voter,
            time: SystemTime::now(),
            timestamp,
            hash,
            weight: Amount::zero(),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ElectionBehavior {
    Manual,
    Priority,
    /**
     * Hinted elections:
     * - shorter timespan
     * - limited space inside AEC
     */
    Hinted,
    /**
     * Optimistic elections:
     * - shorter timespan
     * - limited space inside AEC
     * - more frequent confirmation requests
     */
    Optimistic,
}

impl From<ElectionBehavior> for DetailType {
    fn from(value: ElectionBehavior) -> Self {
        match value {
            ElectionBehavior::Manual => DetailType::Manual,
            ElectionBehavior::Priority => DetailType::Priority,
            ElectionBehavior::Hinted => DetailType::Hinted,
            ElectionBehavior::Optimistic => DetailType::Optimistic,
        }
    }
}
