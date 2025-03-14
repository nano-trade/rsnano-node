use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

use rsnano_core::{
    utils::UnixMillisTimestamp, Amount, BlockHash, DescTallyKey, HardenedConstants,
    MaybeSavedBlock, Networks, PublicKey, QualifiedRoot, SavedBlock, VoteWithWeightInfo,
};
use rsnano_stats::{DetailType, StatType};

use crate::RepWeightCache;

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
    votes: HashMap<PublicKey, VoteInfo>,
    tally: Amount,
    final_tally: Amount,
    tallies: BTreeMap<DescTallyKey, BlockHash>,

    behavior: ElectionBehavior,
    has_quorum: bool,

    election_start: Instant,
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

    pub fn new(block: SavedBlock, behavior: ElectionBehavior, config: ElectionConfig) -> Self {
        Self {
            qualified_root: block.qualified_root(),
            votes: HashMap::from([(
                HardenedConstants::get().not_an_account_key,
                VoteInfo::new(UnixMillisTimestamp::ZERO, block.hash()),
            )]),
            candidate_blocks: HashMap::from([(
                block.hash(),
                MaybeSavedBlock::Saved(block.clone()),
            )]),
            state: ElectionState::Passive,
            tallies: BTreeMap::new(),
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
            election_start: Instant::now(),
            config,
            winner: MaybeSavedBlock::Saved(block),
        }
    }

    pub fn new_test_instance_with(block: SavedBlock) -> Self {
        Self::new(block, ElectionBehavior::Priority, ElectionConfig::default())
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

    pub fn add_candidate_block(&mut self, block: impl Into<MaybeSavedBlock>) {
        let block = block.into();
        self.candidate_blocks.insert(block.hash(), block);
    }

    pub fn votes(&self) -> &HashMap<PublicKey, VoteInfo> {
        &self.votes
    }

    pub fn add_vote(&mut self, voter: PublicKey, timestamp: UnixMillisTimestamp, hash: BlockHash) {
        self.votes.insert(voter, VoteInfo::new(timestamp, hash));
    }

    pub fn tally(&self) -> Amount {
        self.tally
    }

    pub fn final_tally(&self) -> Amount {
        self.final_tally
    }

    /// Tallies for the candidate blocks, ordered by descending tally
    pub fn tallies(&self) -> &BTreeMap<DescTallyKey, BlockHash> {
        &self.tallies
    }

    pub fn confirmation_request_count(&self) -> usize {
        self.confirmation_request_count
    }

    pub fn transition_time(&mut self) {
        match self.state {
            ElectionState::Passive => {
                if self.config.base_latency * Self::PASSIVE_DURATION_FACTOR < self.duration() {
                    self.state_change(ElectionState::Passive, ElectionState::Active)
                        .unwrap();
                }
            }
            ElectionState::Confirmed => {
                self.state_change(ElectionState::Confirmed, ElectionState::ExpiredConfirmed)
                    .unwrap();
            }
            ElectionState::Active
            | ElectionState::ExpiredConfirmed
            | ElectionState::ExpiredUnconfirmed
            | ElectionState::Cancelled => {}
        }

        if !self.state.is_confirmed() && self.time_to_live() < self.duration() {
            // It is possible the election confirmed while acquiring the mutex
            // state_change returning true would indicate it
            let state = self.state;
            let _ = self.state_change(state, ElectionState::ExpiredUnconfirmed);
        }
    }

    pub fn should_broadcast_winner_block(&self) -> bool {
        // Broadcast the block if enough time has passed since the last broadcast (or it's the first broadcast)
        if self.time_since_last_block_broadcast() < self.config.block_broadcast_interval {
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

    pub fn set_winner(&mut self, winner: MaybeSavedBlock) {
        self.winner = winner;
    }

    pub fn cancel(&mut self) {
        let current = self.state;
        let _ = self.state_change(current, ElectionState::Cancelled);
    }

    pub fn vote_count(&self) -> usize {
        self.votes.len()
    }

    pub fn transition_active(&mut self) {
        let _ = self.state_change(ElectionState::Passive, ElectionState::Active);
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

    pub fn time_since_last_block_broadcast(&self) -> Duration {
        self.last_block_broadcast.elapsed()
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

    pub fn state_change(
        &mut self,
        expected: ElectionState,
        desired: ElectionState,
    ) -> anyhow::Result<()> {
        if Self::valid_change(expected, desired) {
            if self.state == expected {
                self.state = desired;
                return Ok(());
            }
        }

        Err(anyhow!(
            "Invalid state change from {:?} to {:?}",
            expected,
            desired
        ))
    }

    pub fn time_to_live(&self) -> Duration {
        match self.behavior {
            ElectionBehavior::Manual | ElectionBehavior::Priority => Duration::from_secs(60 * 5),
            ElectionBehavior::Hinted | ElectionBehavior::Optimistic => Duration::from_secs(30),
        }
    }

    fn valid_change(expected: ElectionState, desired: ElectionState) -> bool {
        match expected {
            ElectionState::Passive => matches!(
                desired,
                ElectionState::Active
                    | ElectionState::Confirmed
                    | ElectionState::ExpiredUnconfirmed
                    | ElectionState::Cancelled
            ),
            ElectionState::Active => matches!(
                desired,
                ElectionState::Confirmed
                    | ElectionState::ExpiredUnconfirmed
                    | ElectionState::Cancelled
            ),
            ElectionState::Confirmed => matches!(desired, ElectionState::ExpiredConfirmed),
            ElectionState::Cancelled
            | ElectionState::ExpiredConfirmed
            | ElectionState::ExpiredUnconfirmed => false,
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

    pub fn duration(&self) -> Duration {
        self.election_start.elapsed()
    }

    pub fn votes_with_weight(&self, rep_weights: &RepWeightCache) -> Vec<VoteWithWeightInfo> {
        let mut sorted_votes: BTreeMap<DescTallyKey, Vec<VoteWithWeightInfo>> = BTreeMap::new();
        for (&representative, info) in &self.votes {
            if representative == HardenedConstants::get().not_an_account_key {
                continue;
            }
            let weight = rep_weights.weight(&representative);
            let vote_with_weight = VoteWithWeightInfo {
                representative,
                time: info.time,
                timestamp: info.timestamp,
                hash: info.hash,
                weight,
            };
            sorted_votes
                .entry(DescTallyKey(weight))
                .or_default()
                .push(vote_with_weight);
        }

        sorted_votes
            .values_mut()
            .map(|i| std::mem::take(i))
            .flatten()
            .collect()
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
                if self.tallies.iter().all(|(_, h)| h != hash) && *hash != winner_hash {
                    block_to_remove = *hash;
                    break;
                }
            }
        } else if min_tally > **self.tallies.last_key_value().unwrap().0 {
            let lowest_hash = self.tallies.last_key_value().unwrap().1.clone();
            if lowest_hash != winner_hash {
                block_to_remove = lowest_hash;
            } else if min_tally > **self.tallies.iter().rev().nth(1).unwrap().0 {
                // Avoid removing winner
                block_to_remove = *self.tallies.iter().rev().nth(1).unwrap().1;
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

        let mut block_weights: HashMap<BlockHash, Amount> = HashMap::new();
        let mut final_weights: HashMap<BlockHash, Amount> = HashMap::new();
        for (account, info) in &self.votes {
            let weight = rep_weights.get(account).cloned().unwrap_or_default();
            *block_weights.entry(info.hash).or_default() += weight;
            if info.timestamp == UnixMillisTimestamp::MAX {
                *final_weights.entry(info.hash).or_default() += weight;
            }
        }

        self.tallies.clear();
        for (hash, weight) in &block_weights {
            if let Some(block) = self.candidate_blocks.get(hash) {
                self.tallies.insert(DescTallyKey(*weight), block.hash());
            }
        }

        let new_winner_hash = self
            .tallies
            .first_key_value()
            .map(|(_, hash)| *hash)
            .unwrap_or(old_winner_hash);

        self.tally = self
            .tallies
            .first_key_value()
            .map(|(tally, _)| tally.amount())
            .unwrap_or_default();

        // Calculate final votes sum for winner
        if let Some(final_weight) = final_weights.get(&new_winner_hash) {
            self.final_tally = *final_weight;
        }

        if self.sum_tallies() >= quorum_delta && new_winner_hash != old_winner_hash {
            let block = self
                .candidate_blocks()
                .get(&new_winner_hash)
                .unwrap()
                .clone();
            self.set_winner(block.clone());
        }

        if self.check_quorum(quorum_delta) {
            self.has_quorum = true;
        }

        if self.final_tally >= quorum_delta {
            self.update_status_to_confirmed();
        }
    }

    fn check_quorum(&self, quorum_delta: Amount) -> bool {
        let mut it = self.tallies.keys();
        let first = it.next().map(|i| i.amount()).unwrap_or_default();
        let second = it.next().map(|i| i.amount()).unwrap_or_default();
        first - second >= quorum_delta
    }

    fn sum_tallies(&self) -> Amount {
        let mut sum = Amount::zero();
        for k in self.tallies().keys() {
            sum += k.amount();
        }
        sum
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
pub struct VoteInfo {
    pub time: SystemTime, // TODO use Instant
    pub timestamp: UnixMillisTimestamp,
    pub hash: BlockHash,
}

impl VoteInfo {
    pub fn new(timestamp: UnixMillisTimestamp, hash: BlockHash) -> Self {
        Self {
            time: SystemTime::now(),
            timestamp,
            hash,
        }
    }
}

impl Default for VoteInfo {
    fn default() -> Self {
        Self::new(UnixMillisTimestamp::ZERO, BlockHash::zero())
    }
}

#[derive(FromPrimitive, Copy, Clone, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ElectionState {
    Passive,   // only listening for incoming votes
    Active,    // actively request confirmations
    Confirmed, // confirmed but still listening for votes
    ExpiredConfirmed,
    ExpiredUnconfirmed,
    Cancelled,
}

impl ElectionState {
    pub fn is_confirmed(&self) -> bool {
        matches!(self, Self::Confirmed | Self::ExpiredConfirmed)
    }

    pub fn has_ended(&self) -> bool {
        matches!(
            self,
            ElectionState::Confirmed
                | ElectionState::Cancelled
                | ElectionState::ExpiredConfirmed
                | ElectionState::ExpiredUnconfirmed
        )
    }
}

impl From<ElectionState> for StatType {
    fn from(value: ElectionState) -> Self {
        match value {
            ElectionState::Passive | ElectionState::Active => StatType::ActiveElectionsDropped,
            ElectionState::Confirmed | ElectionState::ExpiredConfirmed => {
                StatType::ActiveElectionsConfirmed
            }
            ElectionState::ExpiredUnconfirmed => StatType::ActiveElectionsTimeout,
            ElectionState::Cancelled => StatType::ActiveElectionsCancelled,
        }
    }
}

impl From<ElectionState> for DetailType {
    fn from(value: ElectionState) -> Self {
        match value {
            ElectionState::Passive => DetailType::Passive,
            ElectionState::Active => DetailType::Active,
            ElectionState::Confirmed => DetailType::Confirmed,
            ElectionState::ExpiredConfirmed => DetailType::ExpiredConfirmed,
            ElectionState::ExpiredUnconfirmed => DetailType::ExpiredUnconfirmed,
            ElectionState::Cancelled => DetailType::Cancelled,
        }
    }
}

#[derive(FromPrimitive, Copy, Clone, Debug, PartialEq, Eq)]
#[repr(u8)]
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

/// A block that is currently cementing
#[derive(Clone)]
pub struct CementingEntry {
    pub confirmation_root: BlockHash,
    pub election: Option<Arc<Mutex<Election>>>,
    pub timestamp: Instant,
}
