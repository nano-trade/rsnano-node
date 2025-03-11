use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

use rsnano_core::{
    Amount, BlockHash, DescTallyKey, HardenedConstants, MaybeSavedBlock, Networks, PublicKey,
    QualifiedRoot, Root, SavedBlock, VoteWithWeightInfo,
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
    pub status: ElectionStatus,
    pub state: ElectionState,
    pub last_blocks: HashMap<BlockHash, MaybeSavedBlock>,
    pub last_votes: HashMap<PublicKey, VoteInfo>,
    pub final_weight: Amount,
    pub last_tally: HashMap<BlockHash, Amount>,

    /// The last time a vote for this election was generated
    last_vote_generated: Option<Instant>,
    last_broadcasted_block: BlockHash,
    pub behavior: ElectionBehavior,
    is_quorum: bool,
    last_req: Option<Instant>,
    pub confirmation_request_count: u32,
    last_block_broadcast: Instant,
    election_start: Instant,
    config: ElectionConfig,
}

impl Election {
    const PASSIVE_DURATION_FACTOR: u32 = 5;
    pub const MAX_BLOCKS: usize = 10;

    pub fn new(block: SavedBlock, behavior: ElectionBehavior, config: ElectionConfig) -> Self {
        Self {
            qualified_root: block.qualified_root(),
            status: ElectionStatus {
                winner: Some(MaybeSavedBlock::Saved(block.clone())),
                election_end: SystemTime::now(),
                block_count: 1,
                election_status_type: ElectionStatusType::Ongoing,
                ..Default::default()
            },
            last_votes: HashMap::from([(
                HardenedConstants::get().not_an_account_key,
                VoteInfo::new(0, block.hash()),
            )]),
            last_blocks: HashMap::from([(block.hash(), MaybeSavedBlock::Saved(block))]),
            state: ElectionState::Passive,
            last_tally: HashMap::new(),
            final_weight: Amount::zero(),
            last_vote_generated: None,
            last_broadcasted_block: BlockHash::zero(),
            behavior,
            is_quorum: false,
            last_req: None,
            confirmation_request_count: 0,
            last_block_broadcast: Instant::now(),
            election_start: Instant::now(),
            config,
        }
    }

    pub fn root(&self) -> &Root {
        &self.qualified_root.root
    }

    pub fn qualified_root(&self) -> &QualifiedRoot {
        &self.qualified_root
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
            if self
                .state_change(state, ElectionState::ExpiredUnconfirmed)
                .is_ok()
            {
                self.status.election_status_type = ElectionStatusType::Stopped;
            }
        }
    }

    pub fn should_broadcast_winner_block(&self) -> bool {
        // Broadcast the block if enough time has passed since the last broadcast (or it's the first broadcast)
        if self.time_since_last_block_broadcast() < self.config.block_broadcast_interval {
            true
        } else {
            // Or the current election winner has changed
            self.winner_hash().unwrap() != self.last_broadcasted_block
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

    pub fn swap_quorum_on(&mut self) -> bool {
        if !self.is_quorum {
            self.is_quorum = true;
            true
        } else {
            false
        }
    }

    pub fn is_quorum(&self) -> bool {
        self.is_quorum
    }

    pub fn set_winner(&mut self, winner: MaybeSavedBlock) {
        self.status.winner = Some(winner);
    }

    pub fn cancel(&mut self) {
        let current = self.state;
        let _ = self.state_change(current, ElectionState::Cancelled);
    }

    pub fn vote_count(&self) -> usize {
        self.last_votes.len()
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
    pub fn winner_block_broadcasted(&mut self) -> bool {
        let is_initial_broadcast = self.last_broadcasted_block.is_zero();
        self.last_block_broadcast = Instant::now();
        self.last_broadcasted_block = self.winner_hash().unwrap();
        is_initial_broadcast
    }

    pub fn time_since_last_block_broadcast(&self) -> Duration {
        self.last_block_broadcast.elapsed()
    }

    pub fn winner_hash(&self) -> Option<BlockHash> {
        self.status.winner.as_ref().map(|w| w.hash())
    }

    pub fn should_send_confirm_req(&self) -> bool {
        self.confirm_req_interval() < self.last_confirm_request_elapsed()
    }

    pub fn confirm_request_sent(&mut self) {
        self.last_req = Some(Instant::now());
        self.confirmation_request_count += 1;
    }

    fn last_confirm_request_elapsed(&self) -> Duration {
        match self.last_req {
            Some(i) => i.elapsed(),
            None => Duration::MAX,
        }
    }

    pub fn update_status_to_confirmed(&mut self) {
        self.status.election_end = SystemTime::now();
        self.status.election_duration = self.duration();
        self.status.confirmation_request_count = self.confirmation_request_count;
        self.status.block_count = self.last_blocks.len() as u32;
        self.status.voter_count = self.last_votes.len() as u32;
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

    pub fn set_last_vote(&mut self) {
        self.last_vote_generated = Some(Instant::now());
    }

    pub fn last_vote_elapsed(&self) -> Duration {
        match &self.last_vote_generated {
            Some(i) => i.elapsed(),
            None => Duration::from_secs(60 * 60 * 24 * 365), // Duration::MAX caused problems with C++
        }
    }

    pub fn should_vote(&self) -> bool {
        self.last_vote_elapsed() >= self.config.vote_broadcast_interval
    }

    pub fn duration(&self) -> Duration {
        self.election_start.elapsed()
    }

    pub fn votes_with_weight(&self, rep_weights: &RepWeightCache) -> Vec<VoteWithWeightInfo> {
        let mut sorted_votes: BTreeMap<DescTallyKey, Vec<VoteWithWeightInfo>> = BTreeMap::new();
        for (&representative, info) in &self.last_votes {
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
        let result: Vec<_> = sorted_votes
            .values_mut()
            .map(|i| std::mem::take(i))
            .flatten()
            .collect();
        result
    }

    pub fn remove_tally_below(&mut self, min_tally: Amount) -> Option<MaybeSavedBlock> {
        let mut removed_block_hash = BlockHash::zero();
        let winner_hash = self.winner_hash().unwrap();
        // Sort existing blocks tally
        let mut sorted: Vec<_> = self
            .last_tally
            .iter()
            .map(|(hash, amount)| (*hash, *amount))
            .collect();

        // Sort in ascending order
        sorted.sort_by(|left, right| right.cmp(left));

        // Replace if lowest tally is below inactive cache new block weight
        if min_tally > Amount::zero() && sorted.len() < Self::MAX_BLOCKS {
            // If count of tally items is less than 10, remove any block without tally
            for (hash, _) in &self.last_blocks {
                if sorted.iter().all(|(h, _)| h != hash) && *hash != winner_hash {
                    removed_block_hash = *hash;
                    break;
                }
            }
        } else if min_tally > Amount::zero() && min_tally > sorted.first().unwrap().1 {
            if sorted.first().unwrap().0 != winner_hash {
                removed_block_hash = sorted[0].0;
            } else if min_tally > sorted[1].1 {
                // Avoid removing winner
                removed_block_hash = sorted[1].0;
            }
        }

        let removed = if !removed_block_hash.is_zero() {
            self.remove_block(&removed_block_hash)
        } else {
            None
        };

        removed
    }

    pub fn remove_block(&mut self, hash: &BlockHash) -> Option<MaybeSavedBlock> {
        if self.winner_hash().unwrap_or_default() != *hash {
            let existing = self.last_blocks.remove(hash);
            if existing.is_some() {
                self.last_votes.retain(|_, v| v.hash != *hash);
                return existing;
            }
        }

        None
    }
}

#[derive(Clone)]
pub struct VoteInfo {
    pub time: SystemTime, // TODO use Instant
    pub timestamp: u64,
    pub hash: BlockHash,
}

impl VoteInfo {
    pub fn new(timestamp: u64, hash: BlockHash) -> Self {
        Self {
            time: SystemTime::now(),
            timestamp,
            hash,
        }
    }
}

impl Default for VoteInfo {
    fn default() -> Self {
        Self::new(0, BlockHash::zero())
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

/**
 * Tag for the type of the election status
 */
#[derive(PartialEq, Eq, Debug, Clone, Copy, FromPrimitive)]
pub enum ElectionStatusType {
    Ongoing = 0,
    ActiveConfirmedQuorum = 1,
    ActiveConfirmationHeight = 2,
    InactiveConfirmationHeight = 3,
    Stopped = 5,
}

impl ElectionStatusType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Ongoing => "ongoing",
            Self::ActiveConfirmedQuorum => "active_quorum",
            Self::ActiveConfirmationHeight => "active_confirmation_height",
            Self::InactiveConfirmationHeight => "inactive",
            Self::Stopped => "stopped",
        }
    }
}

impl From<ElectionStatusType> for DetailType {
    fn from(value: ElectionStatusType) -> Self {
        match value {
            ElectionStatusType::Ongoing => DetailType::Ongoing,
            ElectionStatusType::ActiveConfirmedQuorum => DetailType::ActiveConfirmedQuorum,
            ElectionStatusType::ActiveConfirmationHeight => DetailType::ActiveConfirmationHeight,
            ElectionStatusType::InactiveConfirmationHeight => {
                DetailType::InactiveConfirmationHeight
            }
            ElectionStatusType::Stopped => DetailType::Stopped,
        }
    }
}

/// Information on the status of an election
#[derive(Clone)]
pub struct ElectionStatus {
    pub winner: Option<MaybeSavedBlock>,
    pub tally: Amount,
    pub final_tally: Amount,
    pub confirmation_request_count: u32,
    pub block_count: u32,
    pub voter_count: u32,
    pub election_end: SystemTime,
    pub election_duration: Duration,
    pub election_status_type: ElectionStatusType,
    pub vote_broadcast_count: u32,
}

impl Default for ElectionStatus {
    fn default() -> Self {
        Self {
            winner: None,
            tally: Amount::zero(),
            final_tally: Amount::zero(),
            block_count: 0,
            voter_count: 0,
            confirmation_request_count: 0,
            election_end: SystemTime::now(),
            election_duration: Duration::ZERO,
            election_status_type: ElectionStatusType::InactiveConfirmationHeight,
            vote_broadcast_count: 0,
        }
    }
}

/// A block that is currently cementing
#[derive(Clone)]
pub struct CementingEntry {
    pub hash: BlockHash,
    pub election: Option<Arc<Mutex<Election>>>,
    pub timestamp: Instant,
}
