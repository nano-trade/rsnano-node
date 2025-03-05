use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    time::{Duration, Instant, SystemTime},
};

use rsnano_core::{
    Amount, BlockHash, MaybeSavedBlock, PublicKey, QualifiedRoot, Root, SavedBlock,
    VoteWithWeightInfo,
};
use rsnano_ledger::RepWeightCache;
use rsnano_stats::{DetailType, StatType};

use super::{ElectionStatus, ElectionStatusType, TallyKey};
use crate::utils::HardenedConstants;

pub struct Election {
    qualified_root: QualifiedRoot,
    pub status: ElectionStatus,
    pub state: ElectionState,
    pub last_blocks: HashMap<BlockHash, MaybeSavedBlock>,
    pub last_votes: HashMap<PublicKey, VoteInfo>,
    pub final_weight: Amount,
    pub last_tally: HashMap<BlockHash, Amount>,

    /// The last time a vote for this election was generated
    pub last_vote: Option<Instant>,
    pub last_block_hash: BlockHash,
    pub behavior: ElectionBehavior,
    is_quorum: bool,
    last_req: Option<Instant>,
    pub confirmation_request_count: u32,
    last_block: Instant,
    election_start: Instant,
}

impl Election {
    pub fn new(block: SavedBlock, behavior: ElectionBehavior) -> Self {
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
            last_vote: None,
            last_block_hash: BlockHash::zero(),
            behavior,
            is_quorum: false,
            last_req: None,
            confirmation_request_count: 0,
            last_block: Instant::now(),
            election_start: Instant::now(),
        }
    }

    pub fn root(&self) -> &Root {
        &self.qualified_root.root
    }

    pub fn qualified_root(&self) -> &QualifiedRoot {
        &self.qualified_root
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
        self.last_vote = None;
        true
    }

    pub fn is_confirmed(&self) -> bool {
        matches!(
            self.state,
            ElectionState::Confirmed | ElectionState::ExpiredConfirmed
        )
    }

    pub fn set_last_block(&mut self) {
        self.last_block = Instant::now();
    }

    pub fn last_block_elapsed(&self) -> Duration {
        self.last_block.elapsed()
    }

    pub fn winner_hash(&self) -> Option<BlockHash> {
        self.status.winner.as_ref().map(|w| w.hash())
    }

    pub fn confirm_request_sent(&mut self) {
        self.last_req = Some(Instant::now());
        self.confirmation_request_count += 1;
    }

    pub fn last_confirm_request_elapsed(&self) -> Duration {
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
        self.last_vote = Some(Instant::now());
    }

    pub fn last_vote_elapsed(&self) -> Duration {
        match &self.last_vote {
            Some(i) => i.elapsed(),
            None => Duration::from_secs(60 * 60 * 24 * 365), // Duration::MAX caused problems with C++
        }
    }

    pub fn duration(&self) -> Duration {
        self.election_start.elapsed()
    }

    pub fn votes_with_weight(&self, rep_weights: &RepWeightCache) -> Vec<VoteWithWeightInfo> {
        let mut sorted_votes: BTreeMap<TallyKey, Vec<VoteWithWeightInfo>> = BTreeMap::new();
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
                .entry(TallyKey(weight))
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
