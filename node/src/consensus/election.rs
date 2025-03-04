use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{atomic::AtomicUsize, Mutex, MutexGuard},
    time::{Duration, Instant, SystemTime},
};

use rsnano_core::{Amount, BlockHash, MaybeSavedBlock, PublicKey, QualifiedRoot, Root, SavedBlock};
use rsnano_stats::{DetailType, StatType};

use super::ElectionStatus;
use crate::utils::HardenedConstants;

pub static NEXT_ELECTION_ID: AtomicUsize = AtomicUsize::new(1);

pub struct Election {
    pub id: usize,
    pub root: Root,
    pub qualified_root: QualifiedRoot,
    pub election_start: Instant,
    pub live_vote_callback: Option<Box<dyn Fn(PublicKey) + Send + Sync>>,

    pub mutex: Mutex<ElectionData>,
}

impl Election {
    pub fn new(
        id: usize,
        block: SavedBlock,
        behavior: ElectionBehavior,
        live_vote_callback: Option<Box<dyn Fn(PublicKey) + Send + Sync>>,
    ) -> Self {
        let root = block.root();
        let qualified_root = block.qualified_root();

        let data = ElectionData {
            status: ElectionStatus {
                winner: Some(rsnano_core::MaybeSavedBlock::Saved(block.clone())),
                election_end: SystemTime::now(),
                block_count: 1,
                election_status_type: super::ElectionStatusType::Ongoing,
                ..Default::default()
            },
            last_votes: HashMap::from([(
                HardenedConstants::get().not_an_account_key,
                VoteInfo::new(0, block.hash()),
            )]),
            last_blocks: HashMap::from([(block.hash(), MaybeSavedBlock::Saved(block))]),
            state: ElectionState::Passive,
            state_start: Instant::now(),
            last_tally: HashMap::new(),
            final_weight: Amount::zero(),
            last_vote: None,
            last_block_hash: BlockHash::zero(),
            behavior,
            is_quorum: false,
            last_req: None,
            confirmation_request_count: 0,
            last_block: Instant::now(),
        };

        Self {
            id,
            mutex: Mutex::new(data),
            root,
            qualified_root,
            election_start: Instant::now(),
            live_vote_callback,
        }
    }

    pub fn duration(&self) -> Duration {
        self.election_start.elapsed()
    }

    pub fn lock(&self) -> MutexGuard<ElectionData> {
        self.mutex.lock().unwrap()
    }
}

impl Debug for Election {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Election")
            .field("id", &self.id)
            .field("qualified_root", &self.qualified_root)
            .finish()
    }
}

pub struct ElectionData {
    pub status: ElectionStatus,
    pub state: ElectionState,
    pub state_start: Instant,
    pub last_blocks: HashMap<BlockHash, MaybeSavedBlock>,
    pub last_votes: HashMap<PublicKey, VoteInfo>,
    pub final_weight: Amount,
    pub last_tally: HashMap<BlockHash, Amount>,
    /** The last time vote for this election was generated */
    pub last_vote: Option<Instant>,
    pub last_block_hash: BlockHash,
    pub behavior: ElectionBehavior,
    pub is_quorum: bool,
    pub last_req: Option<Instant>,
    pub confirmation_request_count: u32,
    pub last_block: Instant,
}

impl ElectionData {
    pub fn swap_quorum_on(&mut self) -> bool {
        if !self.is_quorum {
            self.is_quorum = true;
            true
        } else {
            false
        }
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

    pub fn transition_priority(&mut self) -> bool {
        if matches!(
            self.behavior,
            ElectionBehavior::Priority | ElectionBehavior::Manual
        ) {
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

    pub fn update_status_to_confirmed(&mut self, election: &Election) {
        self.status.election_end = SystemTime::now();
        self.status.election_duration = election.election_start.elapsed();
        self.status.confirmation_request_count = self.confirmation_request_count;
        self.status.block_count = self.last_blocks.len() as u32;
        self.status.voter_count = self.last_votes.len() as u32;
    }

    pub fn state_change(
        &mut self,
        expected: ElectionState,
        desired: ElectionState,
    ) -> Result<(), ()> {
        if Self::valid_change(expected, desired) {
            if self.state == expected {
                self.state = desired;
                self.state_start = Instant::now();
                return Ok(());
            }
        }

        Err(())
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
