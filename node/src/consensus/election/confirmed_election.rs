use std::time::{Duration, SystemTime};

use rsnano_core::{Amount, MaybeSavedBlock, SavedBlock};
use rsnano_stats::DetailType;

use super::VoteSummary;
use strum_macros::{EnumCount, EnumIter};

/// How a block got confirmed
#[derive(PartialEq, Eq, Debug, Clone, Copy, EnumCount, EnumIter)]
pub enum ConfirmationType {
    /// An election for this block was active and received enough votes
    ActiveConfirmedQuorum,
    /// An election for this block was active, but the block got confirmed indirectly
    /// when a newer block got confirmed
    ActiveConfirmationHeight,
    /// There was no active election for this block. It got confirmed indirectly
    /// when a newer block got confirmed
    InactiveConfirmationHeight,
}

impl ConfirmationType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ActiveConfirmedQuorum => "active_quorum",
            Self::ActiveConfirmationHeight => "active_confirmation_height",
            Self::InactiveConfirmationHeight => "inactive",
        }
    }
}

impl From<ConfirmationType> for DetailType {
    fn from(value: ConfirmationType) -> Self {
        match value {
            ConfirmationType::ActiveConfirmedQuorum => DetailType::ActiveConfirmedQuorum,
            ConfirmationType::ActiveConfirmationHeight => DetailType::ActiveConfirmationHeight,
            ConfirmationType::InactiveConfirmationHeight => DetailType::InactiveConfirmationHeight,
        }
    }
}

/// Information about confirmed election
#[derive(Clone)]
pub struct ConfirmedElection {
    pub winner: MaybeSavedBlock,
    pub tally: Amount,
    pub final_tally: Amount,
    pub block_count: u32,
    pub voter_count: u32,
    pub election_end: SystemTime,
    pub election_duration: Duration,
    pub confirmation_type: ConfirmationType,
    pub votes: Vec<VoteSummary>,
}

impl ConfirmedElection {
    pub fn new(block: SavedBlock, confirmation_type: ConfirmationType) -> Self {
        Self {
            winner: MaybeSavedBlock::Saved(block),
            election_end: SystemTime::now(),
            block_count: 1,
            confirmation_type,
            tally: Amount::zero(),
            final_tally: Amount::zero(),
            voter_count: 0,
            election_duration: Duration::ZERO,
            votes: Vec::new(),
        }
    }
}
